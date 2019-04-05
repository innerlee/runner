#!/usr/bin/env julia
using Shell
using Glob
using Suppressor
using Dates

#=
[0-RUN03211300]job-001.sh
[0-ORZ03211301][0-RUN03211300]job-001.sh
=#

runners = filter(x -> strip(x) != "", split(read(ignorestatus(pipeline(
    `ps -U $(ENV["USER"]) -ux`, `grep '[0-9]*:[0-9]*\s*julia\s*.*runner.jl'`)), String), "\n"))

if length(runners) == 0
    error("cannot detect self, the programe must be wrong!")
elseif length(runners) > 1
    println(join(runners, "\n"))
    warn("runner already running, exiting...")
    exit()
end

const jobroot = expanduser("~/jobs")
const jobqueue = joinpath(jobroot, "queue")
const jobdone = joinpath(jobroot, "done")
const jobstop = joinpath(jobroot, "stop")
const jobtrash = joinpath(jobroot, "trash")
const jobresume = joinpath(jobroot, "resume")
const joblog = joinpath(jobroot, "log")
const logfile = joinpath(joblog, "runner.log")
mkpath.([jobroot, jobqueue, jobdone, jobstop, jobtrash, jobresume, joblog])

@suppress Base.println(xs...) = open(f -> (println(f, "[$(now())] ", xs...);
    println(stdout, "[$(now())] ", xs...)), logfile, "a")

println("===== start =====")

const ngpu = parse(Int, read(pipeline(`nvidia-smi -L`, `wc -l`), String))
println("$ngpu GPUs detected.")

RETRY = 0
if length(ARGS) == 1
    VISIBLE_GPU = parse.(Int, split(ARGS[1], ","))
elseif length(ARGS) == 2 && ARGS[1] == "--retry"
    RETRY = parse(Int, ARGS[2])
    VISIBLE_GPU = collect(0:ngpu-1)
elseif length(ARGS) == 4 && ARGS[1] == "--retry" && ARGS[3] == "--gpu"
    RETRY = parse(Int, ARGS[2])
    VISIBLE_GPU = parse.(Int, split(ARGS[4], ","))
else
    VISIBLE_GPU = collect(0:ngpu-1)
end
println("visible gpu: $(VISIBLE_GPU).")

RETRY = min(RETRY, 3)

const CLEAN_TICK = 100
const ticks = CLEAN_TICK * ones(ngpu)
const MAX_BRACKETS = 12

timestamp() = Dates.format(now(), "mmddHHMM")

"""
    an array of gpu status in which `true` means free
"""
function gpustatus()
    stats = ""
    for n in 1:100
        try
            stats = read(`nvidia-smi`, String)
            break
        catch
            print("nvidia-smi err, retry $n...")
            sleep(1 + rand())
        end
    end
    m = match(r"GPU\s*PID\s*Type\s*Process", stats)
    processes = stats[m.offset:end]
    gpus = trues(ngpu)
    for m in eachmatch(r"\|\s+(\d+)\s+\d+\s+C", processes)
        gpus[parse(Int, m.captures[1]) + 1] = false
    end
    return gpus
end

"""
    jobs are `.sh` or `.bk` files in job queue dir.
"""
function nextjob()
    jobs = filter(f->endswith(f, ".sh") || endswith(f, ".sh.bk"), readdir(jobqueue))
    return length(jobs) > 0 ? jobs[1] : nothing
end

"""
    next available gpu.

    gpu is available only if
    * gpu is free, and
    * no job claim that gpu
"""
function nextgpu()
    gpus = gpustatus()
    for i in findall(gpus)
        i - 1 ∉ VISIBLE_GPU && continue
        if(length(glob([Regex("^\\[$(i-1)-RUN\\d+].*\\.sh\$")], jobroot)) == 0)
            return i - 1
        end
    end
    return nothing
end

"""
    stop a job by throwing its .bk file to stop folder.
"""
function stop_job(job)
    println("try stop $job")
    jobstr = replace(replace(job, "[" => "\\["), "]" => "\\]")
    ps = filter(x -> strip(x) != "", split(read(ignorestatus(pipeline(
        `ps -aux`, `grep $jobstr`)), String), "\n"))
    if length(ps) == 1
        m = match(r"\S+\s+(\d+)\s", string(ps))
        try
            println("stopping job $job by cmd $(`kill -9 -$(m.captures[1])`)")
            run(`kill -9 -$(m.captures[1])`)
            println("stopped job $job")
            # move files
            newname = "[STOP$(timestamp())]$job"
            mv(joinpath(jobstop, "$job.bk"), joinpath(jobroot, "$newname.bk"))
            println("mv .bk to $newname.bk")
            if isfile(joinpath(jobroot, job))
                mv(joinpath(jobroot, job), joinpath(jobroot, newname))
                println("mv script to $newname")
            end
            if isfile(joinpath(jobroot, "$job.log"))
                mv(joinpath(jobroot, "$job.log"), joinpath(jobroot, "$newname.log"))
                println("mv log to $newname.log")
            end
        catch err
            println("error when stopping $job, message: $err")
            newname = "[STOPFAIL$(timestamp())]$job"
            mv(joinpath(jobstop, "$job.bk"), joinpath(jobstop, "$newname.bk"))
            println("mv .bk to $newname.bk")
        end
    else
        println("already stopped job $job")
        newname = "[STOPPED$(timestamp())]$job"
        # mv file
        mv(joinpath(jobstop, "$job.bk"), joinpath(jobstop, "$newname.bk"))
        println("mv .bk to $newname.bk")
    end
end

function check_stop()
    stoplist = glob([Regex("^\\[\\d+-RUN\\d+].*\\.sh.bk\$")], jobstop)
    for s in stoplist
        stop_job(basename(s)[1:end-3])
    end
end

function check_resume()
    resumelist = glob(glob"*.sh.bk", jobresume)
    for s in resumelist
        script = strip(read(s, String))
        if length(script) > 2 && script[1] == '"' && script[end] == '"'
            script = strip(script[2:end-1])
        elseif script == ""
            script = "exit 1\necho Empty script!"
        end
        if !occursin("--restore", script)
            script *= " --restore"
        end
        jobname = basename(s)[1:end-3]
        f = open(joinpath(jobqueue, "[RES$(timestamp())]$jobname"), "w")
        println(f, script)
        close(f)
        rm(s)
        println("generate restore script $jobname")
    end
end

"""
    process job.

    * move to job root
    * backup
    * generate running script
        * redirect stdout/stderr
        * if success, mark as done and move self to job done
        * if fail, mark as err
"""
function process_job(job, gpu)
    println("processing $job on gpu $gpu")
    # check job type: normal or bk
    if endswith(job, ".sh.bk")
        jobname = replace(replace(job, r"\[.*\]" => ""), ".bk" => "")
    else
        jobname = job
    end
    jobname = "[$gpu-RUN$(timestamp())]$jobname"
    brackets = collect(eachmatch(r"\[.*?\]", jobname))
    if length(brackets) > MAX_BRACKETS
        cutstart = brackets[MAX_BRACKETS + 1].offset
        cutend = brackets[end].offset + length(brackets[end].match) - 1
        jobname = jobname[1:cutstart-1] * jobname[cutend+1:end]
    end
    jobfile = joinpath(jobroot, jobname)

    # backup script
    mv(joinpath(jobqueue, job), "$jobfile.bk")
    println("backup to $jobname.bk")

    # build running script
    f = open(jobfile, "w")
    script = replace(strip(read("$jobfile.bk", String)), "\$GPU" => gpu)
    if length(script) > 2 && script[1] == '"' && script[end] == '"'
        script = strip(script[2:end-1])
    elseif script == ""
        script = "exit 1\necho Empty script!"
    end
    lines = split(script, "\n", keepempty=false)
    lines[end] = "stdbuf -oL " * lines[end]
    script = join(lines, "\n")
    ismove = length(collect(eachmatch(r"ERR", jobname))) < RETRY

    println(f, """
#!/usr/bin/sh
# redirect output to log file
$script >> '$jobfile.log' 2>&1
# post-process
if [ \$? -eq 0 ]; then
    DATE=\$(date +%m%d%H%M)
    mv '$jobfile' "$jobdone/[DONE\$DATE]$jobname"
    mv '$jobfile.bk' "$jobdone/[DONE\$DATE]$jobname.bk"
    mv '$jobfile.log' "$jobdone/[DONE\$DATE]$jobname.log"
    echo OK
else
    DATE=\$(date +%m%d%H%M)
    mv '$jobfile' "$jobroot/[$gpu-ERR\$DATE]$jobname"
    mv '$jobfile.bk' "$jobroot/[$gpu-ERR\$DATE]$jobname.bk"
    mv '$jobfile.log' "$jobroot/[$gpu-ERR\$DATE]$jobname.log"
    if [ $ismove = true ]; then
        cp "$jobroot/[$gpu-ERR\$DATE]$jobname.bk" "$jobresume"
    fi
    echo FAIL
fi""")
    close(f)
    run(`chmod +x $jobfile`)
    println("generate script $jobname")

    # execute script
    Shell.runfile(jobfile, background=true)
    ticks[gpu+1] = CLEAN_TICK
    println("execute script $jobname")
end

"""
    if a gpu is free for `CLEAN_TICK` seconds and a running job own that gpu,
    then mark this job as unknown.
"""
function check_unknown()
    stats = gpustatus()
    for (i, s) in enumerate(stats)
        if s
            suspects = glob([Regex("^\\[$(i-1)-RUN\\d+].*\\.sh\$")], jobroot)
            if length(suspects) > 0
                ticks[i] -= 1
                if ticks[i] == 0
                    for s in suspects
                        println("unknown detected $s")
                        newname = "[$(i-1)-ORZ$(timestamp())]$(basename(s))"
                        # rename to unknown
                        mv(s, joinpath(jobroot, newname))
                        println("rename script to $newname")
                        # rename log to unknown
                        logname = "$s.log"
                        if isfile(logname)
                            mv(logname, joinpath(jobroot, "$newname.log"))
                            println("rename log to $newname.log")
                        end
                        # rename bk to unknown
                        bkname = "$s.bk"
                        if isfile(bkname)
                            mv(bkname, joinpath(jobroot, "$newname.bk"))
                            println("rename backup to $newname.bk")
                        end
                    end
                else
                    continue
                end
            end
        end
        ticks[i] = CLEAN_TICK
    end
end

while true
    check_stop()
    check_unknown()
    check_resume()

    job = nextjob()

    if job != nothing
        gpu = nextgpu()
        if gpu != nothing
            process_job(job, gpu)
        end
    end

    sleep(1)
end
