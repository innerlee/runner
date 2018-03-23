#!/usr/bin/env julia
# Julia v0.6.2
# Pkg.update()
# Pkg.clone("git@github.com:innerlee/Shell.jl")
# Pkg.add("Glob")
# Pkg.add("Suppressor")
using Shell
using Glob
using Suppressor

#=
[0-RUN-180321-130035]job-001.sh
[0-UNKNOWN-180321-130155][0-RUN-180321-130035]job-001.sh
=#

runners = filter(x -> strip(x) != "", split(readstring(ignorestatus(pipeline(
    `ps -aux`, `grep '[0-9]*:[0-9]*\s*julia\s*.*runner.jl'`))), "\n"))

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
const jobtrash = joinpath(jobroot, "trash")
const joblog = joinpath(jobroot, "log")
const logfile = joinpath(joblog, "runner.log")
mkpath.([jobroot, jobqueue, jobdone, jobtrash, joblog])

@suppress Base.println(xs...) = open(f -> (println(f, "[$(now())] ", xs...);
    println(STDOUT, "[$(now())] ", xs...)), logfile, "a")

println("===== start =====")

const ngpu = parse(Int, readstring(pipeline(`nvidia-smi -L`, `wc -l`)))
println("$ngpu GPUs detected.")

const CLEAN_TICK = 60
const ticks = CLEAN_TICK * ones(ngpu)

timestamp() = Dates.format(now(), "yymmdd-HHMMSS")

"""
    an array of gpu status in which `true` means free
"""
function gpustatus()
    stats = readstring(`nvidia-smi`)
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
    jobs = filter(f->length(f)>3 && f[end-2:end] in [".sh", ".bk"], readdir(jobqueue))
    return length(jobs) > 0 ? jobs[1] : nothing
end

"""
    next available gpu.
"""
function nextgpu()
    gpus = gpustatus()
    gpu = findfirst(gpus)
    return gpu == 0 ? nothing : gpu - 1
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
    if job[end-2:end] == ".bk"
        jobname = replace(replace(job, r"\[.*\]", ""), ".bk", "")
    else
        jobname = job
    end
    jobname = "[$gpu-RUN-$(timestamp())]$jobname"
    jobfile = joinpath(jobroot, jobname)

    # backup script
    mv(joinpath(jobqueue, job), "$jobfile.bk")
    println("backup to $jobname.bk")

    # build running script
    f = open(jobfile, "w")
    script = replace(strip(readstring("$jobfile.bk")), "\$GPU", gpu)
    if script[1] == '"' && script[end] == '"'
        script = script[2:end-1]
    end
    println(f, """
#!/usr/bin/sh
# redirect output to log file
$script >> '$jobfile.log' 2>&1
# post-process
if [ \$? -eq 0 ]; then
    DATE=\$(date +%y%m%d"-"%H%M%S)
    mv '$jobfile' "$jobdone/[DONE-\$DATE]$jobname"
    mv '$jobfile.bk' "$jobdone/[DONE-\$DATE]$jobname.bk"
    mv '$jobfile.log' "$jobdone/[DONE-\$DATE]$jobname.log"
    echo OK
else
    DATE=\$(date +%y%m%d"-"%H%M%S)
    mv '$jobfile' "$jobroot/[$gpu-ERR-\$DATE]$jobname"
    mv '$jobfile.bk' "$jobroot/[$gpu-ERR-\$DATE]$jobname.bk"
    mv '$jobfile.log' "$jobroot/[$gpu-ERR-\$DATE]$jobname.log"
    echo FAIL
fi""")
    close(f)
    run(`chmod +x $jobfile`)
    println("generate script $jobname")

    # execute script
    Shell.runfile(jobfile, background=true)
    ticks[gpu] = CLEAN_TICK
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
            suspects = glob([Regex("^\\[$(i-1)-RUN-\\d+-\\d+].*\.sh\$")], jobroot)
            if length(suspects) > 0
                ticks[i] -= 1
                if ticks[i] == 0
                    for s in suspects
                        println("unknown detected $s")
                        newname = "[$(i-1)-UNKNOWN-$(timestamp())]$(basename(s))"
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
    check_unknown()

    job = nextjob()

    if job != nothing
        gpu = nextgpu()
        if gpu != nothing
            disable_sigint() do
                process_job(job, gpu)
            end
        end
    end

    sleep(1)
end
