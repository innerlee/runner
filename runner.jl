#!/usr/bin/env julia
using Shell
using Glob

#=
[0-RUN-180321-130035]comp-0001.sh
[0-UNKOWN-180321-130155][0-RUN-180321-130035]comp-0001.sh
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

logger = open(logfile, "a")
println(logger, "[$(now())] start")

const ngpu = parse(Int, readstring(pipeline(`nvidia-smi -L`, `wc -l`)))
println(logger, "$ngpu GPUs detected.")

const CLEAN_TICK = 2
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
    # check job type: normal or bk
    if job[end-2:end] == ".bk"
        jobname = replace(replace(job, r"\[.*\]", ""), ".bk", "")
    else
        jobname = job
    end
    jobname = "[$gpu-RUN-$(timestamp())]$jobname"
    jobfile = joinpath(jobroot, jobname)

    # backup script
    mv(joinpath(jobqueue, job), "$jobfile.bk"))

    # build running script
    f = open(jobfile, "w")
    script = replace(strip(readstring("$jobfile.bk"))), raw"$GPU", gpu)
    println(f, "#!/usr/bin/sh")
    println(f, "# redirect output to log file")
    println(f, "$script >> '$("$jobfile.log"))' 2>&1")
    println(f, "# post-process")
    println(f, """
if [ \$? -eq 0 ]; then
    mv '$(jobfile)' '$jobdone'
    mv '$(jobfile).bk' '$jobdone'
    mv '$(jobfile).log' '$jobdone'
    echo OK
else
    DATE=$(date +%y%m%d"-"%H%M%S)
    mv '$(jobfile)' '$jobroot/[$gpu-ERR-\$DATE]$jobname'
    mv '$(jobfile).bk' '$jobroot/[$gpu-ERR-\$DATE]$jobname.bk'
    mv '$(jobfile).log' '$jobroot/[$gpu-ERR-\$DATE]$jobname.log'
    echo FAIL
fi
""")
    close(f)
    run(`chmod +x $jobfile`)

    # execute script
    Shell.runfile(jobfile, background=true)
end

"""
    if a gpu is free for `CLEAN_TICK` seconds and a running job own that gpu,
    then mark this job as unkown.
"""
function check_unkown()
    stats = gpustatus()
    for (i, s) in enumerate(stats)
        if s
            suspects = glob([Regex("^\\[$(i-1)-RUN-\\d+-\\d+].*\.sh\$")], jobroot)
            if length(suspects) > 0
                ticks[i] -= 1
                if ticks[i] == 0
                    for s in suspects
                        # rename to unkown
                        mv(s, joinpath(dirname(s), "[$(i-1)-UNKOWN-$(timestamp())]$(basename(s))"))
                        # rename log to unkown
                        logname = "$s.log"
                        if isfile(logname)
                            mv(logname, joinpath(dirname(logname), "[$(i-1)-UNKOWN-$(timestamp())]$(basename(logname))"))
                        end
                        # rename bk to unkown
                        bkname = "$s.bk"
                        if isfile(bkname)
                            mv(bkname, joinpath(dirname(bkname), "[$(i-1)-UNKOWN-$(timestamp())]$(basename(bkname))"))
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

for i ∈ 1:10
    #0. do some clean work
    check_unkown()

    job = nextjob()

    if job != nothing
        gpu = nextgpu()
        if gpu != nothing
            process_job(job, gpu)
        end
    end

    sleep(1)
end
