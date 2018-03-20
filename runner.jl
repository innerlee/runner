#!/usr/bin/env julia
using Shell

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

function nextjob()
    jobs = readdir(jobqueue)
    return length(jobs) > 0 ? jobs[1] : nothing
end

function nextgpu()
    stats = readstring(`nvidia-smi`)
    m = match(r"GPU\s*PID\s*Type\s*Process", stats)
    processes = stats[m.offset:end]
    gpus = trues(ngpu)
    for m in eachmatch(r"\|\s+(\d+)\s+\d+\s+C", processes)
        gpus[parse(Int, m.captures[1]) + 1] = false
    end
    gpu = findfirst(gpus)

    return gpu == 0 ? nothing : gpu - 1
end

for i ∈ 1:10
    job = nextjob()
    if job != nothing
        gpu = nextgpu()
        if gpu != nothing
            cmd = replace(readstring(joinpath(jobqueue, job)), raw"$GPU", gpu)
            Shell.run(cmd)
        end
    end

    sleep(1)
end
