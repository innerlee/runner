#!/usr/bin/env julia
using JSON
using Glob
using Suppressor
using Dates
using Statistics
using Random
using LinearAlgebra

runners = filter(x -> strip(x) != "", split(read(ignorestatus(pipeline(
    `ps -U $(ENV["USER"]) -ux`, `grep '[0-9]*:[0-9]*\s*julia\s*.*population.jl'`)), String), "\n"))

if length(runners) == 0
    error("cannot detect self, the programe must be wrong!")
elseif length(runners) > 1
    println(join(runners, "\n"))
    warn("population already running, exiting...")
    exit()
end

if length(ARGS) != 1
    @error "please specify config file"
end

CONFIG_FILE = ARGS[1]
CONFIG = JSON.parse(read(CONFIG_FILE, String))
const TOTAL_STAGE = ceil(Int, CONFIG["total_steps"] / CONFIG["step_interval"])

const populationroot = expanduser("./jobs/population")
const jobdeploy = expanduser("~/jobs/queue")
const jobdone = expanduser("~/jobs/done")
const jobs = expanduser("~/jobs")
const jobroot = joinpath(populationroot, CONFIG["name"])
const jobscript = joinpath(jobroot, "script")
const jobresult = joinpath(jobroot, "result") # results
const jobhistory = joinpath(jobroot, "history")
const logfile = joinpath(jobroot, "population.log")
mkpath.([populationroot, jobroot, jobscript, jobresult, jobhistory])

stage_history_path(id) = joinpath(jobhistory, "stage$(lpad(id, 4, "0")).json")
save_stage(stage) = open(f -> println(f, json(stage, 4)), stage_history_path(stage["id"]), "w")
vec_str(v) = "[$(join(v, ", "))]"
weighted_mean(r) = round(dot(r, normalize([0.95^i for i in length(r):-1:1], 1)), digits=4)

@suppress Base.println(xs...) = open(f -> (println(f, "[$(now())] ", xs...);
    println(stdout, "[$(now())] ", xs...)), logfile, "a")

function next_stage(config)
    # check history folder, which contais info of each stage
    id = 1
    vip = config["vip"]
    weight_dir = ""
    historyfiles = glob(joinpath(jobhistory, "stage????.json"))
    if length(historyfiles) > 0
        id = maximum(parse.(Int, getindex.(basename.(historyfiles), [6:9])))
    end

    if isfile(stage_history_path(id))
        # check if finished
        stage = JSON.parse(read(stage_history_path(id), String))
        @assert(length(stage["population"]) > 0)
        @assert(stage["id"] == id)
        @assert(length(stage["vip"]) == length(config["vip"]))
        if all([p["status"] == "done" for p in stage["population"]])
            rewards = [p["reward"] for p in stage["population"]]
            println("all population done in stage $id, rewards $rewards")
            vip_id = argmax(rewards)
            stage["best_person"] = vip_id
            stage["best_reward"] = stage["population"][vip_id]["reward"]
            stage["next_vip"] = stage["population"][vip_id]["config"]
            stage["next_weight_dir"] = stage["population"][vip_id]["weight_dir"]
            save_stage(stage)

            vip = stage["next_vip"]
            weight_dir = stage["next_weight_dir"]
            println("VIP person $(vip_id) $(vec_str(vip)) selected for its average reward $(maximum(rewards))")
            id += 1
        else
            println("stage $id unfinished, continuing...")
            println("stage $id current status:\n",
                    join(["$(p["runname"]): $(p["status"]), config $(vec_str(p["config"]))" *
                          (p["status"] == "done" ? ", reward $(p["reward"])" : "")
                          for p in stage["population"]], "\n"))
            return stage
        end
    end

    # end?
    if id > TOTAL_STAGE
        println("all $TOTAL_STAGE stages done")
        return nothing
    end

    # need create a new stage
    stage = Dict(
        "id" => id,
        "vip" => vip,
        "max_update" => config["step_interval"] * id,
        "weight_dir" => weight_dir
    )

    # generate population
    population = filter(p -> all(p .<= config["upper_bound"]) && all(p .>= config["lower_bound"]),
                        [vip] .+ config["variations"])
    if length(population) < config["pool"]
        popul2 = filter(p -> all(p .<= config["upper_bound"]) && all(p .>= config["lower_bound"]),
                        [vip] .+ config["additional_variations"])
        population = vcat(population, popul2[randperm(length(popul2))], repeat([vip], config["pool"]))[1:config["pool"]]
    end
    @assert length(population) == config["pool"]

    if length(population) == 0
        println("no population candidate, exiting")
        return nothing
    end

    stage["population"] = Dict.(Pair.("i", 1:length(population)), ["status" => "start"], Pair.("config", population))

    # save
    save_stage(stage)

    println("stage $id history file generated")
    return stage
end

"""
    population status: start | deployed | runover | done
    call this function to go to next state, return the result state
"""
function process_population(stage, i, config)
    id = stage["id"]
    p = stage["population"][i]
    if p["status"] == "done"
        return true
    elseif p["status"] == "start"
        # gen script and deploy
        p["runname"] = "$(config["name"])-stage$(lpad(id, 4, "0"))-person$(lpad(i, 3, "0"))"
        template = read(config["template"], String)
        if length(template) > 2 && template[1] == '"' && template[end] == '"'
            template = strip(template[2:end-1])
        elseif template == ""
            template = "exit 1\necho Empty template!"
        end
        lines = strip.(split(template, "\n", keepempty=false))
        lines[end] *= " --maps '{$(join(["\"$m\": $n" for (m, n) in zip(config["envs"], p["config"])], ", "))}'"
        lines[end] *= " --max_update $(stage["max_update"])"
        lines[end] *= " --run_id $(p["runname"])"
        lines[end] *= " --save_interval $(min(250, ceil(Int, config["step_interval"] / 4)))"
        lines[end] *= " --num_snapshot 1"
        id > 1 && (lines[end] *= " --remote_restore $(stage["weight_dir"])")
        script = join(lines, "\n")

        scriptfilename = joinpath(jobscript, "$(p["runname"]).sh")
        open(f -> println(f, script), scriptfilename, "w")
        cp(scriptfilename, joinpath(jobdeploy, "$(p["runname"]).sh"), force=true)

        p["status"] = "deployed"
    elseif p["status"] == "deployed"
        # check if done, if not, whether in queue or in jobroot, else print a warn
        doneshfile = glob("\\[DONE*\\]$(p["runname"]).sh", jobdone)
        if length(doneshfile) >= 1
            p["status"] = "runover"
        else
            return false
        end
    elseif p["status"] == "runover"
        # summaryise
        # find log files
        logfiles = unique(vcat(glob("\\[DONE*\\]$(p["runname"]).sh.log", jobdone),
                               glob("\\[STOP*\\]$(p["runname"]).sh.log", jobs),
                               glob("\\[?-ERR*\\]$(p["runname"]).sh.log", jobs)))

        @assert length(logfiles) >= 1
        p["weight_dir"] = strip(split(read(pipeline(`cat $(logfiles[1])`, `grep 'weights are saved at'`, `cut -b 22-`), String), "\n")[1])
        rewards = []
        for f in logfiles
            r = read(pipeline(`cat $f`, `grep $(config["envs"][1] * "_rew_mean")`, `cut -d'|' -f3`), String)
            append!(rewards, parse.(Float32, split(r, "\n", keepempty=false)))
        end
        p["rewards"] = join(rewards, ",")
        p["reward"] = weighted_mean(rewards)
        for f in logfiles
            for g in [f, f[1:end-4], f[1:end-4] * ".bk"]
                isfile(g) && mv(g, joinpath(jobresult, basename(g)))
            end
        end
        println("stage $id population $i config $(vec_str(p["config"])), ",
                "mean reward $(mean(rewards)), std $(std(rewards)), max $(maximum(rewards)), min $(minimum(rewards)), count $(length(rewards))")
        p["status"] = "done"
    else
        println("warn: stage $id population $i in unknown state")
    end

    println("stage $id population $i goto state $(p["status"])")
    return false
end


#--------------------------------------------------

function main(config)
    println("whaaa! a new day!")
    println("config file $CONFIG_FILE loaded\n", json(config))
    # get where we are
    stage = next_stage(config)
    while stage != nothing
        println("now on stage $(stage["id"]) / $TOTAL_STAGE, updates from $(stage["max_update"] - config["step_interval"]) to $(stage["max_update"])")
        n = length(stage["population"])
        while true
            n_done = 0
            for i in 1:n
                n_done += process_population(stage, i, config)
                save_stage(stage)
                sleep(0.5)
            end
            n_done == length(stage["population"]) && break
        end
        stage = next_stage(config)
    end
end

main(CONFIG)
