#!/usr/bin/env julia
using JSON
using Glob
using Suppressor
using Dates

runners = filter(x -> strip(x) != "", split(read(ignorestatus(pipeline(
    `ps -U $(ENV["USER"]) -ux`, `grep '[0-9]*:[0-9]*\s*julia\s*.*population.jl'`)), String), "\n"))

if length(runners) == 0
    error("cannot detect self, the programe must be wrong!")
elseif length(runners) > 1
    println(join(runners, "\n"))
    warn("population already running, exiting...")
    exit()
end


CONFIG_FILE = "./config.json"
CONFIG = JSON.parse(read(CONFIG_FILE, String))
const TOTAL_STAGE = ceil(Int, CONFIG["total_steps"] / CONFIG["step_interval"])

const populationroot = expanduser("./jobs/population")
const jobdeploy = expanduser("~/jobs/queue")
const jobdone = expanduser("~/jobs/done")
const jobroot = joinpath(populationroot, CONFIG["name"])
const jobscript = joinpath(jobroot, "script")
const jobresult = joinpath(jobroot, "result") # results
const jobhistory = joinpath(jobroot, "history")
const logfile = joinpath(jobroot, "population.log")
mkpath.([populationroot, jobroot, jobscript, jobresult, jobhistory])

stage_history_path(id) = joinpath(jobhistory, "stage$(lpad(id, 4, "0")).json")
save_stage(stage) = open(f -> println(f, json(stage, 4)), stage_history_path(stage["id"]), "w")

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
            stage["best_persion"] = argmax(rewards)
            stage["best_reward"] = stage["population"][stage["best_persion"]]["reward"]
            stage["next_vip"] = stage["population"][stage["best_persion"]]["config"]
            stage["next_weight_dir"] = stage["population"][stage["best_persion"]]["weight_dir"]
            save_stage(stage)

            vip = stage["next_vip"]
            weight_dir = stage["next_weight_dir"]
            println("vip $vip selected for its reward $(maximum(rewards))")
            id += 1
        else
            println("stage $id unfinished, continuing...")
            println("stage $id current status:\n",
                    join(["$(p["runname"]): $(p["status"]), config [$(join(p["config"], ", "))]"
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
    if length(population) == 0
        println("no population candidate, exiting")
        return nothing
    end

    stage["population"] = Dict.(["status" => "start"], Pair.("config", population))

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
        lines[end] *= " --maps '{$(join(["$m: $n" for (m, n) in zip(config["envs"], p["config"])], ", "))}'"
        lines[end] *= " --updates $(config["step_interval"])"
        lines[end] *= " --max_update $(stage["max_update"])"
        lines[end] *= " --run_id $(p["runname"])"
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
        p["weight_dir"] = "$weight(rand(Int))"
        p["reward"] = rand(1:10)
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
    println(json(config))
    # get where we are
    stage = next_stage(config) # fill vip for this stage, error if cannot
    while stage != nothing
        println("now on stage $(stage["id"]) / $TOTAL_STAGE")
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
        stage = next_stage(config) # fill vip for this stage, error if cannot
    end
end

main(CONFIG)
