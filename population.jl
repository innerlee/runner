#!/usr/bin/env julia
using JSON
using Glob
using Suppressor
using Dates

CONFIG_FILE = "./config.json"
CONFIG = JSON.parse(read(CONFIG_FILE, String))
const TOTAL_STAGE = ceil(Int, CONFIG["total_steps"] / CONFIG["step_interval"])

const populationroot = expanduser("./jobs/population")
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
            vip = stage["population"][argmax(rewards)]["config"]
            println("vip $vip selected for its reward $(maximum(rewards))")
            id += 1
        else
            println("stage $id unfinished, continuing...")
            # return nothing # todo: delete this
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
function process_population(stage, i)
    p = stage["population"][i]
    if p["status"] == "done"
        return true
    elseif p["status"] == "start"
        # gen script and deploy
        p["status"] = "deployed"
    elseif p["status"] == "deployed"
        # check if done, if not, whether in queue or in jobroot, else print a warn
        p["status"] = "deployed"
        p["status"] = "runover"
    elseif p["status"] == "runover"
        # summaryise
        p["reward"] = rand(1:10)
        p["status"] = "done"
    else
        println("warn: stage $(stage["id"]) population $i in unknown state")
    end

    println("stage $(stage["id"]) population $i goto state $(p["status"])")
    return false
end


function gen_scripts(stage, config)

end

#--------------------------------------------------

function main(config)
    println("whaaa! a new day!")
    println(json(config))
    # get where we are
    stage = next_stage(config) # fill vip for this stage, error if cannot
    while stage != nothing
        println("now on stage $(stage["id"]) / TOTAL_STAGE")
        n = length(stage["population"])
        while true
            n_done = 0
            for i in 1:n
                n_done += process_population(stage, i)
                save_stage(stage)
                sleep(0.5)
            end
            n_done == length(stage["population"]) && break
        end
        stage = next_stage(config) # fill vip for this stage, error if cannot
    end
end

main(CONFIG)
