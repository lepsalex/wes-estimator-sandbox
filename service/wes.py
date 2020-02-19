import json
import requests
from urllib import request
import aiohttp
import asyncio

WES_BASE = "http://wes.flow.infra.icgc-argo.org/api/v1/runs/"
MIN_MEM = 20


def getWesRuns(wesIds):
    loop = asyncio.get_event_loop()
    coroutines = [getWesRun(wesId) for wesId in wesIds]
    return loop.run_until_complete(asyncio.gather(*coroutines))


async def getWesRun(wesId):
    async with aiohttp.ClientSession() as session:
        async with session.get("{}{}".format(WES_BASE, wesId.strip())) as response:
            data = await response.json()
            return {
                "runId": data["run_id"],
                "params": data["request"]["workflow_params"],
                "start": data["run_log"]["start_time"],
                "end": data["run_log"]["end_time"],
                "duration": data["run_log"]["duration"],
                "tasks": list(filter(None, map(processTasks, data["task_logs"])))
            }


def startWesRuns(paramsList, tokenFile="api_token", scoreTokenFile="score_api_token"):
    # Get tokens from files
    token_f = open(tokenFile, 'r')
    api_token = token_f.readline().strip()
    token_f.close()

    score_token_f = open(scoreTokenFile, 'r')
    score_api_token = score_token_f.readline().strip()
    score_token_f.close()

    loop = asyncio.get_event_loop()
    coroutines = [startVariableParamsRun(params, api_token, score_api_token) for params in paramsList]
    return loop.run_until_complete(asyncio.gather(*coroutines))


# currently running Semaphore at 1 due to wes api start runs not able to handle more
async def startVariableParamsRun(params, api_token, score_api_token, semaphore = asyncio.Semaphore(1)):
    async with semaphore:
        async with aiohttp.ClientSession() as session:
            cpus = params["cpus"]
            mem = max((params["cpus"] * 3) + 2, MIN_MEM)
            nfs = params["nfs"]
            analysisId = params["analysisId"]

            payload = {
                "workflow_url": "icgc-argo/nextflow-dna-seq-alignment",
                "workflow_params": {
                    "study_id": "PANCHOPE-CA",
                    "analysis_id": analysisId,
                    "song_url": "https://song.qa.argo.cancercollaboratory.org",
                    "score_url": "https://score.qa.argo.cancercollaboratory.org",
                    "api_token": api_token,
                    "reference_dir": "/{}/reference/GRCh38_hla_decoy_ebv".format(nfs),
                    "aligned_lane_prefix": "grch38-aligned",
                    "aligned_basename": "wgs.grch38",
                    "download": {
                        "song_url": "https://intermediate-song.qa.argo.cancercollaboratory.org",
                        "song_cpus": 2,
                        "song_mem": 2,
                        "score_url": "https://storage.cancercollaboratory.org",
                        "score_api_token": score_api_token,
                        "score_cpus": 8,
                        "score_mem": 18
                    },
                    "seqtolanebam": {
                        "cpus": cpus,
                        "mem": mem
                    },
                    "align": {
                        "cpus": cpus,
                        "mem": mem
                    },
                    "merge": {
                        "cpus": 4,
                        "mem": 18
                    },
                    "sequencing_alignment_payload_gen": {
                        "cpus": 2,
                        "mem": 2
                    },
                    "upload": {
                        "song_cpus": 2,
                        "song_mem": 2,
                        "score_cpus": 8,
                        "score_mem": 18
                    }
                },
                "workflow_engine_params": {
                    "work_dir": "/{}/work".format(nfs)
                }
            }

            async with session.post(WES_BASE, json=payload) as response:
                data = await response.json()
                # return format for easy write into sheets as column
                return [data["run_id"]]


def processTasks(task):
    if task["state"] == "COMPLETE" and task["exit_code"] != "0":
        return {
            "process": task["process"],
            "tag": task["tag"],
            "cpus": task["cpus"],
            "memory": abs(task["memory"]),
            "duration": task["duration"],
            "realtime": task["realtime"]
        }


def extractTaskInfo(id, task):
    return [id, task["tag"], task["realtime"], task["cpus"]]


def extractRunInfo(run):
    return [extractTaskInfo(run["runId"], task) for task in run["tasks"] if task["process"] == "align"]
