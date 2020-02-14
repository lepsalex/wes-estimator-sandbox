import json
from urllib import request

WES_BASE = "http://wes.flow.infra.icgc-argo.org/api/v1/runs/"


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


def getWesRun(id):
    with request.urlopen("{}{}".format(WES_BASE, id.strip())) as response:
        data = json.loads(response.read())
        return {
            "runId": data["run_id"],
            "params": data["request"]["workflow_params"],
            "start": data["run_log"]["start_time"],
            "end": data["run_log"]["end_time"],
            "duration": data["run_log"]["duration"],
            "tasks": list(filter(None, map(processTasks, data["task_logs"])))
        }


def getWesRuns(wesIds):
    return map(getWesRun, wesIds)


def extractTaskInfo(id, task):
    return [id, task["tag"], task["realtime"], task["cpus"]]


def extractRunInfo(run):
    return [extractTaskInfo(run["runId"], task) for task in run["tasks"] if task["process"] == "align"]
