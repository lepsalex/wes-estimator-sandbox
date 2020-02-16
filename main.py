from service.sheets import Sheet
from service.wes import getWesRuns, startWesRuns, extractRunInfo

# The ID and range of the spreadsheet.
SPREADSHEET_ID = "1oQBjKw9eJbI1ViDiGOTU3i87tBPnn6TGbhQfs6B1_tA"

# RANGES
RUN_ID_COL_TEST = "'AutoAlign'!A2:A7"

RUN_ID_COL = "'AutoAlign'!A2:A82"
ALIGN_WRITE_START = "'AutoAlign'!C2"

RUNS_WRITE_START = "'AutoAlign'!A83"


def readAutoAlign(readCol):
    wesRunIds = autoAlign.readColumn(readCol)
    return getWesRuns(wesRunIds)


def writeAutoAlign(runs, range):
    align_tasks = [item for sublist in [extractRunInfo(run) for run in runs] for item in sublist]
    autoAlign.updateRange(align_tasks, range)


def startNewJobs(params, range):
    newRuns = startWesRuns(params)
    autoAlign.updateRange(newRuns, range)


params_for_test = [
    # {"cpus": 2, "nfs": "nfs-1-c1", "analysisId": "15c2fb45-d531-4735-82fb-45d531573575"},
    # {"cpus": 2, "nfs": "nfs-1-c2", "analysisId": "15c2fb45-d531-4735-82fb-45d531573575"},
    # {"cpus": 36, "nfs": "nfs-1-c3", "analysisId": "15c2fb45-d531-4735-82fb-45d531573575"},
    # {"cpus": 4, "nfs": "nfs-1-c4", "analysisId": "15c2fb45-d531-4735-82fb-45d531573575"},
    # {"cpus": 34, "nfs": "nfs-2-c1", "analysisId": "15c2fb45-d531-4735-82fb-45d531573575"},
    # {"cpus": 32, "nfs": "nfs-2-c2", "analysisId": "15c2fb45-d531-4735-82fb-45d531573575"},
    # {"cpus": 30, "nfs": "nfs-2-c3", "analysisId": "15c2fb45-d531-4735-82fb-45d531573575"},
    # {"cpus": 28, "nfs": "nfs-2-c4", "analysisId": "15c2fb45-d531-4735-82fb-45d531573575"},
    # {"cpus": 26, "nfs": "nfs-3-c1", "analysisId": "15c2fb45-d531-4735-82fb-45d531573575"},
    # {"cpus": 24, "nfs": "nfs-3-c2", "analysisId": "15c2fb45-d531-4735-82fb-45d531573575"},
    # {"cpus": 22, "nfs": "nfs-3-c3", "analysisId": "15c2fb45-d531-4735-82fb-45d531573575"},
    # {"cpus": 20, "nfs": "nfs-3-c4", "analysisId": "15c2fb45-d531-4735-82fb-45d531573575"},
    # {"cpus": 18, "nfs": "nfs-4-c1", "analysisId": "15c2fb45-d531-4735-82fb-45d531573575"},
    # {"cpus": 16, "nfs": "nfs-4-c2", "analysisId": "15c2fb45-d531-4735-82fb-45d531573575"},
    # {"cpus": 14, "nfs": "nfs-4-c3", "analysisId": "15c2fb45-d531-4735-82fb-45d531573575"},
    # {"cpus": 12, "nfs": "nfs-4-c4", "analysisId": "15c2fb45-d531-4735-82fb-45d531573575"}
]

# Init Spreadsheet
autoAlign = Sheet(SPREADSHEET_ID)

# Write align data to sheet
writeAutoAlign(readAutoAlign(RUN_ID_COL), ALIGN_WRITE_START)

# Run new jobs and record run_id to sheet
# startNewJobs(params_for_test, RUNS_WRITE_START)
