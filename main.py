from service.sheets import Sheet
from service.wes import getWesRuns, extractRunInfo, startVariableParamsRun

# The ID and range of the spreadsheet.
SPREADSHEET_ID = "1oQBjKw9eJbI1ViDiGOTU3i87tBPnn6TGbhQfs6B1_tA"

# RANGES
RUN_ID_COL_SHORT = "'AutoAlign'!A2:A7"
RUN_ID_COL = "'AutoAlign'!A2:A39"
WRITE_START = "'AutoAlign'!C2"

def readAutoAlign():
    autoAlign = Sheet(SPREADSHEET_ID)
    wesRunIds = autoAlign.readColumn(RUN_ID_COL_SHORT)
    return list(getWesRuns(wesRunIds))

# align_tasks = [item for sublist in [extractRunInfo(run) for run in runs] for item in sublist]

params_for_test = [
    {"cpus": 2, "nfs": "nfs-1-c1"},
    {"cpus": 2, "nfs": "nfs-1-c2"},
    {"cpus": 6, "nfs": "nfs-1-c3"},
    {"cpus": 6, "nfs": "nfs-1-c4"},
    {"cpus": 10, "nfs": "nfs-2-c1"},
    {"cpus": 10, "nfs": "nfs-2-c2"},
    {"cpus": 14, "nfs": "nfs-2-c3"},
    {"cpus": 14, "nfs": "nfs-2-c4"},
    {"cpus": 18, "nfs": "nfs-3-c1"},
    {"cpus": 18, "nfs": "nfs-3-c2"},
    {"cpus": 22, "nfs": "nfs-3-c3"},
    {"cpus": 22, "nfs": "nfs-3-c4"},
    {"cpus": 26, "nfs": "nfs-4-c1"},
    {"cpus": 26, "nfs": "nfs-4-c2"},
    {"cpus": 30, "nfs": "nfs-4-c3"},
    {"cpus": 30, "nfs": "nfs-4-c4"}
]

result = map(startVariableParamsRun, params_for_test)

print(list(result))
