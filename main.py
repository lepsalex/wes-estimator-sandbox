from service.sheets import Sheet
from service.wes import getWesRuns, extractRunInfo

# The ID and range of the spreadsheet.
SPREADSHEET_ID = "1oQBjKw9eJbI1ViDiGOTU3i87tBPnn6TGbhQfs6B1_tA"

# RANGES
RUN_ID_COL_SHORT = "'AutoAlign'!A2:A7"
RUN_ID_COL = "'AutoAlign'!A2:A39"
WRITE_START = "'AutoAlign'!C2"

autoAlign = Sheet(SPREADSHEET_ID)

wesRunIds = autoAlign.readColumn(RUN_ID_COL_SHORT)

runs = list(getWesRuns(wesRunIds))

align_tasks = [item for sublist in [extractRunInfo(run) for run in runs] for item in sublist]

print(list(align_tasks))
