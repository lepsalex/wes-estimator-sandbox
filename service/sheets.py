import pickle
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request


class Sheet:

    # If modifying these scopes, delete the file token.pickle.
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

    def __init__(self, spreadsheet_id):
        """Shows basic usage of the Sheets API.
        Prints values from a sample spreadsheet.
        """
        creds = None
        # The file token.pickle stores the user's access and refresh tokens, and is
        # created automatically when the authorization flow completes for the first
        # time.
        if os.path.exists('token.pickle'):
            with open('token.pickle', 'rb') as token:
                creds = pickle.load(token)
        # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    'credentials.json', self.SCOPES)
                creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open('token.pickle', 'wb') as token:
                pickle.dump(creds, token)

        service = build('sheets', 'v4', credentials=creds)

        self.sheet = service.spreadsheets()
        self.spreadsheet_id = spreadsheet_id

    def readColumn(self, range):
        return self.sheet.values().get(spreadsheetId=self.spreadsheet_id,
                                  range=range,
                                  majorDimension='COLUMNS').execute().get('values', [])[0]

    def updateRange(self, data, range):
        result = self.sheet.values().update(
            spreadsheetId=self.spreadsheet_id,
            range=range,
            valueInputOption="RAW",
            body={"values": data}).execute()
        print('{0} cells updated.'.format(result.get('updatedCells')))
