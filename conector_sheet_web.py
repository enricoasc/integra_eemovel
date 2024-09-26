from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import pandas as pd
import os
from send_mail import notifica as ntf
import inspect
from dotenv import load_dotenv


class web_sheets_import:
    load_dotenv()
    def __init__(self):
        # Definindo o escopo e autenticando
        self.SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

        # The ID and range of a sample spreadsheet.
        self.SAMPLE_SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
        self.SAMPLE_RANGE_NAME = "Página1!A:B"
        self.FILE_PATCH = os.path.join(os.getcwd(), 'client_secret.json')
        self.creds = None

    def import_df(self,df_dados):
        if os.path.exists("token.json"):
            self.creds = Credentials.from_authorized_user_file("token.json", self.SCOPES)
        # If there are no (valid) credentials available, let the user log in.
        if not self.creds or not self.creds.valid:
            if self.creds and self.creds.expired and self.creds.refresh_token:
             self.creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    "client_secret.json", self.SCOPES
                )
                self.creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open("token.json", "w") as token:
                token.write(self.creds.to_json())

        try:
            service = build("sheets", "v4", credentials=self.creds)

            # Call the Sheets API
            sheet = service.spreadsheets()
            result = (
                sheet.values()
                .get(spreadsheetId=self.SAMPLE_SPREADSHEET_ID, range=self.SAMPLE_RANGE_NAME)
                .execute()
            )
            values = result.get("values", [])
            pos_ini = 'Página1!A'+str(len(values)+1) 

            nova_lista = df_dados.values.tolist()
            
            ## EXPORTA PARA PLANILHA WEB 
            ## #########################################################################
            result = (
            sheet.values()
            .update(spreadsheetId=self.SAMPLE_SPREADSHEET_ID, 
                    range=pos_ini, 
                    valueInputOption='USER_ENTERED', 
                    body={'values':nova_lista} )
            .execute()
            )

            return ''
        
        except HttpError as err:
            ntf().envia(f'''Erro de execução  
                            \nClasse : {self.__class__.__name__} 
                            \nFunção : {inspect.currentframe().f_code.co_name}  
                            \nErro   : {err}''')
            return err



