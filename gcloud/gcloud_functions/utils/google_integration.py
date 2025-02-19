import json
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseUpload
from google.auth.transport import requests
from google.cloud.secretmanager import SecretManagerServiceClient 
import os
from datetime import timedelta
from io import BytesIO
import requests
import openpyxl

class GoogleServiceIntegrator:
    def __init__(self):
        self.creds = None
        self.google_drive_service = None
        self.google_calendar_service = None
        self.gmail_service = None
        self.target_calendar_id = os.getenv("TARGET_CALENDAR_ID")


    def get_secret(self, project_id, secret_id, version_id="latest"):
        """
        Get value of given secret from Secret Manager
        """
        name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
        response = SecretManagerServiceClient().access_secret_version(name=name)
        return response.payload.data.decode('UTF-8')
    
    def get_smtp_config(self):
        name = "projects/481715545022/secrets/smtp_config/versions/latest"
        response = SecretManagerServiceClient().access_secret_version(name=name)
        secret_data = response.payload.data.decode('UTF-8').replace("'", "\"")
        # Parsuj JSON na słownik
        smtp_config = json.loads(secret_data)

        return smtp_config

    
    def update_secret(self, project_id, secret_id, secret_value):
        """
        Update secret with new secret if expired and save it to secrets manager obj
        """
        parent = SecretManagerServiceClient().secret_path(project_id, secret_id)
        SecretManagerServiceClient().add_secret_version(
            request={"parent": parent, "payload": {"data": secret_value.encode("UTF-8")}}
            )
        
    def get_credentials(self, project_id, secret_id):
        """
        Authenticate user using secrets from Secret Manager.
        Update token if expired.
        """
        # Define the scopes required
        SCOPES = [
            'https://www.googleapis.com/auth/calendar',
            'https://www.googleapis.com/auth/calendar.events',
            'https://www.googleapis.com/auth/drive.metadata.readonly',
            'https://www.googleapis.com/auth/drive.file',
            'https://www.googleapis.com/auth/drive',
            'https://www.googleapis.com/auth/spreadsheets'
        ]
        
        creds_json = self.get_secret(project_id, secret_id)
        creds_data = json.loads(creds_json)

        creds = service_account.Credentials.from_service_account_info(creds_data, scopes = SCOPES) 
        return creds

    def get_google_services(self):
        """Authenticate and create Google Drive and Calendar services using service account credentials."""
        
        PROJECT_ID = os.getenv("PROJECT_ID")
        SECRET_ID = os.getenv("SECRET_ID")
        
        try:
            # Access the secret from Secret Manager
            credentials = self.get_credentials(PROJECT_ID, SECRET_ID)

            # Build the Google Drive service
            self.google_drive_service = build("drive", "v3", credentials=credentials)
            print("Google Drive Service created.")

            # Build the Google Calendar service
            self.google_calendar_service = build("calendar", "v3", credentials=credentials)
            print("Google Calendar Service created.")

            # Build the Gmail service
            self.gmail_service = build("gmail", "v1", credentials=credentials)
            print("Gmail Service created.")


        except Exception as e:
            print(f"Error occurred while creating Google services: {e}")
            raise e
        
        return self.google_drive_service, self.google_calendar_service, self.gmail_service

    def get_source_file_url(self,
                            file_name: str = "MieszkoMotors_praca.xlsx",
                            format: str = "xlsx"):
        # Call the Drive v3 API
        results = (
            self.google_drive_service.files().list(pageSize=10, fields="nextPageToken, files(id, name)").execute()
        )
        items = results.get("files", [])

        if not items:
            print("No source files found.")
            return None
        for item in items:
            if item['name'] == file_name:
                return f"https://docs.google.com/spreadsheets/d/{item['id']}/export?format={format}"

    def get_events_list(self, start_date=None, end_date=None, type_of_event=None):
        # Getting current datetime, but in UTC in isoformat:
        # formatting and cutting off last 3 digits to get rid of microseconds) and append 'Z'
        start_date_formatted = start_date.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
        end_date_formatted = end_date.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'

        type_of_event_description_dict = {"car_registration": "rejestracja auta", "car_inspection": "przegląd techniczny",
                                          "car_insurance": "ubezpieczenie samochodu", "follow_up_1": "pierwszy follow up",
                                          "follow_up_2": "drugi follow up", "follow_up_3": "trzeci follow up"}        

        events_result = (
            self.google_calendar_service.events().list(
                calendarId=self.target_calendar_id,
                q=type_of_event_description_dict[type_of_event],
                timeMin=start_date_formatted,
                timeMax=end_date_formatted,
                singleEvents=True,
                orderBy="startTime",
            ).execute()
        )
        events = events_result.get("items", [])

        try: 
            if events:
                return events
            else:
                print(f"No {type_of_event} events found.")
                return []
        except HttpError as e:
            print(f"An error occurred: {e}")

    def create_event_in_calendar(self, event: dict, type_of_event: str):
        """
        Create event reminder for event from this month fetched by get_events_from_timeframe()
        """
        if type_of_event=="car_registration":
            event[type_of_event] = event[type_of_event] + timedelta(days=10)

        type_of_event_description_dict = {"car_registration": "rejestracja auta", "car_inspection": "przegląd techniczny",
                                          "car_insurance": "ubezpieczenie samochodu", "follow_up_1": "pierwszy follow up",
                                          "follow_up_2": "drugi follow up", "follow_up_3": "trzeci follow up"}
        

        try:
            description_html_string = f'Skontaktuj się z<br><b>{event["first_name"]} {event["last_name"]}</b>, właścicielem auta <i>{event["model"]} {event["brand"]}</i>. W związku z {type_of_event_description_dict[type_of_event]} dnia {event[type_of_event]}<hr>Dane kontaktowe:<ul><li>Nr telefonu: <a href="tel:{event["phone_number"]}">{event["phone_number"]}</a></li><li>E-mail: {event["e-mail"]}</li></ul><hr>'
            event_dict = {
                    'summary': f'{event["first_name"]} {event["last_name"]} - {type_of_event_description_dict[type_of_event]} - {event["brand"]} {event["model"]}',
                    'description': description_html_string,
                    'start': {
                        'dateTime': event[type_of_event].strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z',
                        'timeZone': 'Europe/Warsaw',
                    },
                    'end': {
                        'dateTime': event[type_of_event].strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z',
                        'timeZone': 'Europe/Warsaw',
                    },
                    'reminders': {
                        'useDefault': False,
                        'overrides': [
                        {'method': 'email', 'minutes': 24 * 60},
                        {'method': 'popup', 'minutes': 8 * 60},
                        ],
                    },
                    'transparency': 'transparent',
                    'visibility': 'private',
                    'colorId': '3'
                    }
            new_calendar_event = self.google_calendar_service.events().insert(calendarId=self.target_calendar_id, body=event_dict).execute()
            print(f'Event created: {new_calendar_event.get("summary")}')
        except Exception as e:
            print(f'Creating event for {event["first_name"]} {event["last_name"]} - {type_of_event_description_dict[type_of_event]} - {event["brand"]} {event["model"]} did not succeed: {e}')

    def create_events_for_next_month(self, event_start_date, event_end_date, events_to_be_created, type_of_event):
        if not events_to_be_created:
            print("No upcoming follow up events - cannot proceed with events creation. Skipping to Insurance Events checking...")
        else:
            # Get follow up events from given timeframe
            existing_next_month_events = self.get_events_list(event_start_date, event_end_date, type_of_event)

            if not existing_next_month_events:
                print("No existing events for following moth. Proceed with events creation.")
                try:
                    for event in events_to_be_created.values():
                        self.create_event_in_calendar(event, type_of_event)
                except Exception as e:
                    print(f"Error while creating events {e}.")
            else:
                for event_to_be_created in events_to_be_created.values():
                    if self.validate_if_event_already_exists_in_calendar(existing_next_month_events, event_to_be_created, type_of_event):
                        # Create events in Google Calendar if event is not present in calendar
                        self.create_event_in_calendar(event_to_be_created, type_of_event)
                print(f"Process of creating {type_of_event} events finished successfully.")

    def validate_if_event_already_exists_in_calendar(self, existing_events_list, event_to_be_created, type_of_event) -> bool:


        type_of_event_description_dict = {"car_registration": "rejestracja auta", "car_inspection": "przegląd techniczny",
                                          "car_insurance": "ubezpieczenie samochodu", "follow_up_1": "pierwszy follow up",
                                          "follow_up_2": "drugi follow up", "follow_up_3": "trzeci follow up"}        

        # Get list of existing events summaries from existing events fetched from calendar
        existing_events_list_summaries = [existing_event["summary"] for existing_event in existing_events_list]

        # Create summary string for event that we want to validate
        event_to_be_created_summary = f'{event_to_be_created["first_name"]} {event_to_be_created["last_name"]} - {type_of_event_description_dict[type_of_event]} - {event_to_be_created["brand"]} {event_to_be_created["model"]}'

        # Validate if event that we want to create already exists
        if event_to_be_created_summary in existing_events_list_summaries:
            print(f"Event {event_to_be_created_summary} already exits!")
            return False
        else:
            print(f"""This event does not exists. Creating event for {event_to_be_created["first_name"]} {event_to_be_created["last_name"]}""")
            return True


    def remove_events_from_calendar(self, query = None, start_date = None, end_date = None):
        # Getting current datetime, but in UTC in isoformat:
        # formatting and cutting off last 3 digits to get rid of microseconds) and append 'Z'
        start_date_formatted = start_date.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
        end_date_formatted = end_date.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
        # Get list of all follow up events from calendar (uncomment timeMax to remove events from specific period)
        events_result = (
            self.google_calendar_service.events().list(
                calendarId=self.target_calendar_id,
                q=query,
                timeMin=start_date_formatted,
                # timeMax=end_date_formatted,
                singleEvents=True,
                orderBy="startTime",
            ).execute()
        )
        events = events_result.get("items", [])

        for event in events:
            self.google_calendar_service.events().delete(calendarId=self.target_calendar_id, eventId=event['id']).execute()
        
        print("Events removed from calendar")

    def update_xlsx_cell(self, file_url: str, column_name: str, row_index: int, new_value: str):
        """Aktualizuje komórkę w pliku XLSX"""
        try:
            # Pobierz plik
            response = requests.get(file_url, verify=False)
            if response.status_code != 200:
                raise Exception(f"Failed to download file: {response.status_code}")

            # Wczytaj do pamięci
            excel_data = BytesIO(response.content)
            # Wczytaj istniejący workbook z zachowaniem formatowania
            wb = openpyxl.load_workbook(excel_data)
            ws = wb.active

            # Znajdź indeks kolumny
            header_row = next(ws.rows)
            column_index = None
            for idx, cell in enumerate(header_row, 1):
                if cell.value == column_name:
                    column_index = idx
                    break

            if column_index is None:
                raise ValueError(f"Column {column_name} not found in file")

            # Aktualizuj konkretną komórkę
            ws.cell(row=row_index+1, column=column_index, value=new_value)

            # Zapisz do bufora
            output = BytesIO()
            wb.save(output)
            output.seek(0)

            # Upload zaktualizowanego pliku
            file_id = file_url.split('spreadsheets/d/')[1].split('/')[0]
            media = MediaIoBaseUpload(
                output,
                mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                resumable=True,
            )

            self.google_drive_service.files().update(
                fileId=file_id,
                media_body=media,
                fields='id',
                supportsAllDrives=True
            ).execute()

            return True

        except Exception as e:
            print(f"Error updating Excel file: {str(e)}")
            return False