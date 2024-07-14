from datetime import datetime, timezone
import os.path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from events_functions import GoogleCloudEvents


class GoogleServiceIntegrator:
    creds = None
    google_drive_service = None
    google_calendar_service = None

    def get_google_services(self):
    
        """Shows basic usage of the Drive v3 API.
        Prints the names and ids of the first 10 files the user has access to.
        """
        # If modifying these scopes, delete the file token.json.
        SCOPES = ["https://www.googleapis.com/auth/drive.metadata.readonly",
                  "https://www.googleapis.com/auth/calendar",
                  "https://www.googleapis.com/auth/calendar.events"]

        # The file token.json stores the user's access and refresh tokens, and is
        # created automatically when the authorization flow completes for the first time.
        if os.path.exists("secrets/token.json"):
            self.creds = Credentials.from_authorized_user_file("secrets/token.json", SCOPES)

        # If there are no (valid) credentials available, let the user log in.
        if not self.creds or not self.creds.valid:
            if self.creds and self.creds.expired and self.creds.refresh_token:
                self.creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    "secrets/credentials.json", SCOPES
                )
                self.creds = flow.run_local_server(port=0)
                # Save the credentials for the next run
            with open("secrets/token.json", "w") as token:
                token.write(self.creds.to_json())

        try:
            self.google_drive_service = build("drive", "v3", credentials=self.creds)
            print("Google Drive Service created.")
        except Exception as e:
            print(f"Error occured while creating Google Drive Service: {e}")
        
        try:
            self.google_calendar_service = build("calendar", "v3", credentials=self.creds)
            print("Google Calendar Service created.")
        except Exception as e:
            print(f"Error occured while creating Google Calendar Service: {e}")
            
        return self.google_drive_service, self.google_calendar_service

    def get_source_file_url(self,
                            file_name: str = "MieszkoMotors_praca.xlsx",
                            format: str = "xlsx"):
        # Call the Drive v3 API
        results = (
            self.google_drive_service.files()
            .list(pageSize=10, fields="nextPageToken, files(id, name)")
            .execute()
        )
        items = results.get("files", [])

        if not items:
            print("No files found.")
            return
        for item in items:
            if item['name'] == file_name:
                print(f"File found: {file_name}")
                return f"https://docs.google.com/spreadsheets/d/{item["id"]}/export?format={format}"


    def get_follow_up_events_list(self, start_date = None, end_date = None):
        # Call the Calendar API

        # Getting current datetime, but in UTC in isoformat:
        # formatting and cutting off last 3 digits to get rid of microseconds) and append 'Z'
        start_date_formatted = start_date.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
        end_date_formatted = end_date.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'

        events_result = (
            self.google_calendar_service.events().list(
                calendarId="primary",
                q="FOLLOW UP",
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
                print("No follow up events found.")
                return []
        except HttpError as e:
            print(f"An error occurred: {e}")

    def create_follow_up_events(self, event: dict):
        '''
        Create all 4 follow up events for events from this month fetched by get_dict_of_events_from_timeframe()
        '''
        for follow_up_number in range(1, 5):
            try:
                event_dict_follow_up = {
                'summary': f'{event["Imię"]} {event["Nazwisko"]} - FOLLOW UP {follow_up_number}',
                'description': f'Skontaktuj się z {event["Imię"]} {event["Nazwisko"]} właścicielem auta {event["Model"]} {event["Marka"]}.\nDane kontaktowe: {event["Nr_telefonu"]} i {event["Adres_e-mail"]}',
                'start': {
                    'dateTime': event[f"Follow_up_{follow_up_number}"].strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z',
                    'timeZone': 'Europe/Warsaw',
                },
                'end': {
                    'dateTime': event[f"Follow_up_{follow_up_number}"].strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z',
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
                'colorId': '4'
                }
                new_calendar_event = self.google_calendar_service.events().insert(calendarId='primary', body=event_dict_follow_up).execute()
                print(f'Event created: {new_calendar_event.get('summary')}')
            except Exception as e:
                print(f"Creating event for {event["Imię"]} {event["Nazwisko"]} - {event["Model"]} {event["Marka"]} did not succeed.")


    def validate_if_event_already_exists_in_calendar(self, existing_events_list, event_to_be_created) -> bool:
        # Get list of existing events summaries from existing events fetched from calendar (cutting off 2 last chars to avoid issue with follow up number -> follow ups are created in batches, all at once)
        existing_events_list_summaries = [existing_event["summary"][:-2] for existing_event in existing_events_list]
        # Create summary string for event that we want to validate
        event_to_be_created_summary = f'{event_to_be_created["Imię"]} {event_to_be_created["Nazwisko"]} - FOLLOW UP'

        # Validate if event that we want to create already exists
        if event_to_be_created_summary in existing_events_list_summaries:
            print(f"Event {event_to_be_created_summary} already exits!")
            return False
        else:
                print(f"This event does not exists. Creating event for {event_to_be_created["Imię"]} {event_to_be_created["Nazwisko"]}")
                return True
        
    def remove_events_from_calendar(self, query = None, start_date = None, end_date = None):

        # Getting current datetime, but in UTC in isoformat:
        # formatting and cutting off last 3 digits to get rid of microseconds) and append 'Z'
        start_date_formatted = start_date.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
        end_date_formatted = end_date.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'

        # Get list of all follow up events from calendar (uncomment timeMax to remove events from specific period)
        events_result = (
            self.google_calendar_service.events().list(
                calendarId="primary",
                q=query,
                timeMin=start_date_formatted,
                # timeMax=end_date_formatted,
                singleEvents=True,
                orderBy="startTime",
            ).execute()
        )
        events = events_result.get("items", [])

        for event in events:
            self.google_calendar_service.events().delete(calendarId='primary', eventId=event['id']).execute()
        
        print("ALL CLEANED")