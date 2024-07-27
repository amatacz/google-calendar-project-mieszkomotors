import os.path
import json
import logging

from google.auth.transport.requests import Request
from google.oauth2 import service_account 
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.cloud import secretmanager


# logger = logging.getLogger("google_events_logger")

class GoogleServiceIntegrator:
    def __init__(self):
        self.creds = None
        self.google_drive_service = None
        self.google_calendar_service = None

    def access_secret_version(self, project_id, secret_id, version_id="latest"):
        name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
        response = secretmanager.SecretManagerServiceClient().access_secret_version(name=name)
        return response.payload.data.decode('UTF-8')

    def get_google_services(self):
        """Authenticate and create Google Drive and Calendar services using service account credentials."""
        
        project_id = '481715545022'
        secret_id = 'google-calendar-key'

        try:
            # Access the secret from Secret Manager
            key_data = self.access_secret_version(project_id, secret_id)
            service_account_info = json.loads(key_data)

            # Define the scopes required
            SCOPES = [
                "https://www.googleapis.com/auth/drive.metadata.readonly",
                "https://www.googleapis.com/auth/calendar",
                "https://www.googleapis.com/auth/calendar.events"
            ]

            # Authenticate using the service account credentials
            credentials = service_account.Credentials.from_service_account_info(
                service_account_info, scopes=SCOPES)

            # Build the Google Drive service
            self.google_drive_service = build("drive", "v3", credentials=credentials)
            # logger.info("Google Drive Service created.")
            print("Google Drive Service created.")

            # Build the Google Calendar service
            self.google_calendar_service = build("calendar", "v3", credentials=credentials)
            # logger.info("Google Calendar Service created.")
            print("Google Calendar Service created.")

        except Exception as e:
           # logger.error(f"Error occurred while creating Google services: {e}")
            print(f"Error occurred while creating Google services: {e}")
            raise e
        
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
        print("ZNALEZIONE ITEMKI")
        print(items)

        if not items:
            # logger.error("No source files found.")
            print("No source files found.")
            return None
        for item in items:
            if item['name'] == file_name:
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
                # logger.info("No follow up events found.")
                print("No follow up events found.")
                return []
        except HttpError as e:
            # logger.error(f"An error occurred: {e}")
            print(f"An error occurred: {e}")

    def create_follow_up_events(self, event: dict):
        '''
        Create all 4 follow up events for events from this month fetched by get_dict_of_events_from_timeframe()
        '''
        for follow_up_number in range(1, 5):
            try:
                description_html_string = f'Skontaktuj się z<br><b>{event["Imię"]} {event["Nazwisko"]}</b>, właścicielem auta <i>{event["Model"]} {event["Marka"]}</i>.<hr>Dane kontaktowe:<ul><li>Nr telefonu: <a href="tel:{event["Nr_telefonu"]}">{event["Nr_telefonu"]}</a></li><li>E-mail: {event["Adres_e-mail"]}</li></ul><hr>'
                event_dict_follow_up = {
                'summary': f'{event["Imię"]} {event["Nazwisko"]} - FOLLOW UP {follow_up_number}',
                'description': description_html_string,
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
                'colorId': '3'
                }
                new_calendar_event = self.google_calendar_service.events().insert(calendarId='primary', body=event_dict_follow_up).execute()
                # logger.info(f'Event created: {new_calendar_event.get('summary')}')
                print(f'Event created: {new_calendar_event.get('summary')}')
            except Exception as e:
                # logger.error(f"Creating event for {event["Imię"]} {event["Nazwisko"]} - {event["Marka"]} {event["Model"]} did not succeed.")
                print(f"Creating event for {event["Imię"]} {event["Nazwisko"]} - {event["Marka"]} {event["Model"]} did not succeed.")


    def validate_if_event_already_exists_in_calendar(self, existing_events_list, event_to_be_created) -> bool:
        # Get list of existing events summaries from existing events fetched from calendar (cutting off 2 last chars to avoid issue with follow up number -> follow ups are created in batches, all at once)
        existing_events_list_summaries = [existing_event["summary"][:-2] for existing_event in existing_events_list]
        # Create summary string for event that we want to validate
        event_to_be_created_summary = f'{event_to_be_created["Imię"]} {event_to_be_created["Nazwisko"]} - FOLLOW UP'

        # Validate if event that we want to create already exists
        if event_to_be_created_summary in existing_events_list_summaries:
            # logger.info(f"Event {event_to_be_created_summary} already exits!")
            print(f"Event {event_to_be_created_summary} already exits!")
            return False
        else:
                # logger.info(f"This event does not exists. Creating event for {event_to_be_created["Imię"]} {event_to_be_created["Nazwisko"]}")
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
        
        # logger.info("Events removed from calendar")
        print("Events removed from calendar")