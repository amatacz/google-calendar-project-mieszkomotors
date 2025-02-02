import json
from google.oauth2.credentials import Credentials
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.cloud.secretmanager import SecretManagerServiceClient 
from google.auth.transport.requests import Request
import os


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
            "https://www.googleapis.com/auth/drive.metadata.readonly",
            "https://www.googleapis.com/auth/calendar",
            "https://www.googleapis.com/auth/calendar.events"
        ]
        

        creds_json = self.get_secret(project_id, secret_id)
        creds_data = json.loads(creds_json)

        creds = service_account.Credentials.from_service_account_info(creds_data, scopes = SCOPES)
        # creds = Credentials.from_authorized_user_info(creds_data)
        # creds_expiration_date = creds.expired
        # creds_refresh_token = creds.refresh_token
        
        # if creds and creds.expired and creds.refresh_token:
        #     creds.refresh(Request())
        #     self.update_secret(project_id, secret_id, creds.to_json())
        #     return creds
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
                return f"https://docs.google.com/spreadsheets/d/{item["id"]}/export?format={format}"


    # FOLLOW UP EVENTS SECTION
    def get_follow_up_events_list(self, start_date = None, end_date = None):
        # Getting current datetime, but in UTC in isoformat:
        # formatting and cutting off last 3 digits to get rid of microseconds) and append 'Z'
        start_date_formatted = start_date.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
        end_date_formatted = end_date.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'

        events_result = (
            self.google_calendar_service.events().list(
                calendarId=self.target_calendar_id,
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
        Create all 3 follow up events for events from this month fetched by get_dict_of_follow_up_events_from_timeframe()
        '''

        for follow_up_number in range(1, 4):
            try:
                description_html_string = f'Skontaktuj się z<br><b>{event["Imię"]} {event["Nazwisko"]}</b>, właścicielem auta <i>{event["Model"]} {event["Marka"]}</i>.<hr>Dane kontaktowe:<ul><li>Nr telefonu: <a href="tel:{event["Nr_telefonu"]}">{event["Nr_telefonu"]}</a></li><li>E-mail: {event["Adres_e-mail"]}</li></ul><hr>'
                event_dict_follow_up = {
                'summary': f'{event["Imię"]} {event["Nazwisko"]} - FOLLOW UP {follow_up_number} - {event["Marka"]} {event["Model"]}',
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
                new_calendar_event = self.google_calendar_service.events().insert(calendarId=self.target_calendar_id, body=event_dict_follow_up).execute()
                print(f'Event created: {new_calendar_event.get("summary")}')
            except Exception as e:
                print(f'Creating event for {event["Imię"]} {event["Nazwisko"]} - {event["Marka"]} {event["Model"]} did not succeed.')

    # INSURANCE EVENTS SECTION
    def get_insurance_events_list(self, start_date=None, end_date=None):
        # Getting current datetime, but in UTC in isoformat:
        # formatting and cutting off last 3 digits to get rid of microseconds) and append 'Z'
        start_date_formatted = start_date.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
        end_date_formatted = end_date.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'

        events_result = (
            self.google_calendar_service.events().list(
                calendarId=self.target_calendar_id,
                q="Ubezpieczenie samochodu",
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
                print("No insurance events found.")
                return []
        except HttpError as e:
            print(f"An error occurred: {e}")

    def create_insurance_event(self, event: dict):
        """
        Create insurance reminder for event from this month fetched by get_dict_of_insurance_events_from_timeframe()
        """
        try:
            description_html_string = f'Skontaktuj się z<br><b>{event["Imię"]} {event["Nazwisko"]}</b>, właścicielem auta <i>{event["Model"]} {event["Marka"]}</i>. Ubezpieczenie kończy się dnia {event["Ubezpieczenie samochodu"]}<hr>Dane kontaktowe:<ul><li>Nr telefonu: <a href="tel:{event["Nr_telefonu"]}">{event["Nr_telefonu"]}</a></li><li>E-mail: {event["Adres_e-mail"]}</li></ul><hr>'
            event_dict_follow_up = {
                    'summary': f'{event["Imię"]} {event["Nazwisko"]} - Ubezpieczenie samochodu - {event["Marka"]} {event["Model"]}',
                    'description': description_html_string,
                    'start': {
                        'dateTime': event["Ubezpieczenie samochodu"].strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z',
                        'timeZone': 'Europe/Warsaw',
                    },
                    'end': {
                        'dateTime': event["Ubezpieczenie samochodu"].strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z',
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
            new_calendar_event = self.google_calendar_service.events().insert(calendarId=self.target_calendar_id, body=event_dict_follow_up).execute()
            print(f'Event created: {new_calendar_event.get("summary")}')
        except Exception as e:
            print(f'Creating event for {event["Imię"]} {event["Nazwisko"]} - {event["Marka"]} {event["Model"]} did not succeed.')
        

    # CAR INSPECTION SECTION
    def get_car_inspection_events_list(self, start_date=None, end_date=None):
        # Getting current datetime, but in UTC in isoformat:
        # formatting and cutting off last 3 digits to get rid of microseconds) and append 'Z'
        start_date_formatted = start_date.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
        end_date_formatted = end_date.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'

        events_result = (
            self.google_calendar_service.events().list(
                calendarId=self.target_calendar_id,
                q="Przegląd techniczny",
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
                print("No car inspection events found.")
                return []
        except HttpError as e:
            print(f"An error occurred: {e}")

    def create_car_inspection_event(self, event: dict):
        """
        Create car inspection reminder for event from this month fetched by get_dict_of_car_inspection_events_from_timeframe()
        """
        try:
            description_html_string = f'Skontaktuj się z<br><b>{event["Imię"]} {event["Nazwisko"]}</b>, właścicielem auta <i>{event["Model"]} {event["Marka"]}</i>. Przegląd techniczny auta kończy się dnia {event["Przegląd techniczny"]}<hr>Dane kontaktowe:<ul><li>Nr telefonu: <a href="tel:{event["Nr_telefonu"]}">{event["Nr_telefonu"]}</a></li><li>E-mail: {event["Adres_e-mail"]}</li></ul><hr>'
            event_dict_follow_up = {
                    'summary': f'{event["Imię"]} {event["Nazwisko"]} - Przegląd techniczny - {event["Marka"]} {event["Model"]}',
                    'description': description_html_string,
                    'start': {
                        'dateTime': event["Przegląd techniczny"].strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z',
                        'timeZone': 'Europe/Warsaw',
                    },
                    'end': {
                        'dateTime': event["Przegląd techniczny"].strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z',
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
            new_calendar_event = self.google_calendar_service.events().insert(calendarId=self.target_calendar_id, body=event_dict_follow_up).execute()
            print(f'Event created: {new_calendar_event.get("summary")}')
        except Exception as e:
            print(f'Creating event for {event["Imię"]} {event["Nazwisko"]} - {event["Marka"]} {event["Model"]} did not succeed.')




    def validate_if_event_already_exists_in_calendar(self, existing_events_list, event_to_be_created, type_of_event) -> bool:

        # Get list of existing events summaries from existing events fetched from calendar
        # (cutting off 2 last chars to avoid issue with follow up number -> follow ups are created in batches, all at once)
        existing_events_list_summaries = [existing_event["summary"][:-2] for existing_event in existing_events_list]

        # Create summary string for event that we want to validate
        event_to_be_created_summary = f'{event_to_be_created["Imię"]} {event_to_be_created["Nazwisko"]} - {type_of_event} - {event_to_be_created["Marka"]} {event_to_be_created["Model"]}'

        # Validate if event that we want to create already exists
        if event_to_be_created_summary in existing_events_list_summaries:
            print(f"Event {event_to_be_created_summary} already exits!")
            return False
        else:
            print(f"""This event does not exists. Creating event for {event_to_be_created["Imię"]} {event_to_be_created["Nazwisko"]}""")
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


    def get_events_list(self, type_of_event, start_date=None, end_date=None):
        # Getting current datetime, but in UTC in isoformat:
        # formatting and cutting off last 3 digits to get rid of microseconds) and append 'Z'
        start_date_formatted = start_date.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
        end_date_formatted = end_date.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'

        events_result = (
            self.google_calendar_service.events().list(
                calendarId=self.target_calendar_id,
                q=type_of_event,
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

    def create_event(self, event: dict, type_of_event: str):
        """
        Create event reminder for event from this month fetched by get_events_from_timeframe()
        """
        try:
            description_html_string = f'Skontaktuj się z<br><b>{event["Imię"]} {event["Nazwisko"]}</b>, właścicielem auta <i>{event["Model"]} {event["Marka"]}</i>. f{type_of_event} kończy się dnia {event[type_of_event]}<hr>Dane kontaktowe:<ul><li>Nr telefonu: <a href="tel:{event["Nr_telefonu"]}">{event["Nr_telefonu"]}</a></li><li>E-mail: {event["Adres_e-mail"]}</li></ul><hr>'
            event_dict_follow_up = {
                    'summary': f'{event["Imię"]} {event["Nazwisko"]} - {type_of_event} - {event["Marka"]} {event["Model"]}',
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
            new_calendar_event = self.google_calendar_service.events().insert(calendarId=self.target_calendar_id, body=event_dict_follow_up).execute()
            print(f'Event created: {new_calendar_event.get("summary")}')
        except Exception as e:
            print(f'Creating event for {event["Imię"]} {event["Nazwisko"]} - {type_of_event} - {event["Marka"]} {event["Model"]} did not succeed.')