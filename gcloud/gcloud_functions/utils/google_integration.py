from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.api_core.exceptions import GoogleAPIError, Conflict
from googleapiclient.http import MediaIoBaseUpload
from google.auth.transport import requests
from google.cloud.secretmanager import SecretManagerServiceClient 
from google.cloud import bigquery
from google.cloud.bigquery import WriteDisposition
from datetime import timedelta, datetime
from io import BytesIO, StringIO
import os
import json
import requests
import openpyxl
import urllib3



class GoogleServiceIntegrator:
    def __init__(self):
        self.creds = None
        self.google_drive_service = None
        self.google_calendar_service = None
        self.gmail_service = None
        self.bigquery_client = None

        self.target_calendar_id = os.getenv("TARGET_CALENDAR_ID")
        self.PROJECT_ID = os.getenv("PROJECT_ID")
        self.SECRET_ID = os.getenv("SECRET_ID")
        self.SOURCE_FILE_URL = os.getenv("SOURCE_FILE_URL")

        self.EVENT_TYPES = {
            "car_registration": "rejestracja auta",
            "car_inspection": "przegląd techniczny",
            "car_insurance": "ubezpieczenie samochodu",
            "follow_up_1": "pierwszy follow up",
            "follow_up_2": "drugi follow up",
            "follow_up_3": "trzeci follow up"
        }

    def get_secret(self, project_id, secret_id, version_id="latest"):
        """
        Extracts  value of given secret from Secret Manager.
        Args:
            project_id: string with project_id 
            secret_id: string with secret_id
            version_id: string, secret version, by default the latest
        Returns:
            str: decoded secret value
        Raises:
            ValueError: When the required parameters are missing
            Exception: For other errors during file reading or processing
        """
        if not project_id or not secret_id:
            raise ValueError("project_id and secret_id must be non empty strings")
        
        # Concatenate all info into secret name
        name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
        try:
            response = SecretManagerServiceClient().access_secret_version(name=name)
        except Exception as e:
            raise Exception(f"Error during getting secret from Secret Manager Client: {e}")
        return response.payload.data.decode('UTF-8')
    
    def get_smtp_config(self, project_id=None, secret_id=None):
        """
        Extracts smtp_config from SecretManager by invoking get_secret function.
        Args:
            project_id: str with project id, if not provided got from env
            secret_id: str with secret id, if not provided got from env
        Returns:
            str: decoded smtp config value
        """        
        if not project_id:
            project_id = os.getenv("PROJECT_ID")
        if not secret_id:
            secret_id = os.getenv("SMTP_CONFIG_SECRET")

        # return self.get_secret(project_id, secret_id)
    
        secret_data = self.get_secret(project_id, secret_id).replace("'", "\"")
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
        
    def get_service_account_credentials(self, project_id=None, secret_id=None):
        """
        Getting service account credentials using secrets from Secret Manager.
        Args:
            project_id: optional str with project id
            secret_id: optional str with secret id
        Returns:
            str: credentials needed to authenticate user
        Raises:
            ValueError: When the required dataframe is missing
            KeyError: When the required colum name is missing 
            Exception: For other errors during file reading or processing
        """


        # Define the scopes required
        SCOPES = [
            'https://www.googleapis.com/auth/calendar',
            'https://www.googleapis.com/auth/calendar.events',
            'https://www.googleapis.com/auth/drive.metadata.readonly',
            'https://www.googleapis.com/auth/drive.file',
            'https://www.googleapis.com/auth/drive',
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/bigquery',
            'https://www.googleapis.com/auth/devstorage.full_control'
        ]

        creds_json = self.get_secret(project_id, secret_id)
        creds_data = json.loads(creds_json)
        if not creds_data:
            raise ValueError("Credentials cannot be empty JSON")
        
        try:
            creds = service_account.Credentials.from_service_account_info(creds_data, scopes = SCOPES) 
        except Exception as e:
            raise Exception(f"Error occured while getting credentials {e}")

        return creds

    def get_google_services(self):
        """
        Authenticate and create Google services using service account credentials.
        Returns: Google services
        """
        
        
        try:
            # Access the secret from Secret Manager
            credentials = self.get_service_account_credentials(self.PROJECT_ID, self.SECRET_ID)
        except Exception as e:
            raise Exception(f"Issue with getting service account credentials {e}")
        
        try:
            # Build the Google Drive service
            self.google_drive_service = build("drive", "v3", credentials=credentials)
            print("Google Drive Service created.")

            # Build the Google Calendar service
            self.google_calendar_service = build("calendar", "v3", credentials=credentials)
            print("Google Calendar Service created.")

            # Build the Gmail service
            self.gmail_service = build("gmail", "v1", credentials=credentials)
            print("Gmail Service created.")

            # Build the BigQuery Client
            self.bigquery_client = bigquery.Client(credentials=credentials, project=self.PROJECT_ID)
            print("BigQuery Client created.")


        except Exception as e:
            raise Exception(f"Error occurred while creating Google services: {e}")
        
        return self.google_drive_service, self.google_calendar_service, self.gmail_service, self.bigquery_client
    
    def get_source_file_url(self,
                            file_name: str = "MieszkoMotors_praca.xlsx",
                            format: str = "xlsx"):
        """
        Gets URL of source file from files available in Google Drive.
        
        Args:
            file_name (str): Name of the file to search for
            format (str): Desired export format of the file
            
        Returns:
            str: URL of file that will be processed or None if file not found
            
        Raises:
            ValueError: When file_name or format is empty/invalid
            GoogleAPIError: When there's an issue with Google Drive API
            Exception: For unexpected errors
        """
        # Input validation
        if not file_name or not format:
            raise ValueError("File name and format must not be empty")
        
        try:
            # Call the Drive v3 API with specific fields
            results = self.google_drive_service.files().list(
                pageSize=10,  # Consider making this configurable
                fields="files(id, name)",  # Removed nextPageToken as it's not used
                orderBy="modifiedTime desc"  # Optional: get most recent files first
            ).execute()
            
            items = results.get("files", [])
            
            if not items:
                print("No files found in Google Drive")
                return None
                
            # Create files dictionary and get URL
            files_dict = {item['name']: item['id'] for item in items}
            file_id = files_dict.get(file_name)
            
            if not file_id:
                print(f"File '{file_name}' not found in Google Drive")
                return None
                
            return f"https://docs.google.com/spreadsheets/d/{file_id}/export?format={format}"
        except GoogleAPIError as e:
            raise GoogleAPIError(f"GoogleAPIError - Failed to get file URL from Google Drive: {str(e)}.")
        except Exception as e:
            raise Exception(f"Failed to get file URL from Google Drive: {str(e)}")

    def get_events_list(
        self, 
        start_date: datetime = None, 
        end_date: datetime = None, 
        type_of_event: str = None
    ) -> list:
        """
        Retrieves list of events from Google Calendar within specified date range.
        
        Args:
            start_date (datetime): Start date for events search
            end_date (datetime): End date for events search
            type_of_event (str): Type of event to search for (e.g. 'car_registration')
            
        Returns:
            list: List of calendar events or empty list if none found
            
        Raises:
            ValueError: When required parameters are missing or invalid
            HttpError: When Google Calendar API request fails
            Exception: For other unexpected errors
        """
        # Input validation
        if not all([start_date, end_date, type_of_event]):
            raise ValueError("start_date, end_date, and type_of_event are required")
            
        if start_date > end_date:
            raise ValueError("start_date cannot be later than end_date")
        
        if type_of_event not in self.EVENT_TYPES:
            raise ValueError(f"Invalid type_of_event. Must be one of: {', '.join(self.EVENT_TYPES.keys())}")

        try:
            # Format dates for API (ISO format with 'Z' suffix)
            start_date_formatted = self._format_date_for_api(start_date)
            end_date_formatted = self._format_date_for_api(end_date)
            
            # Get events from Google Calendar
            events_result = self.google_calendar_service.events().list(
                calendarId=self.target_calendar_id,
                q=self.EVENT_TYPES[type_of_event],
                timeMin=start_date_formatted,
                timeMax=end_date_formatted,
                singleEvents=True,
                orderBy="startTime"
            ).execute()
            
            events = events_result.get("items", [])
            
            if not events:
                print(f"No {type_of_event} events found.")
                
            return events
            
        except HttpError as e:
            print(f"Google Calendar API error: {str(e)}")
            raise
        except Exception as e:
            raise Exception(f"Failed to retrieve events: {str(e)}")

    def create_events_for_next_month(self, event_start_date, event_end_date, events_to_be_created, type_of_event):
        """
        Creates calendar events for the next month based on provided events list.
        
        Args:
            event_start_date (datetime): Start date for the event search range
            event_end_date (datetime): End date for the event search range
            events_to_be_created (dict): Dictionary of events to create
            type_of_event (str): Type of events to create
            
        Raises:
            ValueError: When required parameters are missing
            Exception: For errors during event creation or validation
        """

        # # Input validation
        # if not all([event_start_date, event_end_date, events_to_be_created, type_of_event]):
        #     raise ValueError("start_date, end_date, events_to_be_created and type_of_event are required")
        
        try:
            # Get calendar events from given timeframe that already exists
            existing_next_month_events = self.get_events_list(event_start_date, event_end_date, type_of_event)

            # Case 1: No existing events, create all new events
            if not existing_next_month_events:
                print(f"No existing {type_of_event} events for following moth. No validation needed.")
                print(f"Proceed with creating {len(events_to_be_created)} events...")

                created_count = 0
                for event in events_to_be_created.values():
                    try:
                        self._create_event_in_calendar(event, type_of_event)
                        created_count += 1
                    except Exception as e:
                        print(f"Error creating event for {event.get('first_name', '')} {event.get('last_name', '')}: {str(e)}")
                print(f"Successfully created {created_count} of {len(events_to_be_created)} events.")
            # Case 2: Existing events found, validate before creating
            else:
                print(f"Found {len(existing_next_month_events)} existing events for next month.")
                print(f"Validating {len(events_to_be_created)} events before creation...")
                
                created_count = 0
                skipped_count = 0
            
                for event_to_be_created in events_to_be_created.values():
                    try:
                        if self._validate_if_event_can_be_created_in_calendar(
                            existing_next_month_events, event_to_be_created, type_of_event
                            ):
                            self._create_event_in_calendar(event_to_be_created, type_of_event)
                            created_count += 1
                        else:
                            skipped_count += 1
                    except Exception as e:
                        print(f'Error processing event for {event_to_be_created["first_name"]} {event_to_be_created["last_name"]}: {str(e)}')
                print(f"Process completed: {created_count} events created, {skipped_count} events skipped (already exist).")
            print(f"Process of creating {type_of_event} events finished successfully.")
        except Exception as e:
            raise Exception(f"Failed to create {type_of_event} events: {str(e)}")

    def _validate_if_event_can_be_created_in_calendar(self, existing_events_list, event_to_be_created, type_of_event) -> bool:
        """
        Function invoked to avoid events duplication.
        Validates if calendar event already exists by looking for summary of this event in list of summaries of calendar events
        that already exits in given timeframe.

        Args:
            existing_events_list (list): list of events that already exists in calendar for given timeframe
            event_to_be_created (dict): event that should be validated if already exists in calendar
            type_of_event (str): string with type of event
        Returns:
            bool: True -> if event does not exists in calendar and can be created
                  False -> if event already exists in calendar and should not be created
        Raises:
            ValueError: When required parameters are missing
            ValueError: When type_of_event is invalid            
            Exception: for unexpected errors
        """
        # Validate inputs
        if not all([existing_events_list, event_to_be_created, type_of_event]):
            raise ValueError("existing_events_list, event_to_be_created, type_of_event are required")
        
        if type_of_event not in self.EVENT_TYPES:
            raise ValueError(f"Invalid type_of_event. Must be one of: {', '.join(self.EVENT_TYPES.keys())}")
    
        
        # Create summary string for event that we want to validate
        event_to_be_created_summary = (
            f'{event_to_be_created["first_name"]} {event_to_be_created["last_name"]} - '
            f'{self.EVENT_TYPES[type_of_event]} - '
            f'{event_to_be_created["brand"]} {event_to_be_created["model"]}'
        )
        try: 
            # Check if event summary exists in list of existing event summaries
            for existing_event in existing_events_list:
                if existing_event.get("summary") == event_to_be_created_summary:
                    print(f"Event {event_to_be_created_summary} already exits!")
                    return False
            
            print(f"""This event does not exist. Creating event for {event_to_be_created["first_name"]} {event_to_be_created["last_name"]} - {self.EVENT_TYPES[type_of_event]}""")
            return True

        except Exception as e:
            raise Exception(f"Issue occured when event was validated: {e}")
        
    def _create_event_in_calendar(self, event: dict, type_of_event: str):
        """
        Create event reminder for event from this month fetched by get_events_from_timeframe().
        Args:
            event (dict): dictionary with details of event that should be created in calendar
            type_of_event (str): string with type of event that should be created in calendar
        Raises:
            ValueError: When event or type_of_event is empty/invalid
            GoogleAPIError: When there's an issue with Google Drive API
            Exception: For unexpected errors
        """
        # Validate input
        if not all([event, type_of_event]):
            raise ValueError("event, type_of_event are required")
        
        try:
            # Add 10 days to car registration event date - when client should pick up car documents
            if type_of_event=="car_registration":
                event[type_of_event] = event[type_of_event] + timedelta(days=10)

            # Create event description in HTML format
            description_html_string = (
                f'Skontaktuj się z<br>'
                f'<b>{event["first_name"]} {event["last_name"]}</b>, '
                f'właścicielem auta <i>{event["model"]} {event["brand"]}</i>. '
                f'W związku z {self.EVENT_TYPES[type_of_event]} dnia {event[type_of_event]}<hr>'
                f'Dane kontaktowe:<ul>'
                f'<li>Nr telefonu: <a href="tel:{event["phone_number"]}">{event["phone_number"]}</a></li>'
                f'<li>E-mail: {event["email"]}</li>'
                f'</ul><hr>'
            )
            
            # Create event summary
            summary_string = (
                f'{event["first_name"]} {event["last_name"]} - '
                f'{self.EVENT_TYPES[type_of_event]} - '
                f'{event["brand"]} {event["model"]}'
            )

            event_dict = {
                    'summary': summary_string,
                    'description': description_html_string,
                    'start': {
                        'dateTime': self._format_date_for_api(event[type_of_event]),
                        'timeZone': 'Europe/Warsaw',
                    },
                    'end': {
                        'dateTime': self._format_date_for_api(event[type_of_event]),
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
            new_calendar_event = self.google_calendar_service.events().insert(
                calendarId=self.target_calendar_id,
                body=event_dict).execute()
            
            print(f'Event created: {new_calendar_event.get("summary")}')
            return new_calendar_event
        except GoogleAPIError as e:
            raise GoogleAPIError(f"Google Calendar API error during event creation: {str(e)}")
        except Exception as e:
            raise Exception(f'Failed to create event: {str(e)}')
        
    def _format_date_for_api(self, date: datetime) -> str:
        """
        Formats datetime object for Google Calendar API.
        
        Args:
            date (datetime): Date to format
            
        Returns:
            str: Formatted date string
        """
        return date.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'

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

    def _create_bigquery_dataset(self, dataset_name):
        ''' Creates new dataset in BigQuery project.'''
        dataset = bigquery.Dataset(f"{self.PROJECT_ID}.{dataset_name}")  # create dataset
        try:
            dataset = self.bigquery_client.create_dataset(dataset, timeout=30)  # make API call
        except Conflict:
            print(f"Dataset {dataset_name} already exists.")
            pass
        except Exception as e:
            print(f"Error occured: {e}")
            pass

    def _create_bigquery_table(self, dataset_name, table_name, schema):

        """
        Create new table in BigQuery project and dataset
        """
        table_id = f"{self.PROJECT_ID}.{dataset_name}.{table_name}"  # create table_id

        try:
            table = bigquery.Table(table_id, schema=schema)
            table = self.bigquery_client.create_table(table)
        except Conflict:
             print(f"Table {table_id} already exists.")

    def insert_data_from_df_to_bigquery_table(self, data, dataset_name, table_name, schema=None):
        ''' Inserts data from DataFrame to BigQuery table '''

        if not schema:
                schema = [                
            bigquery.SchemaField("No", "STRING"),
            bigquery.SchemaField("collaboration_start_date", "DATE"),
            bigquery.SchemaField("collaboration_end_date", "DATE"),
            bigquery.SchemaField("first_name", "STRING"),
            bigquery.SchemaField("last_name", "STRING"),
            bigquery.SchemaField("city", "STRING"),
            bigquery.SchemaField("phone_number", "STRING"),
            bigquery.SchemaField("email", "STRING"),
            bigquery.SchemaField("brand", "STRING"),
            bigquery.SchemaField("model", "STRING"),
            # bigquery.SchemaField("financing", "STRING"),
            # bigquery.SchemaField("seller", "STRING"),
            # bigquery.SchemaField("brutto_commission", "NUMERIC"),
            # bigquery.SchemaField("insurance", "STRING"),
            # bigquery.SchemaField("contact_source", "STRING"),
            # bigquery.SchemaField("seller_representative", "STRING"),
            # bigquery.SchemaField("insurance_agent", "STRING"),
            # bigquery.SchemaField("financing_agent", "STRING"),
            bigquery.SchemaField("follow_up_1", "DATE"),
            bigquery.SchemaField("follow_up_2", "DATE"),
            bigquery.SchemaField("follow_up_3", "DATE"),
            bigquery.SchemaField("car_inspection", "DATE"),
            bigquery.SchemaField("car_insurance", "DATE"),
            bigquery.SchemaField("car_registration", "DATE"),
            bigquery.SchemaField("car_inspection_reminder", "STRING"),
            bigquery.SchemaField("car_insurance_reminder", "STRING"),
            bigquery.SchemaField("car_registration_reminder", "STRING")
        ]

        table_id = f"{self.PROJECT_ID}.{dataset_name}.{table_name}"  # choose the destination table
        job_config = bigquery.LoadJobConfig(schema=schema)  # choose table schema
        try:
            job = self.bigquery_client.load_table_from_dataframe(
                data, table_id, job_config=job_config)  # Upload the contents of a table from a DataFrame
            job.result()  # Start the job and wait for it to complete and get the result
            print("DATA UPLOADED")
        except Exception as e:
            print("Error occured: ", e)        
        
    def create_dataset_table_and_insert_data(self, dataset_name, table_name, schema, data):
        # create BigQuery dataset
        self._create_bigquery_dataset(dataset_name)
        # create BigQueryTable
        self._create_bigquery_table(dataset_name, table_name, schema=schema)
        # populate table with data
        self._insert_data_from_df_to_bigquery_table(data, dataset_name, table_name, schema=None)

    # def upload_data_to_cloud_from_file(self, bucket_name, data_to_upload, blob_name):
    #     ''' Uploads files with api data to GCP buckets. '''
    #     bucket = self.storage_client.bucket(bucket_name) # connect to bucket
    #     blob = bucket.blob(blob_name)  # create a blob
    #     with open(data_to_upload, "rb") as file:
    #         blob.upload_from_file(file)  # upload data to blob

    def upload_data_to_cloud_from_url(self, bucket_name, data_to_upload, blob_name):
        ''' Uploads files with api data to GCP buckets. '''

        # csv_content = StringIO()
        # data_to_upload.to_csv(csv_content, index=False, sep=',')
        # csv_content.seek(0)


        bucket = self.storage_client.bucket(bucket_name) # connect to bucket
        blob = bucket.blob(blob_name)  # create a blob
        # Ustawienie typu zawartości
        blob.content_type = 'text/csv'

        blob.upload_from_file(data_to_upload, content_type='text/csv')

        # Zwracamy URI pliku, który można użyć w BigQuery
        gcs_uri = f"gs://{bucket_name}/{blob_name}"