import pandas as pd
import requests
import os
from io import BytesIO
from requests.exceptions import HTTPError, RequestException, Timeout, ConnectionError


class DataTransformer:

    def read_source_file_from_path(self, path=None):
        """
        Read source excel file from given path
        Args:
            path (str): path fo excel source file
        Returns:
            df: data frame
        Raises:
            FileNotFoundError: When the Excel file is not found
            ValueError: When the required sheet or columns are missing
            Exception: For other errors during file reading or processing
        """
        try:
            file_path = path if path else "./MieszkoMotors_praca.xlsx"
            sheet_name = "Wykaz_realizacji"

            # Check if file exists
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"Excel file not found at: {file_path}")
            
            # Read Excel file
            try:
                df = pd.read_excel(file_path, sheet_name)
                return df
            except ValueError as e:
                raise ValueError(f"Sheet '{sheet_name}' not found in file: {file_path}")
            
        except Exception as e:
            # Log the error
            raise Exception(f"Failed to read source file: {str(e)}")

    def load_source_file_from_gdrive(self, url: str):
        """
        Loads excel source file from google drive.
        Args:
            url (str): URL to source file stored on Google Drive
        Returns:
            DataFrame: DataFrame with data from source file
        Raises:
            ValueError: When URL is invalid or empty
            HTTPError: When there's an HTTP-related issue
            IOError: When there's an issue reading the data
            Exception: For any other unexpected errors
        """

        # Turn off SSL certificate warnings 
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        if not url or not isinstance(url, str):
            raise ValueError("URL must be a non-empty string")

        try:
            # Load file from URL
            response = requests.get(url, verify=False)
            response.raise_for_status()  # Raises HTTPError for bad responses

            try:
                data = BytesIO(response.content)
                if len(response.content) == 0:
                    raise IOError("Downloaded file is empty")
                    
                df = pd.read_excel(data)
                if df.empty:
                    raise ValueError("Loaded Excel file contains no data")
                    
                return df            
            except pd.errors.EmptyDataError:
                raise ValueError("Excel file is empty or has no valid data")
            except pd.errors.ParserError as e:
                raise IOError(f"Failed to parse Excel file: {str(e)}")
            except Exception as e:
                raise IOError(f"Error processing Excel data: {str(e)}")            
            
        except HTTPError as e:
            raise HTTPError(f"HTTP Error: {str(e)}")
        except ConnectionError as e:
            raise HTTPError(f"Connection Error: {str(e)}")
        except Timeout as e:
            raise HTTPError(f"Timeout Error: {str(e)}")
        except RequestException as e:
            raise HTTPError(f"Request failed: {str(e)}")
        except Exception as e:
            raise Exception(f"Unexpected error loading file: {str(e)}")


    def transform_file(self, df):
        """
        Extracts required columns and corrects datatypes.
        Args:
            df: DataFrame with client data
        Returns:
            df: DataFrame with useful columns extracted and datatypes corrected
        Raises:
            ValueError: When the required dataframe is missing
            KeyError: When the required colum name is missing 
            Exception: For other errors during file reading or processing
        """
        pd.options.mode.chained_assignment = None

        required_columns = ["No.", "collaboration_start_date",  "collaboration_end_date",  "first_name",
        "last_name",  "city",  "phone_number",  "e-mail",  "brand",  "model", "follow_up_1",  "follow_up_2",
        "follow_up_3", "car_inspection", "car_insurance", "car_registration", "car_inspection_reminder",
        "car_insurance_reminder", "car_registration_reminder"]
        reminder_columns = ["car_inspection_reminder", "car_insurance_reminder", "car_registration_reminder"]
        datetime_columns = ["collaboration_start_date", "collaboration_end_date", "follow_up_1", "follow_up_2", "follow_up_3", "car_inspection", "car_insurance", "car_registration"]
        
        # Verify all required columns exist
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")        
        
        # Choose only useful columns from df
        df_transformed = df[required_columns]
        
        # Sort and extract columns
        try:
            df_transformed = df.sort_values("follow_up_1")
            df_transformed = df_transformed[required_columns]

            # Format phone number to be clickable on mobile calendar
            df_transformed["phone_number"] = df_transformed["phone_number"].astype("str").apply(lambda x: x.replace(" ", ""))

            # For email reminder status columns change datatype to str
            df_transformed[reminder_columns] = df_transformed[reminder_columns].astype("str").apply(lambda x: x.replace(" ", ""))
    
            # Format dates to datetime type
            df_transformed[datetime_columns] = df_transformed[datetime_columns].apply(pd.to_datetime, errors='coerce')
        except KeyError as e:
            raise KeyError(f"Error during sorting: {str(e)}")
        except Exception as e:
            raise Exception(f"Error processing dataframe: {str(e)}")    

        return df_transformed

    def get_dict_of_events_from_timeframe(self, df, event_start_day, event_end_day, type_of_event):
        """
        Chosses events from events dataframe that are within given start and end date.
        Args:
            df: DataFrame with client data
            event_start_day: Datetime
            event_end_day: Datetime
            type_of_event: string with type of event
        Returns:
            dict: dictionary with relevant events and details
        Raises:
            ValueError: When the required dataframe is empty
            Exception: For other errors during file reading or processing
        """

        try:
            # Get only relevant events
            events_df = df[(df[type_of_event] >= event_start_day) & (df[type_of_event] <= event_end_day)]
            events_df.reset_index(drop=True, inplace=True)

            # Save events as dictionary
            events_dict = events_df.to_dict(orient="index")
            return events_dict
        except Exception as e:
            print(f"Error occured while catching {type_of_event} events from {event_start_day} and {event_end_day}: {e}")



