import pandas as pd
import requests
from io import BytesIO
import logging


class DataTransformer:

    def read_source_file_from_path(self, path=None):
        file = pd.read_excel("./MieszkoMotors_praca.xlsx", "Wykaz_realizacji").sort_values("Follow_up_1")
        df_useful_columns_extracted = df[["No.", "collaboration_start_date",  "collaboration_end_date",  "first_name",
            "last_name",  "city",  "phone_number",  "e-mail",  "brand",  "model",
            "follow_up_1",  "follow_up_2",  "follow_up_3", "car_inspection", "car_insurance", "car_registration",
            "car_inspection_reminder", "car_insurance_reminder", "car_registration_reminder"]]

        return df_useful_columns_extracted

    def load_source_file_from_gdrive(self, url: str):
        # Wyłączamy ostrzeżenia o niezweryfikowanym SSL
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        # Load file from URL
        response = requests.get(url, verify=False)

        # If status_coode is 200, load file using BytesIO and save it to DataFrame
        if response.status_code == 200:
            data = BytesIO(response.content)
            df = pd.read_excel(data)
            return df
        else:
            return None

    def transform_file(self, df):
        pd.options.mode.chained_assignment = None
        
        # Choose only useful columns from df
        df_useful_columns_extracted = df[["No.", "collaboration_start_date",  "collaboration_end_date",  "first_name",
            "last_name",  "city",  "phone_number",  "e-mail",  "brand",  "model",
            "follow_up_1",  "follow_up_2",  "follow_up_3", "car_inspection", "car_insurance", "car_registration",
            "car_inspection_reminder", "car_insurance_reminder", "car_registration_reminder"]]
        
        # Sort df on column that will be use in condition
        df_useful_columns_extracted.sort_values("follow_up_1")

        # Format phone number to be clickable on mobile calendar
        df_useful_columns_extracted["phone_number"] = df_useful_columns_extracted["phone_number"].astype("str").apply(lambda x: x.replace(" ", ""))
        df_useful_columns_extracted[["car_inspection_reminder", "car_insurance_reminder", "car_registration_reminder"]] = df_useful_columns_extracted[["car_inspection_reminder", "car_insurance_reminder", "car_registration_reminder"]].astype("str").apply(lambda x: x.replace(" ", ""))
        
        # Format dates to datetime type
        datetime_columns = ["collaboration_start_date", "collaboration_end_date", "follow_up_1", "follow_up_2", "follow_up_3", "car_inspection", "car_insurance", "car_registration"]
        df_useful_columns_extracted[datetime_columns] = df_useful_columns_extracted[datetime_columns].apply(pd.to_datetime, errors='coerce')

        data_dict = df_useful_columns_extracted.to_dict(orient='index')
        return df_useful_columns_extracted
    
    def get_dict_of_events_from_timeframe(self, df, event_start_day, event_end_day, type_of_event):
        # Get new events
        try:
            events_df = df[(df[type_of_event] >= event_start_day) & (df[type_of_event] <= event_end_day)]
            events_df.reset_index(drop=True, inplace=True)

            # Save insurance events as dictionary
            events_dict = events_df.to_dict(orient="index")
            return events_dict
        except Exception as e:
            print(f"Error occured while catching new {type_of_event} events: {e}")



