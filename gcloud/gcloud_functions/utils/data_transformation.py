import pandas as pd
import requests
from io import BytesIO
import logging


class DataTransformer:

    def read_source_file_from_path(self, path=None):
        file = pd.read_excel("./MieszkoMotors_praca.xlsx", "Wykaz_realizacji").sort_values("Follow_up_1")
        df_useful_columns_extracted = df[["Data_rozpoczęcia",  "Data_zakończenia",  "Imię",
            "Nazwisko",  "Miasto",  "Nr_telefonu",  "Adres_e-mail",  "Marka",  "Model",
            "Follow_up_1",  "Follow_up_2",  "Follow_up_3", "Przegląd techniczny", "Ubezpieczenie samochodu"]]

        return df_useful_columns_extracted

    def load_source_file_from_gdrive(self, url: str):
        # Load file from URL
        response = requests.get(url)

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
        df_useful_columns_extracted = df[["Data_rozpoczęcia",  "Data_zakończenia",  "Imię",
            "Nazwisko",  "Miasto",  "Nr_telefonu",  "Adres_e-mail",  "Marka",  "Model",
            "Follow_up_1",  "Follow_up_2",  "Follow_up_3", "Przegląd techniczny", "Ubezpieczenie samochodu", "Rejestracja auta"]]
        # Sort df on column that will be use in condition
        df_useful_columns_extracted.sort_values("Follow_up_1")
        # Format phone number to be clickable on mobile calendar
        df_useful_columns_extracted["Nr_telefonu"] = df_useful_columns_extracted["Nr_telefonu"].astype("str").apply(lambda x: x.replace(" ", "")) 

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



