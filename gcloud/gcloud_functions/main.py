from utils.google_integration import GoogleServiceIntegrator
from utils.data_transformation import DataTransformer
from utils.utils import UtilsConfigurator
import functions_framework
import os



@functions_framework.http
def create_new_events(request, context=None):
    print("Starting main execution.") 

    # Create google services
    GoogleServiceIntegratorObject = GoogleServiceIntegrator()
    GoogleServiceIntegratorObject.get_google_services()

    # Create Obj. for Data Transformation
    DataTransformerObject = DataTransformer()

    # Create utilities object
    DataConfiguratorObject = UtilsConfigurator()
    START, END = DataConfiguratorObject.timeframe_window()

    # Get SOURCE_FILE_URL from env
    SOURCE_FILE_URL = os.getenv("SOURCE_FILE_URL")

    # Get file, transform it and get list of events to be created
    source_file = DataTransformerObject.load_source_file_from_gdrive(SOURCE_FILE_URL)
    source_file_transformed = DataTransformerObject.transform_file(source_file)
    follow_up_events_to_be_created = DataTransformerObject.get_dict_of_events_from_timeframe(source_file_transformed, START, END, "Follow_up_1")
    insurance_events_to_be_created = DataTransformerObject.get_dict_of_events_from_timeframe(source_file_transformed, START, END, "Ubezpieczenie samochodu")
    car_inspection_events_to_be_created = DataTransformerObject.get_dict_of_events_from_timeframe(source_file_transformed, START, END, "Przegląd techniczny")

    if not follow_up_events_to_be_created:
        return "No upcoming follow up events - cannot proceed with events creation. Skipping to Insurance Events checking..."
    else:
        # Get follow up events from given timeframe
        existing_next_month_follow_up_events = GoogleServiceIntegratorObject.get_events_list(START, END, "FOLLOW UP")

        if not existing_next_month_follow_up_events:
            print("No existing events for following moth. Proceed with events creation.")
            try:
                for event in follow_up_events_to_be_created.values():
                    GoogleServiceIntegratorObject.create_event(event, "FOLLOW UP")
            except Exception as e:
                print(f"Error while creating events {e}")
        else:
            for event_to_be_created in follow_up_events_to_be_created.values():
                if GoogleServiceIntegratorObject.validate_if_event_already_exists_in_calendar(existing_next_month_follow_up_events, event_to_be_created, "FOLLOW UP"):

                    # Create events in Google Calendar if event is not present in calendar
                    GoogleServiceIntegratorObject.create_event(event_to_be_created, "FOLLOW UP")
            print("Process of creating follow up events finished successfully.")


    if not insurance_events_to_be_created:
        return "No upcoming insurance events - cannot proceed with events creation. Skipping to Car Inspection Events checking..."
    else:
        # Get insurance events from given timeframe
        existing_next_month_insurance_events = GoogleServiceIntegratorObject.get_events_list(START, END, "Ubezpieczenie samochodu")

        if not insurance_events_to_be_created:
            print("No existing insurance events for following month. Proceed with insurance events creation.")
            try:
                for insurance_event_to_be_created in insurance_events_to_be_created.values():
                    GoogleServiceIntegratorObject.create_event(insurance_event_to_be_created, "Ubezpieczenie samochodu")
            except Exception as e:
                print(f"Error while creating insurance event: {e}")
        else:
            for insurance_event_to_be_created in insurance_events_to_be_created.values():
                if GoogleServiceIntegratorObject.validate_if_event_already_exists_in_calendar(existing_next_month_insurance_events, insurance_event_to_be_created, "Ubezpieczenie samochodu"):

                    # Create insurance event in Google Calendar if event is not present in calendar
                    GoogleServiceIntegratorObject.create_event(insurance_event_to_be_created, "Ubezpieczenie samochodu")
            print("Process of creating insurance events finished successfully.")


    if not car_inspection_events_to_be_created:
        return "No upcoming car inspection events - cannot proceed with events creation. Exiting..."
    else:
        # Get insurance events from given timeframe
        existing_next_month_car_inspection_events = GoogleServiceIntegratorObject.get_events_list(START, END, "Przegląd techniczny")

        if not existing_next_month_car_inspection_events:
            print("No existing car inspection events for following month. Proceed with car inspection events creation.")
            try:
                for car_inspection_event_to_be_created in car_inspection_events_to_be_created.values():
                    GoogleServiceIntegratorObject.create_event(car_inspection_event_to_be_created, "Przegląd techniczny")
            except Exception as e:
                print(f"Error while creating insurance event: {e}")
        else:
            for car_inspection_event_to_be_created in car_inspection_events_to_be_created.values():
                if GoogleServiceIntegratorObject.validate_if_event_already_exists_in_calendar(existing_next_month_car_inspection_events, insurance_event_to_be_created, "Przegląd techniczny"):

                    # Create insurance event in Google Calendar if event is not present in calendar
                    GoogleServiceIntegratorObject.create_event(car_inspection_event_to_be_created, "Przegląd techniczny")
            print("Process of creating car inspection events finished successfully.")

    return "Events creation function finished"

def clean_events(type_of_event):
    """
    Function to remove Insurance events from Google Calendar.
    By default removes events that start in month from today - dateframe can be specified.
    Not deployed in GCF - to be used intentionally!
    """                   
    GoogleServiceIntegratorObject = GoogleServiceIntegrator()

    # Utils object
    DataConfiguratorObject = UtilsConfigurator()
    START, END = DataConfiguratorObject.timeframe_window()

    # Create google services
    GoogleServiceIntegratorObject.get_google_services()
    GoogleServiceIntegratorObject.remove_events_from_calendar(type_of_event, START, END)

    return "Done"

def refresh_secrets(request, context=None):

    PROJECT_ID = os.getenv("PROJECT_ID")
    SECRET_ID = os.getenv("SECRET_ID")

    GoogleServiceIntegratorObject = GoogleServiceIntegrator()
    secrets = GoogleServiceIntegratorObject.get_credentials(project_id=PROJECT_ID,
                                            secret_id=SECRET_ID)
    
    if not secrets:
        return "Secrets were not refreshed successfully."
    return "Secrets refreshed successfully."



    # if not follow_up_events_to_be_created:
    #     return "No upcoming follow up events - cannot proceed with events creation. Skipping to Insurance Events checking..."
    # else:
    #     # Get follow up events from given timeframe
    #     existing_next_month_follow_up_events = GoogleServiceIntegratorObject.get_follow_up_events_list(START, END)

    #     if not follow_up_events_to_be_created:
    #         print("No existing events for following moth. Proceed with events creation.")
    #         try:
    #             for event in follow_up_events_to_be_created.values():
    #                 GoogleServiceIntegratorObject.create_follow_up_events(event)
    #         except Exception as e:
    #             print(f"Error while creating events {e}")
    #     else:
    #         for event_to_be_created in follow_up_events_to_be_created.values():
    #             if GoogleServiceIntegratorObject.validate_if_event_already_exists_in_calendar(existing_next_month_follow_up_events, event_to_be_created, "FOLLOW UP"):

    #                 # Create events in Google Calendar if event is not present in calendar
    #                 GoogleServiceIntegratorObject.create_follow_up_events(event_to_be_created)
    #         print("Process of creating follow up events finished successfully.")

    # if not insurance_events_to_be_created:
    #     return "No upcoming insurance events - cannot proceed with events creation. Skipping to Car Inspection Events checking..."
    # else:
    #     # Get insurance events from given timeframe
    #     existing_next_month_insurance_events = GoogleServiceIntegratorObject.get_insurance_events_list(START, END)

    #     if not insurance_events_to_be_created:
    #         print("No existing insurance events for following month. Proceed with insurance events creation.")
    #         try:
    #             for insurance_event_to_be_created in insurance_events_to_be_created.values():
    #                 GoogleServiceIntegratorObject.create_insurance_event(insurance_event_to_be_created)
    #         except Exception as e:
    #             print(f"Error while creating insurance event: {e}")
    #     else:
    #         for insurance_event_to_be_created in insurance_events_to_be_created.values():
    #             if GoogleServiceIntegratorObject.validate_if_event_already_exists_in_calendar(existing_next_month_insurance_events, insurance_event_to_be_created, "Ubezpieczenie samochodu"):

    #                 # Create insurance event in Google Calendar if event is not present in calendar
    #                 GoogleServiceIntegratorObject.create_insurance_event(insurance_event_to_be_created)
    #         print("Process of creating insurance events finished successfully.")


    # if not car_inspection_events_to_be_created:
    #     return "No upcoming car inspection events - cannot proceed with events creation. Exiting..."
    # else:
    #     # Get insurance events from given timeframe
    #     existing_next_month_car_inspection_events = GoogleServiceIntegratorObject.get_car_inspection_events_list(START, END)

    #     if not car_inspection_events_to_be_created:
    #         print("No existing car inspection events for following month. Proceed with car inspection events creation.")
    #         try:
    #             for car_inspection_event_to_be_created in car_inspection_events_to_be_created.values():
    #                 GoogleServiceIntegratorObject.create_car_inspection_event(car_inspection_event_to_be_created)
    #         except Exception as e:
    #             print(f"Error while creating insurance event: {e}")
    #     else:
    #         for car_inspection_event_to_be_created in car_inspection_events_to_be_created.values():
    #             if GoogleServiceIntegratorObject.validate_if_event_already_exists_in_calendar(existing_next_month_car_inspection_events, insurance_event_to_be_created, "Przegląd techniczny"):

    #                 # Create insurance event in Google Calendar if event is not present in calendar
    #                 GoogleServiceIntegratorObject.create_car_inspection_event(car_inspection_event_to_be_created)
    #         print("Process of creating car inspection events finished successfully.")