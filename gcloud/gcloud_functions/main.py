from utils.google_integration import GoogleServiceIntegrator
from utils.data_transformation import DataTransformer
from utils.utils import UtilsConfigurator
# from utils.logger_config import setup_logging
import functions_framework
import os



@functions_framework.http
def create_new_events(request, context=None):
    # logger = setup_logging()
    # logger.info("Starting main execution.")
    print("Starting main execution.")

    SOURCE_FILE_URL = os.getenv("SOURCE_FILE_URL")

    # Create google services
    GoogleServiceIntegratorObject = GoogleServiceIntegrator()
    GoogleServiceIntegratorObject.get_google_services()

    # Create Obj. for Data Transformation
    DataTransformerObject = DataTransformer()

    # Create utilities object
    DataConfiguratorObject = UtilsConfigurator()
    START, END = DataConfiguratorObject.timeframe_window()


    # Get file, transform it and get list of events to be created
    # source_file_url = GoogleServiceIntegratorObject.get_source_file_url()
    source_file = DataTransformerObject.load_source_file_from_gdrive(SOURCE_FILE_URL)
    # source_file = DataTransformerObject.load_source_file_from_gdrive("https://docs.google.com/spreadsheets/d/1GvAwkCIMPoSAxbfsCbLblEVp0E5CeFw6/export?format=xlsx")
    source_file_transformed = DataTransformerObject.transform_file(source_file)
    events_to_be_created = DataTransformerObject.get_dict_of_events_from_timeframe(source_file_transformed, START, END)
        
    if not events_to_be_created:
        # logger.info("No upcoming events - cannot proceed with events creation. Exiting...")
        print("No upcoming events - cannot proceed with events creation. Exiting...")
        return "No upcoming events - cannot proceed with events creation. Exiting..."
    else:
        # Get follow up events from given timeframe
        existing_next_month_follow_up_events = GoogleServiceIntegratorObject.get_follow_up_events_list(START, END)

        if not existing_next_month_follow_up_events:
            # logger.info("No existing events for following moth. Proceed with events creation.")
            print("No existing events for following moth. Proceed with events creation.")
            try:
                for event in events_to_be_created.values():
                    GoogleServiceIntegratorObject.create_follow_up_events(event)
            except Exception as e:
                # logger.error(f"Error while creating events {e}")
                print(f"Error while creating events {e}")
        else:
            for event_to_be_created in events_to_be_created.values():
                if GoogleServiceIntegratorObject.validate_if_event_already_exists_in_calendar(existing_next_month_follow_up_events, event_to_be_created):
                    # Create events in Google Calendar if event is not present in calendar
                    GoogleServiceIntegratorObject.create_follow_up_events(event_to_be_created)
            # logger.info("Process finished successfully.")
            print("Process finished successfully.")

    return "Events creation function finished"

def clean_follow_up_events():                   
    GoogleServiceIntegratorObject = GoogleServiceIntegrator()

    # Utils object
    DataConfiguratorObject = UtilsConfigurator()
    START, END = DataConfiguratorObject.timeframe_window()

    # Create google services
    GoogleServiceIntegratorObject.get_google_services()
    GoogleServiceIntegratorObject.remove_events_from_calendar("FOLLOW UP", START, END)

    return "Done"