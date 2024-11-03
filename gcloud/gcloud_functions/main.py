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

    # Get url of source file
    # SOURCE_FILE_URL = GoogleServiceIntegratorObject.get_source_file_url()
    SOURCE_FILE_URL = "https://docs.google.com/spreadsheets/d/1GvAwkCIMPoSAxbfsCbLblEVp0E5CeFw6/export?format=xlsx"
    # Get file, transform it and get list of events to be created
    source_file = DataTransformerObject.load_source_file_from_gdrive(SOURCE_FILE_URL)
    source_file_transformed = DataTransformerObject.transform_file(source_file)
    events_to_be_created = DataTransformerObject.get_dict_of_events_from_timeframe(source_file_transformed, START, END)
        
    if not events_to_be_created:
        return "No upcoming events - cannot proceed with events creation. Exiting..."
    else:
        # Get follow up events from given timeframe
        existing_next_month_follow_up_events = GoogleServiceIntegratorObject.get_follow_up_events_list(START, END)

        if not existing_next_month_follow_up_events:
            print("No existing events for following moth. Proceed with events creation.")
            try:
                for event in events_to_be_created.values():
                    GoogleServiceIntegratorObject.create_follow_up_events(event)
            except Exception as e:
                print(f"Error while creating events {e}")
        else:
            for event_to_be_created in events_to_be_created.values():
                if GoogleServiceIntegratorObject.validate_if_event_already_exists_in_calendar(existing_next_month_follow_up_events, event_to_be_created):
                    # Create events in Google Calendar if event is not present in calendar
                    GoogleServiceIntegratorObject.create_follow_up_events(event_to_be_created)
            print("Process finished successfully.")

    return "Events creation function finished"

def clean_follow_up_events():
    """
    Function to remove Follow Up events from Google Calendar.
    By default removes events that start in month from today - dateframe can be specified.
    Not deployed in GCF - to be used intentionally!
    """                   
    GoogleServiceIntegratorObject = GoogleServiceIntegrator()

    # Utils object
    DataConfiguratorObject = UtilsConfigurator()
    START, END = DataConfiguratorObject.timeframe_window()

    # Create google services
    GoogleServiceIntegratorObject.get_google_services()
    GoogleServiceIntegratorObject.remove_events_from_calendar("FOLLOW UP", START, END)

    return "Done"

@functions_framework.http
def refresh_token(request, context=None):
    project_id = '481715545022'
    secret_id = 'oauth-token-google-calendar-project'
    GoogleServiceIntegrator.get_credentials(project_id=project_id,
                                            secret_id=secret_id) 