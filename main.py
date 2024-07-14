from google_integration import GoogleServiceIntegrator
from events_functions import GoogleCloudEvents
from data_configurator import DataConfigurator


def main():
    GoogleServiceIntegratorObject = GoogleServiceIntegrator()
    GoogleCloudEventsObject = GoogleCloudEvents()

    # Utils object
    DataConfiguratorObject = DataConfigurator()
    START, END = DataConfiguratorObject.timeframe_window()

    # Create google services
    GoogleServiceIntegratorObject.get_google_services()

    # Get file, transform it and get list of events to be created
    source_file_url = GoogleServiceIntegratorObject.get_source_file_url()
    source_file = GoogleCloudEventsObject.load_source_file_from_gdrive(source_file_url)
    source_file_transformed = GoogleCloudEventsObject.transform_file(source_file)
    events_to_be_created = GoogleCloudEventsObject.get_dict_of_events_from_timeframe(source_file_transformed, START, END)
        
    if not events_to_be_created:
        print("No upcoming events - cannot proceed with events creation. Exiting...")
        return None
    else:
        # Get follow up events from given timeframe
        existing_next_month_follow_up_events = GoogleServiceIntegratorObject.get_follow_up_events_list(START, END)

        if not existing_next_month_follow_up_events:
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

def clean_follow_up_events():                   
    GoogleServiceIntegratorObject = GoogleServiceIntegrator()

    # Utils object
    DataConfiguratorObject = DataConfigurator()
    START, END = DataConfiguratorObject.timeframe_window()

    # Create google services
    GoogleServiceIntegratorObject.get_google_services()

    GoogleServiceIntegratorObject.remove_events_from_calendar("FOLLOW UP", START, END)

main()