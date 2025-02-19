from utils.google_integration import GoogleServiceIntegrator
from utils.data_transformation import DataTransformer
from utils.utils import UtilsConfigurator
from utils.mail_service import EmailService
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
    START, END = DataConfiguratorObject.timeframe_window() # By default next 30 days, today included

    # Create email service object
    EmailServiceObject = EmailService()

    # Get SOURCE_FILE_URL from env
    SOURCE_FILE_URL = os.getenv("SOURCE_FILE_URL")

    # Get file, transform it and get list of events to be created
    source_file = DataTransformerObject.load_source_file_from_gdrive(SOURCE_FILE_URL)
    source_file_transformed = DataTransformerObject.transform_file(source_file)

    # Get all next month events from source
    follow_up_1_events_to_be_created = DataTransformerObject.get_dict_of_events_from_timeframe(source_file_transformed, START, END, "follow_up_1")
    follow_up_2_events_to_be_created = DataTransformerObject.get_dict_of_events_from_timeframe(source_file_transformed, START, END, "follow_up_2")
    follow_up_3_events_to_be_created = DataTransformerObject.get_dict_of_events_from_timeframe(source_file_transformed, START, END, "follow_up_3")
    insurance_events_to_be_created = DataTransformerObject.get_dict_of_events_from_timeframe(source_file_transformed, START, END, "car_insurance")
    car_inspection_events_to_be_created = DataTransformerObject.get_dict_of_events_from_timeframe(source_file_transformed, START, END, "car_inspection")
    car_registration_events_to_be_created = DataTransformerObject.get_dict_of_events_from_timeframe(source_file_transformed, START, END, "car_registration")

    # Create all calendar reminders in kontakt@mieszkomotors.com Google Calendar
    GoogleServiceIntegratorObject.create_events_for_next_month(START, END, follow_up_1_events_to_be_created, "follow_up_1")
    GoogleServiceIntegratorObject.create_events_for_next_month(START, END, follow_up_2_events_to_be_created, "follow_up_2")
    GoogleServiceIntegratorObject.create_events_for_next_month(START, END, follow_up_3_events_to_be_created, "follow_up_3")
    GoogleServiceIntegratorObject.create_events_for_next_month(START, END, insurance_events_to_be_created, "car_insurance")
    GoogleServiceIntegratorObject.create_events_for_next_month(START, END, car_inspection_events_to_be_created, "car_inspection")
    GoogleServiceIntegratorObject.create_events_for_next_month(START, END, car_registration_events_to_be_created, "car_registration")

    # # Send reminder emails to clients
    EmailServiceObject.send_email("car_inspection", car_inspection_events_to_be_created, SOURCE_FILE_URL, GoogleServiceIntegratorObject)
    EmailServiceObject.send_email("car_insurance", insurance_events_to_be_created, SOURCE_FILE_URL, GoogleServiceIntegratorObject)

    return "Events creation function finished"









# def clean_events(type_of_event):
#     """
#     Function to remove Insurance events from Google Calendar.
#     By default removes events that start in month from today - dateframe can be specified.
#     Not deployed in GCF - to be used intentionally!
#     """                   
#     GoogleServiceIntegratorObject = GoogleServiceIntegrator()

#     # Utils object
#     DataConfiguratorObject = UtilsConfigurator()
#     START, END = DataConfiguratorObject.timeframe_window()

#     # Create google services
#     GoogleServiceIntegratorObject.get_google_services()
#     GoogleServiceIntegratorObject.remove_events_from_calendar(type_of_event, START, END)

#     return "Done"

# def refresh_secrets(request, context=None):

#     PROJECT_ID = os.getenv("PROJECT_ID")
#     SECRET_ID = os.getenv("SECRET_ID")

#     GoogleServiceIntegratorObject = GoogleServiceIntegrator()
#     secrets = GoogleServiceIntegratorObject.get_credentials(project_id=PROJECT_ID,
#                                             secret_id=SECRET_ID)
    
#     if not secrets:
#         return "Secrets were not refreshed successfully."
#     return "Secrets refreshed successfully."