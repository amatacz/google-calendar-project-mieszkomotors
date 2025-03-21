import pandas as pd
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os

class EmailService():
    def __init__(self):
        self.smtp_config = os.getenv("SMTP_CONFIG")
        self.EVENT_TYPES = {
            "car_registration": "rejestracja auta",
            "car_inspection": "przegląd techniczny",
            "car_insurance": "ubezpieczenie samochodu",
            "follow_up_1": "pierwszy follow up",
            "follow_up_2": "drugi follow up",
            "follow_up_3": "trzeci follow up"
        }  
        self.raw_template = None

    def load_template(self, template_path):
        """
        Reads HTML template from file
        Args:
            template_path (str): path to HTML file with email reminder template
        Returns:
            str: HTML template read from file
        """
        with open(template_path, "r", encoding="utf-8") as f:
            template = f.read()
        return template

    def process_template(self, type_of_event, event_details):
        """
        Reads HTML template and fills it with variables from get_email_details()
        Args:
            type_of_event (str): type of event determining which template to use 
            event_details (dict): dictionary with details of event that will be used to fill template
        Returns:
            str: HTML filled with variables ready to use in send_email()
        Raises:
            FileNotFoundError: When HTML template is not found
            KeyError: When HTML template contains variable not present in email_details dict
            Exception: When error occurs during template processing
        """
        try:
            # Read HTML template
            self.raw_template = self.load_template(os.path.join("utils", "reminders_templates", f"{type_of_event}.html"))

            # Initialize processed_template var
            processed_template = self.raw_template
            
            # Insert details from event_details to placeholder in template
            for key, value in event_details.items():
                if value is not pd.NaT:  # Check if value is not NaT (Not a Time)
                    value_str = str(value)
                    # If it is timestamp, format it properly
                    if isinstance(value, pd.Timestamp):
                        value_str = value.strftime('%Y-%m-%d')
                    processed_template = processed_template.replace(f"{{{key}}}", value_str)
                else:
                    processed_template = processed_template.replace(f"{{{key}}}", "")   
            return processed_template
            
        except FileNotFoundError:
            raise FileNotFoundError(f"Unable to find template: {self.raw_template}")
        except KeyError:
            raise KeyError(f"Key is not present in event_details: {str(e)}")
        except Exception as e:
            raise Exception(f"Error during processing template: {str(e)}.")
        

    def send_email(self, type_of_event, emails_to_be_sent, source_file_url, google_service_integrator):
        """
        Sends reminder emails to clients and updates the sending status in the source file.
        
        Args:
            type_of_event (str): Type of event determining the template and email subject (e.g. 'car_registration')
            emails_to_be_sent (dict): Dictionary with email data to send, where key is the index
                                    and value is a dictionary with client data
            source_file_url (str): URL to the source file in Google Drive for status updates
            google_service_integrator (object): Integration object for communication with Google services
            
        Returns:
            None: Function does not return anything, it prints status information to standard output
            
        Raises:
            Exception: When an error occurs during email processing or sending
        """
        
        # Get SMTP configuration if not previously defined
        if not self.smtp_config:
            smtp_config = google_service_integrator.get_smtp_config()
            if not smtp_config:
                print(f"Failed to retrieve SMTP configuration")
                return None
        
        for index, email_data in emails_to_be_sent.items():
            # Determine reminder status column name based on event type
            reminder_column = f"{type_of_event}_reminder"
            reminder_value = email_data.get(reminder_column)
            
            # Check if the reminder has already been sent
            if pd.isna(reminder_value) or str(reminder_value).lower() == 'nan' or reminder_value != 'Sent':
                try:
                    # Prepare email message based on template
                    filled_html_template = self.process_template(type_of_event, email_data)


                    # Configure email message headers
                    msg = MIMEMultipart()
                    msg['From'] = smtp_config['user']
                    msg['To'] = email_data["email"]
                    msg['BCC'] = smtp_config["user"]
                    msg['Subject'] = f'[WAŻNE] {self.EVENT_TYPES[type_of_event]} wymaga odnowienia - {email_data["brand"]} {email_data["model"]} - MieszkoMotors'

                    
                    # Attach HTML content to the message
                    html_part = MIMEText(filled_html_template, "html")
                    msg.attach(html_part)
                    
                    # Send message via SMTP
                    with smtplib.SMTP_SSL(smtp_config['server'], smtp_config['port']) as server:
                        server.login(msg['From'], smtp_config['password'])
                        server.send_message(msg)
                    print(f"{msg['Subject']} email sent")
                    
                    # Update reminder status in source file
                    update_success = google_service_integrator.update_xlsx_cell(
                        file_url=source_file_url,
                        column_name=reminder_column,
                        row_index=email_data['No'],
                        new_value='Sent'
                    )
                   
                    if update_success:
                        print(f"Successfully updated status for {email_data['email']} in Google Drive")
                    else:
                        print(f"Failed to update status for {email_data['email']} in Google Drive")
                except Exception as e:
                    print(f"Error processing email for {email_data['email']}: {str(e)}")
            else:
                print(f"All {type_of_event} reminders sent already!")
 
