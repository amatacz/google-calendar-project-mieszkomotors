from datetime import datetime, timedelta
import pandas as pd
from dataclasses import dataclass
from typing import List
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from pathlib import Path
import os
from utils.google_integration import GoogleServiceIntegrator

class EmailService():
    def __init__(self):
         self.smtp_config = os.getenv("SMTP_CONFIG")

    def load_template(self, template_path):
        """Wczytuje szablon HTML z pliku"""
        with open(template_path, "r", encoding="utf-8") as f:
            template = f.read()
        return template

    def process_template(self, type_of_event, event_details):
        """
        Wczytuje szablon HTML i podstawia do niego zmienne z get_email_details()
        Args:
            template_path (str): Ścieżka do pliku szablonu HTML 
        Returns:
            str: Wypełniony szablon HTML 
        Raises:
            FileNotFoundError: Gdy nie można znaleźć pliku szablonu
            KeyError: Gdy w szablonie występuje zmienna, której nie ma w słowniku event
        """
        try:
            # Wczytaj szablon
            #template = self.load_template(f".\\reminders_templates\\{type_of_event}.html")
            raw_remplate = self.load_template(f"C:\\Users\\amatacz\\moje\\GoogleCalendarPythonIntegration\\gcloud\\gcloud_functions\\utils\\reminders_templates\\{type_of_event}.html")

            # Inicjalizuj processed_template
            processed_template = raw_remplate
            
            # Zastąp wszystkie zmienne w szablonie
            for key, value in event_details.items():
                if value is not pd.NaT:  # Sprawdź czy wartość nie jest NaT (Not a Time)
                    value_str = str(value)
                    # Jeśli to timestamp, sformatuj go odpowiednio
                    if isinstance(value, pd.Timestamp):
                        value_str = value.strftime('%Y-%m-%d')  # lub inny format daty
                    processed_template = processed_template.replace(f"{{{key}}}", value_str)
                else:
                    processed_template = processed_template.replace(f"{{{key}}}", "")  # lub inna wartość domyślna dla NaT      
            return processed_template
            
        except FileNotFoundError:
            raise FileNotFoundError(f"Unable to find template: {raw_remplate}")
        except Exception as e:
            raise Exception(f"Error during processing template: {str(e)}.")
        

    def send_email(self, type_of_event, emails_to_be_sent, source_file_url, google_service_integrator):
        """Wysyła przypomnienie email do klienta""" 

        if not self.smtp_config:
            smtp_config = google_service_integrator.get_smtp_config()

        for index, email_data in emails_to_be_sent.items():

            reminder_column = f"{type_of_event}_reminder"
            reminder_value = email_data.get(reminder_column)
    
            if pd.isna(reminder_value) or str(reminder_value).lower() == 'nan' or reminder_value != 'Sent':
                try:
                    # Get email template and fill it with event details
                    filled_html_template = self.process_template(type_of_event, email_data)

                    msg = MIMEMultipart()
                    msg['From'] = smtp_config['user']
                    msg['To'] = email_data["e-mail"]
                    #msg['BCC'] = smtp_config["user"]
                    msg['Subject'] = f'Przypomnienie MieszkoMotors - {type_of_event} - {email_data["brand"]} {email_data["model"]}'


                    html_part = MIMEText(filled_html_template, "html")
                    msg.attach(html_part)
                        
                    with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
                            server.login(msg['From'], smtp_config['password'])
                            server.send_message(msg)
                    print(f"{msg['Subject']} email sent")

                    # Aktualizacja statusu w pliku
                    update_success = google_service_integrator.update_xlsx_cell(
                        file_url=source_file_url,
                        column_name=reminder_column,
                        row_index=email_data['No.'],  # Używamy index z emails_to_be_sent
                        new_value='Sent'
                    )

                    if update_success:
                        print(f"Successfully updated status for {email_data['e-mail']} in Google Drive")
                    else:
                        print(f"Failed to update status for {email_data['e-mail']} in Google Drive")
                except Exception as e:
                    print(f"Error processing email for {email_data['e-mail']}: {str(e)}")
                    continue
            else:
                print("All reminders sent already!")
 
        


            

            
    