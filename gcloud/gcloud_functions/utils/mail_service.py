from datetime import datetime, timedelta
import sqlite3
from dataclasses import dataclass
from typing import List
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from pathlib import Path
import os

class EmailService():
    def __init__(self):
         self.smtp_config = os.getenv("SMTP_CONFIG")
    
    def get_email_details(self, event):
        event["Model"] = "911"
        event["Marka"] = "Porsche"
        event["Imię"] = "Aleksandra"
        event["Nazwisko"] = "Matacz"
        event["type_of_event"] = "Ubezpieczenie"
        event["data"] = "2025-02-29"
        event["email"] = "aleksandra.matacz93@gmail.com"

        return event

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
            template = self.load_template(f".\\reminders_templates\\{type_of_event}.html")
            
            # Pobierz dane
            event = event_details
            
            # Przygotuj słownik z możliwymi zmiennymi i ich formatami
            variables = {
                'Model': '{event["Model"]}',
                'Marka': '{event["Marka"]}',
                'Imię': '{event["Imię"]}',
                'Nazwisko': '{event["Nazwisko"]}',
                'data': '{event["data"]}'
            }
            
            # Zastąp wszystkie zmienne w szablonie
            processed_template = template
            for key, placeholder in variables.items():
                if key in event:
                    processed_template = processed_template.replace(placeholder, str(event[key]))
                else:
                    raise KeyError(f"There is no key '{key}' in event dict.")
                    
            return processed_template
            
        except FileNotFoundError:
            raise FileNotFoundError(f"Unable to find template: {template}")
        except Exception as e:
            raise Exception(f"Error during processing template: {str(e)}")
        

    def get_smtp_config():
        smtp_config = {
                'server': 'smtp.gmail.com',
                'port': 587,
                'user': 'kontakt@mieszkomotors.com',
                'password': 'ioep cbdn adti dalh'
            }
        return smtp_config

    def send_email(self, type_of_event, event):
            """Wysyła przypomnienie email do klienta"""        
            # Get details of event
            # email_details = self.get_email_details()

            # Get email template and fill it with event details
            filled_html_template = self.process_template(type_of_event, event)

            msg = MIMEMultipart()
            msg['From'] = self.smtp_config['user']
            msg['To'] = event["Adres_e-mail"]
            msg['BCC'] = self.smtp_config["user"]
            msg['Subject'] = f"Przypomnienie MieszkoMotors - TEST HTML"
        
            html_part = MIMEText(filled_html_template, "html")
            msg.attach(html_part)
            
            with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
                    server.login(msg['From'], self.smtp_config['password'])
                    server.send_message(msg)
    