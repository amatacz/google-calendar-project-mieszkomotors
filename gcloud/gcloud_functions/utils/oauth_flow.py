import os
import json
from google_auth_oauthlib.flow import Flow
from google.oauth2.credentials import Credentials


# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/gmail.modify',
          'https://www.googleapis.com/auth/drive',
          'https://www.googleapis.com/auth/calendar']

def save_credentials(credentials):
    creds_data = {
        'token': credentials.token,
        'refresh_token': credentials.refresh_token,
        'token_uri': credentials.token_uri,
        'client_id': credentials.client_id,
        'client_secret': credentials.client_secret,
        'scopes': credentials.scopes
    }
    
    with open('token.json', 'w') as token_file:
        json.dump(creds_data, token_file)
    
    print("Credentials saved to 'token.json'")

def perform_oauth_flow(path_to_secrets):
    # Load client secrets from the downloaded JSON file
    client_secrets_file = path_to_secrets
    
    flow = Flow.from_client_secrets_file(
        client_secrets_file,
        scopes=SCOPES,
        redirect_uri='urn:ietf:wg:oauth:2.0:oob'  # For desktop app flow
    )

    # Tell the user to go to the authorization URL.
    auth_url, _ = flow.authorization_url(prompt='consent', access_type='offline')
    print(f'Please go to this URL: {auth_url}')

    # The user will get an authorization code. This code is used to get the
    # access token.
    code = input('Enter the authorization code: ')
    flow.fetch_token(code=code)

    # You now have access to the credentials, including the refresh token
    credentials = flow.credentials

    # Save the credentials for future use
    save_credentials(credentials)

    print("OAuth flow completed successfully.")
    return credentials


if __name__ == '__main__':
    perform_oauth_flow("C:\\Users\\amatacz\\OneDrive - DXC Production\\Desktop\\moje\\GoogleCalendarPythonIntegration\\secrets\\oauth-token-google-calendar-project.json")