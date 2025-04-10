# How to connect the Spotify API <img src="https://upload.wikimedia.org/wikipedia/commons/thumb/1/19/Spotify_logo_without_text.svg/2048px-Spotify_logo_without_text.svg.png" alt="Spotify" width="25px"/>

> Firstly, you need to access to [Spotify for Developers](https://developer.spotify.com/dashboard), create an account and then, you will be ready to go to continue with this guide.

---

### Create an app

The app provides, among others, the _Client ID_ and _Client Secret_ needed to implement any of the authorization flows.

1. Log into [Spotify for Developers](https://developer.spotify.com/dashboard) and open Dashboard.

2. Click on the _Create an App_ button.

    <img src="https://github.com/SEBASBELMOS/workshop-002/blob/main/assets/create_an_app.png" width="500"/>

3. Enter the app details of your choice, accept the _Developer Terms of Service_, then click on CREATE.

    <img src="https://github.com/SEBASBELMOS/workshop-002/blob/main/assets/createappdialog.png" width="500"/>

4. Afterwards, you will see this screen with your Client ID and secret (Save them and add them in your .env).

    <img src="https://github.com/SEBASBELMOS/workshop-002/blob/main/assets/info_api.png" width="500"/>

---

Here's what your `.env` file should look like at the end of this guide.

    ```python
        #PostgreSQL Variables
        PG_HOST = #host address, e.g. localhost or 127.0.0.1
        PG_PORT = #PostgreSQL port, e.g. 5432
        PG_USER = #your PostgreSQL user
        PG_PASSWORD = #your user password
        PG_DATABASE = #your database name, e.g. postgres

        #Google Drive Variables
        #Path to the client secrets file used for Google Drive authentication.
        CLIENT_SECRETS_PATH = "/path/to/your/credentials/client_secrets.json"

        #Path to the settings file for the application configuration.
        SETTINGS_PATH = "/path/to/your/env/settings.yaml"

        #Path to the file where Google Drive saved credentials are stored.
        SAVED_CREDENTIALS_PATH = "/path/to/your/credentials/saved_credentials.json"

        #The ID of your Google Drive folder. This can be found in the link in your folder.
        FOLDER_ID = # your-drive-folder-id

        SPOTIFY_CLIENT_ID=client_id

        SPOTIFY_CLIENT_SECRET=client_secret
        ```

