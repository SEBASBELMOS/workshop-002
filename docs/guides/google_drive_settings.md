# Google Drive Settings (PyDrive2) - `settings.yaml` <img src="https://upload.wikimedia.org/wikipedia/commons/thumb/1/12/Google_Drive_icon_%282020%29.svg/800px-Google_Drive_icon_%282020%29.svg.png" alt="GCP" width="25px"/>

> Before you proceed with this guide, you need to have the **Google Cloud credentials** with the **OAuth 2.0 credentials**, access to your **Client ID**, **Client Secret** and **Redirect URI** for Google Drive API access.

This file is used by the `PyDrive2` library with the `store.py` module (Part of the data pipeline managed by Airflow)

---

### Template for the `settings.yaml` file.

Replace all the values with your own credentials.

    ```bash
    client_config_backend: file
    client_config:
    client_id: your_client_id.apps.googleusercontent.com
    client_secret: your_client_secret
    redirect_uris: ['http://localhost:8090/']
    auth_uri: https://accounts.google.com/o/oauth2/auth
    token_uri: https://accounts.google.com/o/oauth2/token

    save_credentials: True
    save_credentials_backend: file
    save_credentials_file: /path/to/your/project/credentials/saved_credentials.json

    get_refresh_token: True

    oauth_scope:
    - https://www.googleapis.com/auth/drive
    ```

---

### Instructions


