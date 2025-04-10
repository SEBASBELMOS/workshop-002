# Google Drive Settings (PyDrive2) - `settings.yaml` <img src="https://upload.wikimedia.org/wikipedia/commons/thumb/1/12/Google_Drive_icon_%282020%29.svg/800px-Google_Drive_icon_%282020%29.svg.png" alt="GCP" width="25px"/>

> Before you proceed with this guide, you need to have the **Google Cloud credentials** with the **OAuth 2.0 credentials**, access to your **Client ID**, **Client Secret** and **Redirect URI** for Google Drive API access.

This file is used by the `PyDrive2` library with the `store.py` module (Part of the data pipeline managed by Airflow)

---

### Template for the `settings.yaml` file.

Replace all the values with your own credentials.

```yaml
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

1. **Client Backend Configuration**
    ```yaml
    client_config_backend: file
    ```
    This is a parameter used by PyDrive2 in order to retrieve the OAuth 2.0 client configuration, so here we leave it set as `file` as we will be loading it from a file.

2. **Client Configuration**
    ```yaml
    client_config:
    client_id: your_client_id.apps.googleusercontent.com
    client_secret: your_client_secret
    redirect_uris: ['http://localhost:8090/']
    auth_uri: https://accounts.google.com/o/oauth2/auth
    auth_uri: https://accounts.google.com/o/oauth2/token
    ```

    Here is where we define the credentias to authenticate the application, make sure you add your own credentials. (Usually `redirect_uris`, `uth_uri` and `auth_uri` are not modified)

3. **Credentials**
    ```yaml
    save_credentials: True
    save_credentials_backend: file
    save_credentials_file: /path/to/your/project/credentials/saved_credentials.json
    ```
    Here is where we define if the credentials will be saved or not and also, where will they be stored.

---

### **`client_secrets.json`**

We will require this file in order to store the OAuth credentials from Google and this is the structure:

```json
    {
        "web": {
            "client_id": "your_client_id.apps.googleusercontent.com",
            "project_id": "your_project_id",
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "client_secret": "your_client_secret",
            "redirect_uris": [
                "http://localhost:8080/",
                "http://localhost:8090/"
                ]
        }
    }
```

Make sure you replace the values: `client_id`, `project_id`, `client_secret` and if necessary, `redirect_uris` but with this one, you will need at least one pointing to the local server.

> These files must be located in the env/ directory.