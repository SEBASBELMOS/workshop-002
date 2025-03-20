# Google Drive API Configuration <img src="https://upload.wikimedia.org/wikipedia/commons/thumb/1/12/Google_Drive_icon_%282020%29.svg/800px-Google_Drive_icon_%282020%29.svg.png" alt="GCP" width="25px"/>

1. **Create a new project in Google Cloud**
    - In [Google Cloud Console](https://console.cloud.google.com/), create a new project.

        <img src="https://github.com/SEBASBELMOS/workshop-002/blob/main/assets/new_project_gcp.png" width="400"/>

2. **Enable the Google Drive API**
    - Open the navigation bar, navigate to _APIs & Services_ (You can also use the search bar and type _APIs & Services_).

        <img src="https://github.com/SEBASBELMOS/workshop-002/blob/main/assets/gcp_navigation.png" width="600"/>

    - In the search field of _Library_, type `Google Drive API`, select and enable it.

        <img src="https://github.com/SEBASBELMOS/workshop-002/blob/main/assets/gd_api_enable.png" width="400"/>

3. **Create OAuth 2.0 Credentials**
    - In the navigation bar select, `Credentials`.

        <img src="https://github.com/SEBASBELMOS/workshop-002/blob/main/assets/create_credentials.png" width="500"/>

    - Click on `Create Credentials` and select _**OAuth client ID**_.
    - If you have not configures the OAuth consent screen, then you need to:
        - Select *Configure consent screen*
        - The user type must be _External_
        - Then, you need to add some personal basic information and everything will be completed.
    - Then you will see this and you have to select *Desktop App*

        <img src="https://github.com/SEBASBELMOS/workshop-002/blob/main/assets/oauth_client_id.png" width="400"/>

    - Finally, download the JSON file and locate it in the project directory.

        <img src="https://github.com/SEBASBELMOS/workshop-002/blob/main/assets/oauth_client_created.png" width="400"/>

4. **Install the PyDrive2 library**
    To interact with Google Drive in Python, you need this library, here is how you can install it:
           ```python
            pip install PyDrive2
            ```
