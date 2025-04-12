# Workshop-002: Data Engineer 💻

## Overview  
This project simulates a real-world Data Engineering task, focusing on building an **ETL pipeline** using **Apache Airflow**. The objective is to:  
- **Extract** data from three sources: the Spotify API, a CSV file (Spotify dataset), and a database (Grammys dataset).  
- **Transform** and merge the data using Apache Airflow.  
- **Load** the processed data into a database and Google Drive as a CSV.  
- Create a **dashboard** using Power BI to display insights from the database.  

---

## Dataset Information
1. **Spotify Dataset** (*spotify_dataset.csv*) <img src="https://upload.wikimedia.org/wikipedia/commons/thumb/1/19/Spotify_logo_without_text.svg/2048px-Spotify_logo_without_text.svg.png" alt="Spotify" width="15px"/>
- Source: CSV file
- Description: Contains metadata and audio features of Spotify tracks.
- Link: [Spotify Tracks Dataset on Kaggle](https://www.kaggle.com/datasets/maharshipandya/-spotify-tracks-dataset)

    **Key Columns:**
    - track_id: Unique track identifier.
    - artists: Artist(s) name(s).
    - track_name: Song title.
    - popularity: Score (0-100) of track popularity.
    - duration_ms: Track length in milliseconds.
    - danceability: Suitability for dancing (0-1).
    - energy: Intensity measure (0-1).
    - loudness: Loudness in decibels.
    - tempo: Beats per minute (BPM).
    - track_genre: Genre of the track.

2. **Grammy Awards Dataset** (*the_grammy_awards.csv*) <img src="https://www.pikpng.com/pngl/b/135-1355099_grammy-award-logo-png-download-grammy-awards-2018.png" alt="Grammys" width="15px"/>
- Source: Initial database load
- Description: Details Grammy nominations and wins from 1958 to 2019.
- Link: [Grammy Awards on Kaggle](https://www.kaggle.com/datasets/unanimad/grammy-awards)

    **Key Columns:**
    - year: Year of the Grammy event.
    - category: Award category (e.g., Best Pop Solo Performance).
    - nominee: Nominated song or album.
    - artist: Associated artist(s).
    - winner: Boolean indicating win (True/False).

3. **Spotify API** <img src="https://upload.wikimedia.org/wikipedia/commons/thumb/1/19/Spotify_logo_without_text.svg/2048px-Spotify_logo_without_text.svg.png" alt="Spotify" width="15px"/>
- Source: API
- Description: In order to improve the analysis, the key column will be `followers` of each artist.
- Link: [Spotify for Developers](https://developer.spotify.com/dashboard)

    **Key Column:**
    - followers: Artist Followers.

---

## Project Structure

| Folder/File            | Description |
|------------------------|------------|
| **assets/**             | Static resources (images, documentation, etc.) |
| **data/**             | Dataset used in the project (ignored in .gitignore) |
| **dags/**             | Stores Apache Airflow Dags |
| ├── **tasks/**  | Stores Apache Airflow Tasks that will be used by our dag  |  
| **docs/**              | Documentation, Guides and workshop PDFs |
| **drive_config/**              | Stores the client secret from OAuth and saved credentials to store the merge data |
| **notebooks/**        | Jupyter Notebooks with analysis |
| ├── 01_grammys-raw-load.ipynb | Loads Grammys data into PostgreSQL  |  
| ├── 02_spotify-EDA.ipynb   | Exploratory Data Analysis of Spotify dataset    |  
| ├── 03_grammys-EDA.ipynb   | Exploratory Data Analysis of Grammys dataset    |  
| ├── 04_data-pipeline.ipynb | ETL pipeline execution and Google Drive credentials creation    |
| **src/**                   | Python scripts for Airflow tasks and utilities   | 
| **venv/**              | Virtual environment (ignored in .gitignore) |
| **env/**               | Environment variables (ignored in .gitignore) |
| ├── .env                 |	Stores credentials and paths  |
| **pbi/**               | Power BI files (ignored in .gitignore) |
| **docker-compose.yaml**         | Docker configuration |
| **requirements.txt**         | All the libraries required to execute this project properly |
| **README.md**         | This file |

## Tools and Libraries

- **Programming Language:** Python 3.13 -> [Download here](https://www.python.org/downloads/)
- **Workflow Orchestration**: Apache Airflow → [Documentation here](https://airflow.apache.org/docs/)
- **Data Handling:** pandas -> [Documentation here](https://pandas.pydata.org/)
- **Database:** PostgreSQL -> [Download here](https://www.postgresql.org/download/)
- **Database Interaction:** SQLAlchemy -> [Documentation here](https://docs.sqlalchemy.org/)
- **Visualisation:** Power BI Desktop -> [Download here](https://www.microsoft.com/es-es/power-platform/products/power-bi/desktop)
- **Environment:** Jupyter Notebook -> [VSCode tool used](https://code.visualstudio.com/docs/datascience/jupyter-notebooks)
- **Storage:** Google Drive & PyDrive2 -> [Documentation here](https://docs.iterative.ai/PyDrive2/)

---

## Workflow

<img src="https://github.com/SEBASBELMOS/workshop-002/blob/develop/assets/workflow.png" width="800"/>

---

## Installation and Setup

1. **Clone the Repository:**
   ```bash
   git clone https://github.com/SEBASBELMOS/workshop-002.git
   cd workshop-002
   ```

2. **Database Google Cloud Platform**
    > To create the databases in GCP, you can follow this [guide](https://github.com/SEBASBELMOS/workshop-002/blob/main/docs/guides/google_cloud_config.md)

    - Use the `public IP` for connections, and ensure the IP `0.0.0.0/0` is added to authorised networks for testing.

3. **Google Drive Auth**
    > To create the Google Drive Authentication, you can follow this [guide](https://github.com/SEBASBELMOS/workshop-002/blob/main/docs/guides/google_drive_api.md)

    - Ensure `PyDrive2` is installed as part of the dependencies (requirements.txt).

4. **Configure PyDrive2 (_settings.yaml_)**

    > To configure the `settings.yaml` file for authentication and authorisation, follow this [guide](https://github.com/SEBASBELMOS/workshop-002/blob/main/docs/guides/google_drive_settings.md)

5. **Virtual Environment (This must be done in Ubuntu or WSL)**
    - Create virtual environment.
        ```bash
        python3 -m venv workshop2
        ```

    - Activate it using this command:
        ```bash
        source workshop2/bin/activate 
        ```

    - Install all the requirements and libraries with this command:
        ```bash
        pip install -r requirements.txt 
        ```

6. **Enviromental variables**
    >Realise this in VS Code.

    To establish a connection with the database, we use a module called _connection.py_. This Python script retrieves a file containing our environment variables. Here’s how to create it:
    1. Inside the cloned repository, create a new directory named *env/*.
    2. Within that directory, create a file called *.env*.
    3. In the *.env file*, define the following six environment variables (without double quotes around values):
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

7. **Execute Docker Containers**

    After generating your own `docker-compose.yaml`, you must run these commands in your terminal in order to execute your containers:

    - `docker-compose build` to create them.
    - `docker-compose up -d` to execute them.
    - `docker-compose down` to turn them off.

8. **Create the connection to the Spotify API**

    > To generate the client ID and client secret, follow this [guide](https://github.com/SEBASBELMOS/workshop-002/blob/main/docs/guides/spotify_api.md)

9. **Dag Validation**

    After running the dag, you will see this file in your Google Drive Folder:

    <img src="https://github.com/SEBASBELMOS/workshop-002/blob/main/assets/drive_csv.png" width="400"/>

    And you will see this in your Airflow Home:

    <img src="https://github.com/SEBASBELMOS/workshop-002/blob/main/assets/dag_working.png" width="400"/>

---

## Power BI Connection (Not necessary)
1. Open Power BI Desktop and create a new dashboard. 
2. Select the _Get data_ option, then choose the "_PostgreSQL Database_" option.

    <img src="https://github.com/SEBASBELMOS/workshop-002/blob/main/assets/pbi.png" width="400"/>

3. Insert the _PostgreSQL Server_ and _Database Name_.
    
    <img src="https://github.com/SEBASBELMOS/workshop-002/blob/main/assets/postgres_pbi.png" width="400"/>

4. Fill the following fields with your Postgres credentials.
    
    <img src="https://github.com/SEBASBELMOS/workshop-002/blob/main/assets/postgres_access_pbi.png" width="400"/>    

5. After establishing the connection, the tables of your db will be displayed. You need to select the ones you need, and then start creating your own dashboards.

<<<<<<< Updated upstream
- Open my Power BI Visualisation [here]()
=======
- Open my Power BI Visualisation [here](https://app.powerbi.com/links/9WTfzzj7fp?ctid=693cbea0-4ef9-4254-8977-76e05cb5f556&pbi_source=linkSharez)
>>>>>>> Stashed changes

---


## **Author**  
Created by **Sebastian Belalcazar Mosquera**. Connect with me on [LinkedIn](https://www.linkedin.com/in/sebasbelmos/) for feedback, suggestions, or collaboration opportunities!

---
