# Workshop-002: Data Engineer 💻

## Overview  
This project simulates a real-world Data Engineering task, focusing on building an **ETL pipeline** using **Apache Airflow**. The objective is to:  
- **Extract** data from three sources: an API, a CSV file (Spotify dataset), and a database (Grammys dataset).  
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

3. **API Data Source**
- Source: User-selected (e.g., Spotify API).
- Description: Additional data to enrich the pipeline (e.g., recent track data).


## Project Structure

| Folder/File            | Description |
|------------------------|------------|
| **airflow/**               | Airflow configuration and DAGs  |
| **assets**             | Static resources (images, documentation, etc.) |
| **data/**             | Dataset used in the project (ignored in .gitignore) |
| **docs/**              | Documentation and workshop PDFs |
| **notebooks/**        | Jupyter Notebooks with analysis |
| ├── 00_grammys-raw-load.ipynb | Loads Grammys data into PostgreSQL  |  
| ├── 01_Spotify-EDA.ipynb   | Exploratory Data Analysis of Spotify dataset    |  
| ├── 02_Grammys-EDA.ipynb   | Exploratory Data Analysis of Grammys dataset    |  
| ├── 03_data-pipeline.ipynb | ETL pipeline execution and merging    |
| **src/**                   | Python scripts for Airflow tasks and utilities   | 
| **venv/**              | Environment variables (ignored in .gitignore) |
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

All the libraries are included in the Poetry project config file (_pyproject.toml_).

---

## Installation and Setup

1. **Clone the Repository:**
   ```bash
   git clone https://github.com/SEBASBELMOS/workshop-002.git
   cd workshop-002
   ```

   2. **Installing the dependencies with _Poetry_**
    - Windows: 
        - In Powershell, execute this command: 
            ```powershell
            (Invoke-WebRequest -Uri https://install.python-poetry.org -UseBasicParsing).Content | py -
            ```
            <img src="https://github.com/SEBASBELMOS/workshop-002/blob/main/assets/poetry_installation.png" width="600"/>
        - Press Win + R, type _sysdm.cpl_, and press **Enter**. 
        - Go to the _Advanced_ tab, select _environment variable_.
        - Under System variables, select Path → Click Edit.
        - Click _Edit_ and set the path provided during the installation in **PATH** so that the `poetry` command works. ("C:\Users\username\AppData\Roaming\Python\Scripts")
        - Restart Powershell and execute _poetry --version_.

        
    - Linux
        - In a terminal, execute this command:
            ```bash
            curl -sSL https://install.python-poetry.org | python3 -
            ```
            <img src="https://github.com/SEBASBELMOS/workshop-002/blob/main/assets/poetry_linux.png" width="600"/>
        -  Now, execute:
            ```bash
            export PATH = "/home/user/.locar/bin:$PATH"
            ```
        -Finally, restart the terminal and execute _poetry --version_.


        <img src="https://github.com/SEBASBELMOS/workshop-002/blob/main/assets/poetry_linux_installed.png" width="400"/>

3. **Poetry Shell**
    - Enter the Poetry shell with _poetry shell_.
    - Then, execute _poetry init_, it will create a file called _pyproject.toml_
    - To add all the dependencies, execute this: 
        ```bash
        poetry add pandas matplotlib psycopg2-binary sqlalchemy python-dotenv seaborn ipykernel dotenv
        ```
    - Install the dependencies with: 
        ```bash
        poetry install
        ```
        In case of error with the .lock file, just execute _poetry lock_ to fix it.
    - Create the kernel with this command (You must choose this kernel when running the notebooks):
        ```bash
        poetry run python -m ipykernel install --user --name workshop-002 --display-name "Python (workshop-002)"
        ```

4. **PostgreSQL Database**
    - Install PostgreSQL with this [link here](https://www.postgresql.org/download/)
    - Open a terminal and execute this command, If the **postgres** user has a password, you will be prompted to enter it: 
        ```bash
        psql -U postgres
        ```
    - Create a new database with this command:
        ```bash 
        CREATE DATABASE database_name;
        ```
    - This is the information you need to add to the _.env_ file in the next step.

5. **Enviromental variables**
    >Realise this in VS Code.

    To establish a connection with the database, we use a module called _connection.py_. This Python script retrieves a file containing our environment variables. Here’s how to create it:
    1. Inside the cloned repository, create a new directory named *env/*.
    2. Within that directory, create a file called *.env*.
    3. In the *.env file*, define the following six environment variables (without double quotes around values):
        ```python
        PG_HOST = #host address, e.g. localhost or 127.0.0.1
        PG_PORT = #PostgreSQL port, e.g. 5432

        PG_USER = #your PostgreSQL user
        PG_PASSWORD = #your user password
        
        PG_DRIVER = postgresql+psycopg2
        PG_DATABASE = #your database name, e.g. postgres
        ```

---