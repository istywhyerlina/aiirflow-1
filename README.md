
## Project Overview 

**Will be updated**

## Architecture Description

## Pipeline Flow

## How to use this project?
1. Preparations
2. Run Docker Compose



### 1. Preparations
- **Clone repo** :
  ```
  git clone https://github.com/istywhyerlina/aiirflow-1
  ```

-  **Create env file** in project root directory  :
  ```
    AIRFLOW_FERNET_KEY=
    AIRFLOW_DB_URI=
    AIRFLOW_DB_USER=
    AIRFLOW_DB_PASSWORD=
    AIRFLOW_DB_NAME=
    AIRFLOW_WWW_USER_USERNAME=
    AIRFLOW_WWW_USER_PASSWORD=
    
    SRC_USER=
    SRC_PASSWORD=
    SRC_DB_NAME=
    
    DWH_USER=
    DWH_PASSWORD=
    DWH_DB_NAME=
    
    MINIO_ROOT_USER=
    MINIO_ROOT_PASSWORD=
  ```
 - Run fernet.py, copy the code as  AIRFLOW_FERNET_KEY in evv file
### 2. Run Docker Compose :
  ```
  docker compose up -d
  ```

  If you're already have a running service, you can run this command
  
  ```
  docker compose down --volumes && docker compose up -d
  ```
### 3. Create connection airflow to minio and postgres :
  - Get username and password
  ```
  docker logs airflow_w2 | grep username
  ```
  - Login to airflow, open in browser localhost:8081
  - Add connection to airflow >> Screenshot saved in png directory

### 4. Run DAG :

  - Screenshot of Projects

  - Dumped data to Minio

  - DAG Graphs

  - Example of Details Task runs 
