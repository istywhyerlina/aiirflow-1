from helper.minio import MinioClient
from helper.postgres import Execute
import pandas as pd
import json


BASE_PATH = "/opt/airflow/dags"

class Load:
    def _load_minio(table_name: str):
        minio_client = MinioClient._get()
        bucket_name = 'extracted-data'
        object_name = f"/temp/{table_name}.csv"

        data = minio_client.get_object(
            bucket_name = bucket_name,
            object_name = object_name
        )

        df = pd.read_csv(data)
        if table_name == 'aircrafts_data':
            df['model'] = df['model'].apply(json.dumps)

        if table_name == 'airports_data':
            df['airport_name'] = df['airport_name'].apply(json.dumps)
            df['city'] = df['city'].apply(json.dumps)

        if table_name == 'tickets':
            df['contact_data'] = df['contact_data'].apply(json.dumps)
        
        if table_name == 'flights':
            df = df.replace({float('nan'): None})

        Execute._insert_dataframe(
                connection_id = "dwh", 
                query_path =  f'flights_data_pipeline/query/insert_{table_name}.sql',
                dataframe = df
        )
