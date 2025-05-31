from airflow.providers.postgres.hooks.postgres import PostgresHook
from helper.minio import MinioClient
from helper.postgres import Execute
from io import BytesIO
import json

BASE_PATH = "/opt/airflow/dags"

class Extract:
    def _extract_src(table_name: str):

        df = Execute._get_dataframe(
            connection_id = 'src',
            query_path = f'flights_data_pipeline_1/query/get_{table_name}.sql'
        )


        bucket_name = 'extracted-data'
        minio_client = MinioClient._get()
        
        csv_bytes = df.to_csv(index = False).encode('utf-8')
        csv_buffer = BytesIO(csv_bytes)

        minio_client.put_object(
            bucket_name = bucket_name,
            object_name = f'/temp/{table_name}.csv',
            data = csv_buffer,
            length = len(csv_bytes),
            content_type='application/csv'
        )

    def _aircrafts_data():

        df = Execute._get_dataframe(
            connection_id = 'src',
            query_path = 'flights_data_pipeline_1/query/get_aircrafts_data.sql'
        )


        bucket_name = 'extracted-data'
        minio_client = MinioClient._get()
        
        csv_bytes = df.to_csv(index = False).encode('utf-8')
        csv_buffer = BytesIO(csv_bytes)

        minio_client.put_object(
            bucket_name = bucket_name,
            object_name = f'/temp/aircrafts_data.csv',
            data = csv_buffer,
            length = len(csv_bytes),
            content_type='application/csv'
        )

    def _airports_data():

        df = Execute._get_dataframe(
            connection_id = 'src',
            query_path = 'flights_data_pipeline_1/query/get_airports_data.sql'
        )


        bucket_name = 'extracted-data'
        minio_client = MinioClient._get()
        
        csv_bytes = df.to_csv(index = False).encode('utf-8')
        csv_buffer = BytesIO(csv_bytes)

        minio_client.put_object(
            bucket_name = bucket_name,
            object_name = f'/temp/airports_data.csv',
            data = csv_buffer,
            length = len(csv_bytes),
            content_type='application/csv'
        )