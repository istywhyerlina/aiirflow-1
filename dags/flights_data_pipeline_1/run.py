from airflow.decorators import dag, task, task_group
from airflow.operators.python import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from datetime import datetime
from helper.minio import MinioClient
from helper.read_sql import read_sql_file
from flights_data_pipeline_1.tasks.extract import Extract
from flights_data_pipeline_1.tasks.load import Load


@dag(
    dag_id = 'flights_data_pipeline_1',
    start_date = datetime.now(),
    schedule = "@daily",
    catchup = False
)
def flights_data_pipeline_1():
    @task
    def create_bucket():
        minio_client = MinioClient._get()
        bucket_name = 'extracted-data'
        
        if not minio_client.bucket_exists(bucket_name):
            minio_client.make_bucket(bucket_name)

    @task_group
    def extract():
        extract_aircrafts_data = PythonOperator(
            task_id = 'extract_aircrafts_data',
            python_callable = Extract._extract_src,
            op_kwargs={'table_name': 'aircrafts_data'},

        )

        extract_airports_data = PythonOperator(
            task_id = 'extract_airports_data',
            python_callable = Extract._extract_src,
            op_kwargs={'table_name': 'airports_data'},
        )

        extract_boarding_passes = PythonOperator(
            task_id = 'extract_boarding_passes',
            python_callable = Extract._extract_src,
            op_kwargs={'table_name': 'boarding_passes'},
        )

        extract_bookings = PythonOperator(
            task_id = 'extract_bookings',
            python_callable = Extract._extract_src,
            op_kwargs={'table_name': 'bookings'},
        )

        extract_flights = PythonOperator(
            task_id = 'extract_flights',
            python_callable = Extract._extract_src,
            op_kwargs={'table_name': 'flights'},
        )

        extract_seats = PythonOperator(
            task_id = 'extract_seats',
            python_callable = Extract._extract_src,
            op_kwargs={'table_name': 'seats'},
        )

        extract_ticket_flights = PythonOperator(
            task_id = 'extract_ticket_flights',
            python_callable = Extract._extract_src,
            op_kwargs={'table_name': 'ticket_flights'},
        )

        extract_tickets = PythonOperator(
            task_id = 'extract_tickets',
            python_callable = Extract._extract_src,
            op_kwargs={'table_name': 'tickets'},
        )

  
        extract_aircrafts_data 
        extract_airports_data 
        extract_boarding_passes 
        extract_bookings 
        extract_flights 
        extract_seats 
        extract_ticket_flights 
        extract_tickets

    @task_group
    def load():
        load_aircrafts_data = PythonOperator(
            task_id = 'load_aircrafts_data',
            python_callable = Load._load_minio,
            op_kwargs={'table_name': 'aircrafts_data'},

        )
        load_airports_data = PythonOperator(
            task_id = 'load_airports_data',
            python_callable = Load._load_minio,
            op_kwargs={'table_name': 'airports_data'},

        )

        load_bookings = PythonOperator(
            task_id = 'load_bookings',
            python_callable = Load._load_minio,
            op_kwargs={'table_name': 'bookings'},

        )


        load_tickets = PythonOperator(
            task_id = 'load_tickets',
            python_callable = Load._load_minio,
            op_kwargs={'table_name': 'tickets'},

        )

        load_seats = PythonOperator(
            task_id = 'load_seats',
            python_callable = Load._load_minio,
            op_kwargs={'table_name': 'seats'},

        )

        load_flights = PythonOperator(
            task_id = 'load_flights',
            python_callable = Load._load_minio,
            op_kwargs={'table_name': 'flights'},

        )
        load_ticket_flights = PythonOperator(
            task_id = 'load_ticket_flights',
            python_callable = Load._load_minio,
            op_kwargs={'table_name': 'ticket_flights'},

        )

        load_boarding_passes = PythonOperator(
            task_id = 'load_boarding_passes',
            python_callable = Load._load_minio,
            op_kwargs={'table_name': 'boarding_passes'},

        )
        load_aircrafts_data >> load_airports_data  >> load_bookings >> load_tickets >> load_seats >> load_flights >> load_ticket_flights >> load_boarding_passes

    @task_group
    def transform():
        sql_dim_aircrafts = read_sql_file(f'flights_data_pipeline_1/query_transform/dim_aircrafts.sql')
        transform_dim_aircrafts = PostgresOperator(
            task_id = 'transform_dim_aircrafts',
            sql = sql_dim_aircrafts,
            postgres_conn_id = 'dwh',
            autocommit = True)
        

        sql_dim_airport = read_sql_file(f'flights_data_pipeline_1/query_transform/dim_airport.sql')
        transform_dim_airport = PostgresOperator(
            task_id = 'transform_dim_airport',
            sql = sql_dim_airport,
            postgres_conn_id = 'dwh',
            autocommit = True)
        
        sql_dim_passenger = read_sql_file(f'flights_data_pipeline_1/query_transform/dim_passenger.sql')
        transform_dim_passenger = PostgresOperator(
            task_id = 'transform_dim_passenger',
            sql = sql_dim_passenger,
            postgres_conn_id = 'dwh',
            autocommit = True)
        
        sql_dim_seat = read_sql_file(f'flights_data_pipeline_1/query_transform/dim_seat.sql')
        transform_dim_seat = PostgresOperator(
            task_id = 'transform_dim_seat',
            sql = sql_dim_seat,
            postgres_conn_id = 'dwh',
            autocommit = True)
        

        sql_fct_boarding_pass = read_sql_file(f'flights_data_pipeline_1/query_transform/fct_boarding_pass.sql')
        transform_fct_boarding_pass = PostgresOperator(
            task_id = 'transform_fct_boarding_pass',
            sql = sql_fct_boarding_pass,
            postgres_conn_id = 'dwh',
            autocommit = True)
        
        sql_fct_booking_ticket = read_sql_file(f'flights_data_pipeline_1/query_transform/fct_booking_ticket.sql')
        transform_fct_booking_ticket = PostgresOperator(
            task_id = 'transform_fct_booking_ticket',
            sql = sql_fct_booking_ticket,
            postgres_conn_id = 'dwh',
            autocommit = True)
        
        sql_fct_flight_activity = read_sql_file(f'flights_data_pipeline_1/query_transform/fct_flight_activity.sql')
        transform_fct_flight_activity = PostgresOperator(
            task_id = 'transform_fct_flight_activity',
            sql = sql_fct_flight_activity,
            postgres_conn_id = 'dwh',
            autocommit = True)
        
        sql_fct_seat_occupied_daily = read_sql_file(f'flights_data_pipeline_1/query_transform/fct_seat_occupied_daily.sql')
        transform_fct_seat_occupied_daily = PostgresOperator(
            task_id = 'transform_fct_seat_occupied_daily',
            sql = sql_fct_seat_occupied_daily,
            postgres_conn_id = 'dwh',
            autocommit = True)
        
        transform_dim_aircrafts >> transform_dim_airport >> transform_dim_passenger >> transform_dim_seat >> transform_fct_boarding_pass >> transform_fct_booking_ticket >> transform_fct_flight_activity >> transform_fct_seat_occupied_daily


    create_bucket() >> extract() >> load() >> transform()

flights_data_pipeline_1()