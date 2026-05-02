from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator
from airflow.sdk import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta


# Retry config (important)
default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id='nasa_apod_postgres',
    start_date=datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False,
    default_args=default_args
) as dag:

    # Step 1: Create table
    @task
    def create_table():
        postgres_hook = PostgresHook(postgres_conn_id="my_postgres_connection")

        create_query = """
        CREATE TABLE IF NOT EXISTS apod_data (
            date DATE PRIMARY KEY,
            title VARCHAR(255),
            explanation TEXT,
            url TEXT,
            media_type VARCHAR(50)
        );
        """

        postgres_hook.run(create_query)

    # Step 2: Extract
    extract_apod = HttpOperator(
        task_id='extract_apod',
        http_conn_id='nasa_api',
        endpoint='planetary/apod?api_key={{ conn.nasa_api.password }}',
        method='GET',
        response_filter=lambda response: response.json(),
    )

    # Step 3: Transform
    @task
    def transform_apod_data(response):
        if response.get("media_type") != "image":
            return None

        return {
            'title': response.get('title', ''),
            'explanation': response.get('explanation', ''),
            'url': response.get('url', ''),
            'date': response.get('date', ''),
            'media_type': response.get('media_type', '')
        }

    # Step 4: Load
    @task
    def load_data_to_postgres(apod_data):
        if apod_data is None:
            return

        postgres_hook = PostgresHook(postgres_conn_id='my_postgres_connection')

        insert_query = """
        INSERT INTO apod_data (date, title, explanation, url, media_type)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (date)
        DO UPDATE SET
            title = EXCLUDED.title,
            explanation = EXCLUDED.explanation,
            url = EXCLUDED.url,
            media_type = EXCLUDED.media_type;
        """

        postgres_hook.run(
            insert_query,
            parameters=(
                apod_data['date'],
                apod_data['title'],
                apod_data['explanation'],
                apod_data['url'],
                apod_data['media_type']
            )
        )

    # Dependencies
    table = create_table()
    api_response = extract_apod.output
    transformed = transform_apod_data(api_response)
    load = load_data_to_postgres(transformed)

    table >> extract_apod >> transformed >> load