from airflow import DAG
from airflow.operators.python import PythonOperator
from pendulum import datetime, timedelta
import os
import requests
import gzip
import pandas

BASE_URL = (
    'https://dumps.wikimedia.org/other/pageviews/2025/2025-10/'
)

DATA_PATH = "/opt/airflow/data"
RAW_PATH = f"{DATA_PATH}/raw"
EXTRACTED_PATH = f"{DATA_PATH}/extracted"
PROCESSED_PATH = f"{DATA_PATH}/processed"
DATABASE_PATH = f"{DATA_PATH}/pageviews.db"

COMPANIES = {
    "Amazon": ["Amazon"],
    "Apple": ["Apple_Inc.", "Apple"],
    "Facebook": ["Facebook"],
    "Google": ["Google"],
    "Microsoft": ["Microsoft"]
}

def download_pageviews():
    os.makedirs(RAW_PATH, exist_ok=True)

    local_file = f"{RAW_PATH}/pageviews.gz"

    response = requests.get(DATA_PATH_URL, stream=True)
    response.raise_for_status()

    with open(local_file, "wb") as f:
        for part in response.iter_content(part_size=1024):
            f.write(part)

    print(f"Downloaded file to {local_file}")

def extract_pageviews():
    os.makedirs(EXTRACTED_PATH, exist_ok=True)

    input_file = f"{RAW_PATH}/pageviews.gz"
    output_file = f"{EXTRACTED_PATH}/pageviews.txt"

    with gzip.open(input_file, "rt", encoding="utf-8") as gz_file:
        with open(output_file, "w", encoding="utf-8") as out_file:
            for line in gz_file:
                out_file.write(line)

    print(f"Extracted data to {output_file}")


def filter_companies():
    os.makedirs(PROCESSED_PATH, exist_ok=True)

    input_file = f"{EXTRACTED_PATH}/pageviews.txt"
    output_file = f"{PROCESSED_PATH}/company_pageviews.csv"

    records = []

    with open(input_file, "r", encoding="utf-8") as f:
        for line in f:
            project, page, views, _ =line.strip().split(" ")

            for company, keywords in COMPANIES.items():
                if page in keywords:
                    records.append({
                        "company": company,
                        "page": page,
                        "views": int(views)
                    })

    df = pd.DataFrame(records)
    df.to_csv(output_file, index=False)

    print(f"Filtered data saved to {output_file}")

def load_to_database():
    csv_file = f"{PROCESSED_PATH}/company_pageviews.csv"

    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS pageviews (
            company TEXT,
            page TEXT,
            views INTEGER
        )
    """)

    df = pd.read_csv(csv_file)
    df.to_sql("pageviews", conn, if_exists="replace", index=False)

    conn.commit()
    conn.close()

    print("Data loaded into SQLite database")

def analyze_pageviews():
    conn = sqlite3.connect(DATABASE_PATH)

    query = """
        SELECT company, SUM(views) AS total_views
        FROM pageviews
        GROUP BY company
        ORDER BY total_views DESC
        LIMIT 1
    """

    result = pd.read_sql(query, conn)
    conn.close()

    print("Company with highest pageviews:")
    print(result)

def wiki_pageviews_dag():
    default_args = {
    "owner": "launchsentiment",
    "depend_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    }

    with DAG(
        dag_id="wiki_pageviews_pipeline",
        default_args=default_args,
        description="Download and analyze Wikipedia pageviews for sentiment analysis",
        start_date=datetime(2025, 12, 31),
        schedule_interval='@once',
        catchup=False,
        tags=["wikipedia", "sentiment"], 
    ) as dag:
    
        download_task = PythonOperator(
            task_id="download_pageviews",
            python_callable=lambda: None
        )

        extract_task = PythonOperator(
            task_id="extract_pageviews",
            python_callable=lambda: None
        )


        transform_task = PythonOperator(
            task_id="filter_companies",
            python_callable=lambda: None
        )

        load_task = PythonOperator(
            task_id="load_to_db",
            python_callable=lambda: None
        )

        analyze_task = PythonOperator(
            task_id="analyze_pageviews",
            python_callable=lambda: None
        )

        download_task>>extract_task>>transform_task>>load_task>>analyze_task
    
    return dag

dag = wiki_pageviews_dag()

