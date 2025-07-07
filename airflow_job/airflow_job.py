from datetime import datetime, timedelta
import uuid
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchoperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.models import Variable

#DAG Default arguments
daefault_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 7, 6),
}

#Define the DAG
with DAG(
    dag_id="flight_booking_dataproc_bq_dag",
    daefault_args=daefault_args,
    schedule_interval=None,
    catcup=False,
) as dag:
    
    #Fetch environment variables
    env = Variable.get("env", default_var="dev")
    gcs_bucket = Variable.get("gcs_bucket",default_var="airflow-projects-gds")
    bq_project = Variable.get("bq_project", default_var="helical-cursor-465004-n8")
    bq_dataset = Variable.get("bq_dataset", default_var=f"flight_data_{env}")
    tables = Variable.get("table", deserialize_json=True)

    #Extract table names from the "tables" variable
    transformed_table = tables["transformed_table"]
    route_insights_table = tables["route_insights_table"]
    origin_insights_table = tables["origin_insights_table"]

    #Generate a unique batch id using uiid
    batch_id = f"flight-booking-batch-{env}--{str(uuid.uuid4())[:8]}"

    # Task1 : File sensor for GCS

    file_sensor = GCSObjectExistenceSensor(
        task_id = "check_file_arrival",
        bucket = gcs_bucket,
        object=f"airflow-project-1/source-{env}/flight_booking.csv" # full file path in gcs
        google_cloud_conn_id="google_cloud_default",
        timeout=300,
        poke_interval=30,
        mode="poke",
    )

    # task 2: Submit Pyspark job t Dataproc Serverless
    batch_details = {
        "pyspark_batch": {
            "main_python_file_uri" : f"gs://{gcs_bucket}/airflow-project-1/spark-job/spark-transformation_job.py",
            "python_file_uris" : [],
            "jar_file_uris" : [],
            "args" : [
                f"--env={env}",
                f"--bq_project={bq_project}",
                f"--bq_dataset={bq_dataset}",
                f"--transformed_table={transformed_table}",
                f"--route_insights_table={route_insights_table}",
                f"--origin_insights_table={origin_insights_table}",
            ]
        },
        "runtime_config": {
            "version": "2.2",
        },
        "environment_config": {
            "execution_config": {
                "service_account" : "",
                "network_uri": "",
                "subnetwork_uri": ""
            }
        },

    }

    pyspark_task = DataprocCreateBatchoperator(
        task_id = "run_spark_job_on_dataproc_serverless",
        batch=batch_details,
        batch_id=batch_id,
        project_id="",
        region="us-central",
        gcp_conn_id="google_cloud_default",
    )

    # task dependcies
    file_sensor >> pyspark_task