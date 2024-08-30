from airflow.decorators import dag, task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago

@dag(
    dag_id="Spark_processing",
    default_args={
        "owner": "YNS_Bousetta",
        "start_date": days_ago(0),
    },
    schedule_interval="@daily",
    catchup=False,
    tags=["Spark_Processing", "Data_inserting_to_cassandra"]
)
def spark_processing():

    @task
    def start_starting():
        print("The Job Is starting ....")

    @task
    def Python_Processing_job():
        python_job = SparkSubmitOperator(
            task_id="The_Processiong_JOB",
            conn_id="connection",
            application="jobs/spark_processing.py",
            packages="org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,"
                     "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,"
                     "org.apache.spark:spark-avro_2.12:3.4.1"
        )
        return python_job.execute({})

    @task
    def the_ending_of_the_job():    
        print("Jobs Completed Successfully ....")
    
    # Define task dependencies
    start_starting() >> Python_Processing_job() >> the_ending_of_the_job()

# Instantiate the DAG
spark_processing()
