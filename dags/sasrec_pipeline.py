from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'minhk',
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'sasrec_recommender_system',
    default_args=default_args,
    description='Production Pipeline',
    schedule_interval='@weekly',
    start_date=datetime(2025, 12, 7),
    catchup=False,
    tags=['production', 'sasrec']
) as dag:

    # -----------------------------------------------------------
    # TASK 1: Spark ETL
    # -----------------------------------------------------------
    task_etl = BashOperator(
        task_id='spark_etl_process',
        bash_command="""
            echo "====================================================="
            echo "🚀 AIRFLOW WEEKLY WINDOW"
            echo "START_DATE = {{ dag_run.conf.get('start_date', macros.ds_add(ds, -7)) }}"
            echo "END_DATE   = {{ dag_run.conf.get('end_date', ds) }}"
            echo "====================================================="
            echo "🔥 Running Spark ETL..."
            docker exec -w /home/spark/work spark-master spark-submit \
            --verbose \
            --packages org.apache.spark:spark-hadoop-cloud_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4 \
            src/processing/batch/batch_etl_train.py \
            {{ dag_run.conf.get('start_date', macros.ds_add(ds, -7)) }} \
            {{ dag_run.conf.get('end_date', ds) }}

            echo "✅ ETL DONE"
        """
    )


    # -----------------------------------------------------------
    # TASK 2: Auto Train
    # -----------------------------------------------------------
    task_train = BashOperator(
        task_id='model_fine_tuning',
        bash_command="""
            echo "🚀 [TRAIN] Đang chạy training..."

            docker exec spark-master pip install mlflow tensorflow pandas s3fs && \

            docker exec \
            -e MLFLOW_TRACKING_URI=http://mlflow:5000 \
            -e MLFLOW_EXPERIMENT_NAME=auto_train_recsys \
            -w /home/spark/work \
            spark-master \
            python3 src/ai_core/auto_train.py
        """
    )

    # -----------------------------------------------------------
    # TASK 3: Evaluation
    # -----------------------------------------------------------
    task_eval = BashOperator(
        task_id='model_evaluation',
        bash_command="""
            echo "🚀 [EVAL] Đang đánh giá model..."
            docker exec spark-master pip install tensorflow pandas s3fs tqdm && \
            docker exec -w /home/spark/work spark-master python3 src/ai_core/evaluate_metrics.py
        """
    )

    task_etl >> task_train >> task_eval