"""
Example Airflow DAG for Google Vertex AI Generative Model.
"""

import random
import logging
from datetime import datetime, timedelta
from airflow import models, AirflowException
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.vertex_ai.generative_model import (
    GenerativeModelGenerateContentOperator
)

#---------------------
# Universal DAG info
#---------------------
ON_DAG_FAILURE_ALERT = "Airflow DAG Failure:"
BASE_PROMPT = """
You're a Data Engineer and Apache Airflow expert tasked with debugging Airflow DAG Errors. 
Provide possible causes, next steps, and any additional troubleshooting information. Always follow 
these rules:

1. Respond in plain-text pararaph only.
2. Respond in less than 100 words.
"""

#-------------------------
# Callback Functions
#-------------------------

def log_on_dag_failure(context):
    """
    collect DAG information and send to console.log on failure.
    """

    gemini_insights_task = GenerativeModelGenerateContentOperator(
        task_id="gemini_insights_task",
        project_id="your-project",
        location="us-central1",
        contents= [
            f"{BASE_PROMPT} ... Help us understand what happened here: {context}."
        ],
        pretrained_model="gemini-1.5-flash-001"
    )

    gemini_insights = gemini_insights_task.execute(context)

    dag = context.get('dag')

    log_msg = f"""
    {ON_DAG_FAILURE_ALERT}
    *DAG*: {dag.dag_id}
    *DAG Description*: {dag.description}
    *DAG Tags*: {dag.tags}
    *Context*: {context}
    *Gemini Insight*: {gemini_insights}
    """

    logging.info(log_msg)

#-------------------------
# Begin DAG Generation
#-------------------------
with models.DAG(
    f"enhanced_logging_dag",
    schedule="@once",
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    default_args={
        "start_date": datetime(2024, 1, 1),
        "owner": "Google",
        "depends_on_past": False,
        "retries": 0,
        "retry_delay": timedelta(minutes=1),
        "sla": timedelta(minutes=55),
        "execution_timeout": timedelta(minutes=60)
    },
    on_failure_callback=log_on_dag_failure,
) as dag:

    def sample_run():
        # 50% chance to raise an exception
        if random.randint(0,4) % 2 == 0:
            raise AirflowException("Error msg")

    sample_task_1 = PythonOperator(
        task_id='sample_task_1', 
        python_callable=sample_run,
    )

