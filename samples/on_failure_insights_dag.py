"""
Example Airflow DAG for Google Vertex AI Generative Model.
"""

import random
import logging
from datetime import datetime, timedelta
from functools import partial
from airflow import models, AirflowException
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.vertex_ai.generative_model import (
    GenerativeModelGenerateContentOperator
)

#---------------------
# Universal DAG info
#---------------------

PROJECT_ID = "your-project"
LOCATION = "us-central1"

ON_DAG_FAILURE_ALERT = "Airflow DAG Failure:"
ON_TASK_FAILURE_ALERT = "Airflow Task Failure:"
ON_SLA_MISS_ALERT = "Airflow DAG SLA Miss:"

ANALYSIS_PROMPT = """
You are an Airflow expert troubleshooter. Analyze the task failure and respond in a single line with this exact format:
"Task failed because [technical reason + root cause] - Recommend [step-by-step fix + research topics]"
Consider:

Task type and its specific failure patterns
Both technical error details and underlying causes
Python/Airflow logs and stack traces
System state and configurations
Common failure patterns for this task type
Resource constraints or environmental factors

Your response must:

Explain the failure clearly combining technical and contextual details
Provide actionable troubleshooting steps
Suggest specific solutions
Recommend relevant topics to research
Include task-specific guidance
Maintain single line format

Example: "Task failed because BigQuery API timeout during large data load (insufficient query slots) - Recommend 1) increase slot quota 2) implement backoff retry 3) research BigQuery quotas and concurrent query optimization"
"""

#-------------------------
# Callback Functions
#-------------------------

def log_on_task_failure_with_insights(context, project_id, location):
    """collect DAG information and send to console.log on failure."""
    task_instance = context.get('task_instance')
    dag_id = task_instance.dag_id
    task_id = task_instance.task_id
    execution_date = context.get('execution_date')
    exception = context.get('exception')

    gemini_insights_task = GenerativeModelGenerateContentOperator(
        task_id="gemini_insights_task",
        project_id=project_id,
        location=location,
        contents= [
            ANALYSIS_PROMPT,
            f"{context}"
        ],
        pretrained_model="gemini-1.5-pro"
    )

    gemini_insights = gemini_insights_task.execute(context)

    log_msg = f"""{ON_TASK_FAILURE_ALERT} | DAG: {dag_id} | Task ID: {task_id} | Execution Date: {execution_date} | Exception: {exception} | Gemini Insights: {gemini_insights}"""

    logging.info(log_msg)

#-------------------------
# Begin DAG Generation
#-------------------------
with models.DAG(
    f"on_failure_insights_v1",
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
        "execution_timeout": timedelta(minutes=60),
        "on_failure_callback": partial(
            log_on_task_failure_with_insights,
            project_id=PROJECT_ID,
            location=LOCATION
        )
    },
) as dag:

    def sample_run():
        # 50% chance to raise an exception
        if random.randint(0,4) % 2 == 0:
            raise AirflowException("Error msg")

    sample_task_1 = PythonOperator(
        task_id='sample_task_1', 
        python_callable=sample_run,
    )

