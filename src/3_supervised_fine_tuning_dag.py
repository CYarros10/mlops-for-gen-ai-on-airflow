"""
Example Airflow DAG for Google Vertex AI Supervised Fine Tuning.
"""

from datetime import datetime, timedelta

from airflow import models
from airflow.providers.google.cloud.operators.vertex_ai.generative_model import (
    SupervisedFineTuningTrainOperator,
)
from airflow.providers.google.cloud.sensors.gcs import (
    GCSObjectExistenceSensor,
)

# --------------------------------------------------------------------------------------------------
# Constants / input
# --------------------------------------------------------------------------------------------------

PROJECT_ID = "cy-artifacts"
REGION = "us-central1"

# TODO: make a list of all available models. loop through it
PRO_MODEL = "gemini-1.0-pro-002"

# To tune a model, you provide a training dataset. A training dataset must include a minimum of 16
# examples. For best results, we recommend that you provide at least 100 to 500 examples. The more
# examples you provide in your dataset, the better the results. There is no limit for the number of
# examples in a training dataset.
# https://cloud.google.com/vertex-ai/generative-ai/docs/models/gemini-supervised-tuning-about
TRAIN_DATA_BUCKET = "cloud-samples-data"
TRAIN_DATA_PATH = "ai-platform/generative_ai/gemini-1_5/text/sft_train_data.jsonl"

# --------------------------------------------------------------------------------------------------
# Begin DAG generation
# --------------------------------------------------------------------------------------------------

with models.DAG(
    "supervised_fine_tuning_dag_v1",
    description="Demonstration of Vertex AI Supervised Fine Tuning on Airflow/Composer",
    tags=["demo", "vertex_ai", "generative_ai", "tuning", "genops"],
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
    },
) as dag:
    # ----------------------------------------------------------------------------------------------
    # Wait for training data to arrive
    # ----------------------------------------------------------------------------------------------

    training_data_exists_sensor = GCSObjectExistenceSensor(
        task_id="training_data_exists_sensor",
        bucket=TRAIN_DATA_BUCKET,
        object=TRAIN_DATA_PATH,
    )

    # ----------------------------------------------------------------------------------------------
    # Create a tuned model based on a provided training dataset. (may take up to 1 hour)
    # ----------------------------------------------------------------------------------------------

    # Supervised fine-tuning is a good option when you have a well-defined task with available
    # labeled data. It's particularly effective for domain-specific applications where the language
    # or content significantly differs from the data the large model was originally trained on.
    # Supervised fine-tuning adapts model behavior with a labeled dataset. This process adjusts the
    # model's weights to minimize the difference between its predictions and the actual labels
    # note: not all models support tuning.
    # https://cloud.google.com/vertex-ai/generative-ai/docs/models/gemini-supervised-tuning
    sft_train_base_task = SupervisedFineTuningTrainOperator(
        task_id="sft_train_base_task",
        project_id=PROJECT_ID,
        location=REGION,
        source_model=PRO_MODEL,
        train_dataset=f"gs://{TRAIN_DATA_BUCKET}/{TRAIN_DATA_PATH}",
    )

    # ----------------------------------------------------------------------------------------------
    # Dependencies
    # ----------------------------------------------------------------------------------------------

    training_data_exists_sensor >> sft_train_base_task
