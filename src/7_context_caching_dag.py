
"""
Example Airflow DAG for Google Vertex AI Generative Model Context Caching
"""

from datetime import datetime

from vertexai.generative_models import HarmBlockThreshold, HarmCategory, Part

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.vertex_ai.generative_model import (
    CreateCachedContentOperator,
    GenerateFromCachedContentOperator,
)

PROJECT_ID = "your-project"
DAG_ID = "context_caching_dag_v1"
REGION = "us-central1"

GENERATION_CONFIG = {"max_output_tokens": 256, "top_p": 0.95, "temperature": 0.0}
SAFETY_SETTINGS = {
    HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_ONLY_HIGH,
    HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_ONLY_HIGH,
    HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_ONLY_HIGH,
    HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_ONLY_HIGH,
}

CACHED_MODEL = "gemini-1.5-pro-002"
CACHED_SYSTEM_INSTRUCTION = """
You are an expert researcher. You always stick to the facts in the sources provided, and never make up new facts.
Now look at these research papers, and answer the following questions.
"""
CACHED_CONTENTS = [
    Part.from_uri(
        "gs://cloud-samples-data/generative-ai/pdf/2312.11805v3.pdf",
        mime_type="application/pdf",
    ),
    Part.from_uri(
        "gs://cloud-samples-data/generative-ai/pdf/2403.05530.pdf",
        mime_type="application/pdf",
    ),
]

with DAG(
    dag_id=DAG_ID,
    description="Sample DAG with generative models.",
    schedule="@once",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example", "vertex_ai", "generative_model"],
) as dag:

    create_cached_content_task = CreateCachedContentOperator(
        task_id="create_cached_content_task",
        project_id=PROJECT_ID,
        location=REGION,
        model_name=CACHED_MODEL,
        system_instruction=CACHED_SYSTEM_INSTRUCTION,
        contents=CACHED_CONTENTS,
        ttl_hours=1,
        display_name="example-cache",
    )

    generate_from_cached_content_task = GenerateFromCachedContentOperator(
        task_id="generate_from_cached_content_task",
        project_id=PROJECT_ID,
        location=REGION,
        cached_content_name="{{ task_instance.xcom_pull(task_ids='create_cached_content_task', key='return_value') }}",
        contents=["What are the papers about?"],
        generation_config=GENERATION_CONFIG,
        safety_settings=SAFETY_SETTINGS,
    )

    create_cached_content_task >> generate_from_cached_content_task
