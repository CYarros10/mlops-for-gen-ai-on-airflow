"""
Example Airflow DAG for Google Vertex AI GenOps.
"""

from datetime import datetime, timedelta
from uuid import uuid4
import logging

from vertexai.preview.evaluation import MetricPromptTemplateExamples
from vertexai.generative_models import HarmBlockThreshold, HarmCategory, Tool, grounding

from airflow import models
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.vertex_ai.generative_model import (
    GenerativeModelGenerateContentOperator,
    SupervisedFineTuningTrainOperator,
    CountTokensOperator,
    RunEvaluationOperator,
)
from airflow.providers.google.cloud.sensors.gcs import (
    GCSObjectExistenceSensor,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)

# --------------------------------------------------------------------------------------------------
# Constants / input
# --------------------------------------------------------------------------------------------------

PROJECT_ID = "cy-artifacts"
REGION = "us-central1"
BUCKET_NAME = "cy-sandbox"

# Models
PRO_MODEL = "gemini-1.0-pro-002"
FLASH_MODEL = "gemini-1.5-flash"

# Prompt Budgets
CHARACTER_BUDGET = 1000
TOKEN_BUDGET = 500

# To tune a model, you provide a training dataset. A training dataset must include a minimum of 16
# examples. For best results, we recommend that you provide at least 100 to 500 examples. The more
# examples you provide in your dataset, the better the results. There is no limit for the number of
# examples in a training dataset.
# https://cloud.google.com/vertex-ai/generative-ai/docs/models/gemini-supervised-tuning-about
TRAIN_DATA_BUCKET = "cloud-samples-data"
TRAIN_DATA_PATH = "ai-platform/generative_ai/gemini-1_5/text/sft_train_data.jsonl"

# System instructions are like a preamble added before the generative model is exposed further
# instructions from the user. It lets users steer the behavior of the model based on their specific
# needs and use cases. When you set a system instruction, you give the model additional context to
# understand the task, provide more customized responses, and adhere to specific guidelines over
# the full user interaction with the model. For developers, product-level behavior can be specified
# in system instructions, separate from prompts provided by end users
# https://cloud.google.com/vertex-ai/generative-ai/docs/learn/prompts/system-instructions
SYSTEM_INSTRUCTION = "Always respond in plain-text only."

# Per request settings for blocking unsafe content.
SAFETY_SETTINGS = {
    HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_ONLY_HIGH,
    HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_ONLY_HIGH,
    HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_ONLY_HIGH,
    HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_ONLY_HIGH,
}

# Optional. A piece of code that enables the system to interact with external systems to perform an
# action, or set of actions, outside of knowledge and scope of the model.
# https://cloud.google.com/vertex-ai/generative-ai/docs/grounding/overview
TOOLS = [Tool.from_google_search_retrieval(grounding.GoogleSearchRetrieval())]

# top_k: Specify a lower value for less random responses and a higher value for more random
#        responses. (1-40) default: model dependent.
# top_p: Specify a lower value for less random responses and a higher value for more random
#        responses. (0.0 - 1.0) default: model dependent.
# temperature: Temperature controls the degree of randomness in token selection. Lower temperatures
#              are good for prompts that require a less open-ended or creative response, while
#              higher temperatures can lead to more diverse or creative results.
DETERMINISTIC_GEN_CONFIG = {"top_k": 1, "top_p": 0.0, "temperature": 0.1}
RANDOM_GEN_CONFIG = {"top_k": 40, "top_p": 0.9, "temperature": 1.0}
BALANCED_GEN_CONFIG = {"top_k": 20, "top_p": 0.5, "temperature": 0.5}

# context: Inference-time text containing all information, which can be used in the generative model response.
# instruction: Instruction used at inference time.
# reference: Golden generative model response for reference.
# https://cloud.google.com/vertex-ai/generative-ai/docs/model-reference/evaluation#evaluate_an_output
CONTEXT="To make a classic spaghetti carbonara, start by bringing a large pot of salted water to a boil. While the water is heating up, cook pancetta or guanciale in a skillet with olive oil over medium heat until it's crispy and golden brown. Once the pancetta is done, remove it from the skillet and set it aside. In the same skillet, whisk together eggs, grated Parmesan cheese, and black pepper to make the sauce. When the pasta is cooked al dente, drain it and immediately toss it in the skillet with the egg mixture, adding a splash of the pasta cooking water to create a creamy sauce.",
INSTRUCTION = "Summarize the following article"
REFERENCE = "The process of making spaghetti carbonara involves boiling pasta, crisping pancetta or guanciale, whisking together eggs and Parmesan cheese, and tossing everything together to create a creamy sauce."
EVAL_DATASET = {
    "context": [
        CONTEXT
    ],
    "instruction": [INSTRUCTION],
    "reference": [
        REFERENCE    
    ],
}

# Metrics include model-based metrics, such as PointwiseMetric and PairwiseMetric, and in-memory
# computed metrics, such as rouge, bleu, and tool function-call metrics. PointwiseMetric and
# PairwiseMetric are generic model-based metrics that you can customize with your own criteria.
# Because the service takes the prediction results directly from models as input, the evaluation
# service can perform both inference and subsequent evaluation on all models supported by Vertex AI.
# https://cloud.google.com/vertex-ai/generative-ai/docs/model-reference/evaluation

# bleu/mean: Higher is better translation/generation quality
# bleu/std: Lower is more consistent output quality
# exactMatch/mean: Higher is better accuracy
# exactMatch/std: Lower is more consistent accuracy
# groundedness/mean: Higher is better understanding of context
# groundedness/std: Lower is more consistent understanding
# instructionFollowing/mean: Higher is better adherence to instructions
# instructionFollowing/std: Lower is more consistent instruction following
# rouge1/mean: Higher is better content similarity (single words)
# rouge1/std: Lower is more consistent unigram overlap
# rouge2/mean: Higher is better content similarity & fluency (two-word phrases)
# rouge2/std: Lower is more consistent bigram overlap
# rougeLSum/mean: Higher is better sentence-level similarity
# rougeLSum/std: Lower is more consistent sentence-level similarity
# rowCount: Higher is more evaluated responses (potentially more reliable results)
# summarizationQuality/mean: Higher is better human-rated summarization quality
# summarizationQuality/std: Lower is higher agreement among human raters
# verbosity/mean: Higher is longer responses, lower is shorter responses
# verbosity/std: Lower is more consistent response lengths
METRICS = [
    MetricPromptTemplateExamples.Pointwise.SUMMARIZATION_QUALITY,
    MetricPromptTemplateExamples.Pointwise.GROUNDEDNESS,
    MetricPromptTemplateExamples.Pointwise.VERBOSITY,
    MetricPromptTemplateExamples.Pointwise.INSTRUCTION_FOLLOWING,
    "exact_match",
    "bleu",
    "rouge_1",
    "rouge_2",
    "rouge_l_sum",
]
EXPERIMENT_NAME = "eval-experiment-airflow-operator"
EXPERIMENT_RUN_NAME = "eval-experiment-airflow-operator-run"
PROMPT_TEMPLATE = "{instruction}. Article: {context}. Summary:"

SAMPLE_PROMPT = f"{INSTRUCTION}. Article: {CONTEXT}"

# --------------------------------------------------------------------------------------------------
# Begin DAG generation
# --------------------------------------------------------------------------------------------------

with models.DAG(
    "model_comparison_dag_v1",
    description="Demonstration of Vertex AI GenOps on Airflow/Composer",
    tags=["demo", "vertex_ai", "generative_ai", "genops"],
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
    # Count tokens for sample prompt
    # ----------------------------------------------------------------------------------------------

    # The CountTokens API calculates the number of input tokens before sending a request to the
    # Gemini API. Use the CountTokens API to prevent requests from exceeding the model context
    # window, and estimate potential costs based on billable characters.
    # https://cloud.google.com/vertex-ai/generative-ai/docs/model-reference/count-tokens#python
    count_tokens_task = CountTokensOperator(
        task_id="count_tokens_task",
        project_id=PROJECT_ID,
        location=REGION,
        pretrained_model=PRO_MODEL,
        contents=[SAMPLE_PROMPT],
    )

    def validate_tokens(character_budget, token_budget, total_billable_characters, total_tokens):
        return int(total_billable_characters) < character_budget and int(total_tokens) < token_budget

    validate_tokens_task = PythonOperator(
        task_id="validate_tokens_task",
        python_callable=validate_tokens,
        op_kwargs={
            "character_budget": CHARACTER_BUDGET,
            "token_budget": TOKEN_BUDGET,
            "total_billable_characters": "{{ task_instance.xcom_pull(task_ids='count_tokens_task', key='total_billable_characters') }}",
            "total_tokens": "{{ task_instance.xcom_pull(task_ids='count_tokens_task', key='total_tokens') }}",
        },
        provide_context=True,
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
    # Run evaluations for various model configurations
    # ----------------------------------------------------------------------------------------------

    # The Gen AI Evaluation Service lets you evaluate your generative models, both
    # pointwise and pairwise, across several metrics, with your own criteria. You can provide
    # inference-time inputs, model responses and additional parameters, and the Gen AI Evaluation
    # Service returns metrics specific to the evaluation task.
    # https://cloud.google.com/vertex-ai/generative-ai/docs/model-reference/evaluation#python
    evaluate_pro_model_task = RunEvaluationOperator(
        task_id="evaluate_pro_model_task",
        project_id=PROJECT_ID,
        location=REGION,
        pretrained_model=PRO_MODEL,
        system_instruction=SYSTEM_INSTRUCTION,
        eval_dataset=EVAL_DATASET,
        metrics=METRICS,
        experiment_name=EXPERIMENT_NAME,
        experiment_run_name=f"{EXPERIMENT_RUN_NAME}-{uuid4()}",
        prompt_template=PROMPT_TEMPLATE,
    )
    evaluate_deterministic_model_task = RunEvaluationOperator(
        task_id="evaluate_deterministic_model_task",
        project_id=PROJECT_ID,
        location=REGION,
        pretrained_model=PRO_MODEL,
        system_instruction=SYSTEM_INSTRUCTION,
        eval_dataset=EVAL_DATASET,
        metrics=METRICS,
        experiment_name=EXPERIMENT_NAME,
        experiment_run_name=f"{EXPERIMENT_RUN_NAME}-{uuid4()}",
        prompt_template=PROMPT_TEMPLATE,
        generation_config=DETERMINISTIC_GEN_CONFIG,
    )
    evaluate_balanced_model_task = RunEvaluationOperator(
        task_id="evaluate_balanced_model_task",
        project_id=PROJECT_ID,
        location=REGION,
        pretrained_model=PRO_MODEL,
        system_instruction=SYSTEM_INSTRUCTION,
        eval_dataset=EVAL_DATASET,
        metrics=METRICS,
        experiment_name=EXPERIMENT_NAME,
        experiment_run_name=f"{EXPERIMENT_RUN_NAME}-{uuid4()}",
        prompt_template=PROMPT_TEMPLATE,
        generation_config=BALANCED_GEN_CONFIG,
    )
    evaluate_random_model_task = RunEvaluationOperator(
        task_id="evaluate_random_model_task",
        project_id=PROJECT_ID,
        location=REGION,
        pretrained_model=PRO_MODEL,
        system_instruction=SYSTEM_INSTRUCTION,
        eval_dataset=EVAL_DATASET,
        metrics=METRICS,
        experiment_name=EXPERIMENT_NAME,
        experiment_run_name=f"{EXPERIMENT_RUN_NAME}-{uuid4()}",
        prompt_template=PROMPT_TEMPLATE,
        generation_config=RANDOM_GEN_CONFIG,
    )
    evaluate_tuned_base_model_task = RunEvaluationOperator(
        task_id="evaluate_tuned_base_model_task",
        project_id=PROJECT_ID,
        location=REGION,
        pretrained_model="{{ task_instance.xcom_pull(task_ids='sft_train_base_task', key='tuned_model_endpoint_name') }}",
        system_instruction=SYSTEM_INSTRUCTION,
        eval_dataset=EVAL_DATASET,
        metrics=METRICS,
        experiment_name=EXPERIMENT_NAME,
        experiment_run_name=f"{EXPERIMENT_RUN_NAME}-{uuid4()}",
        prompt_template=PROMPT_TEMPLATE,
    )
    evaluate_flash_model_task = RunEvaluationOperator(
        task_id="evaluate_flash_model_task",
        project_id=PROJECT_ID,
        location=REGION,
        pretrained_model=FLASH_MODEL,
        system_instruction=SYSTEM_INSTRUCTION,
        eval_dataset=EVAL_DATASET,
        metrics=METRICS,
        experiment_name=EXPERIMENT_NAME,
        experiment_run_name=f"{EXPERIMENT_RUN_NAME}-{uuid4()}",
        prompt_template=PROMPT_TEMPLATE,
    )
    evaluate_grounded_model_task = RunEvaluationOperator(
        task_id="evaluate_grounded_model_task",
        project_id=PROJECT_ID,
        location=REGION,
        pretrained_model=PRO_MODEL,
        system_instruction=SYSTEM_INSTRUCTION,
        eval_dataset=EVAL_DATASET,
        metrics=METRICS,
        experiment_name=EXPERIMENT_NAME,
        experiment_run_name=f"{EXPERIMENT_RUN_NAME}-{uuid4()}",
        prompt_template=PROMPT_TEMPLATE,
    )

    # ----------------------------------------------------------------------------------------------
    # Submit prompt to various model configurations
    # ----------------------------------------------------------------------------------------------

    # The Gemini model family includes models that work with multimodal prompt requests. The term
    # multimodal indicates that you can use more than one modality, or type of input, in a prompt.
    # Models that aren't multimodal accept prompts only with text. Modalities can include text,
    # audio, video, and more. Generate a non-streaming model response from a text input.
    # https://cloud.google.com/vertex-ai/generative-ai/docs/model-reference/inference
    pro_model_task = GenerativeModelGenerateContentOperator(
        task_id="pro_model_task",
        project_id=PROJECT_ID,
        location=REGION,
        pretrained_model=PRO_MODEL,
        system_instruction=SYSTEM_INSTRUCTION,
        contents=[SAMPLE_PROMPT],
    )

    flash_model_task = GenerativeModelGenerateContentOperator(
        task_id="flash_model_task",
        project_id=PROJECT_ID,
        location=REGION,
        pretrained_model=FLASH_MODEL,
        system_instruction=SYSTEM_INSTRUCTION,
        contents=[SAMPLE_PROMPT],
    )

    deterministic_model_task = GenerativeModelGenerateContentOperator(
        task_id="deterministic_model_task",
        project_id=PROJECT_ID,
        location=REGION,
        pretrained_model=PRO_MODEL,
        system_instruction=SYSTEM_INSTRUCTION,
        contents=[SAMPLE_PROMPT],
        generation_config=DETERMINISTIC_GEN_CONFIG,
        safety_settings=SAFETY_SETTINGS,
    )

    balanced_model_task = GenerativeModelGenerateContentOperator(
        task_id="balanced_model_task",
        project_id=PROJECT_ID,
        location=REGION,
        pretrained_model=PRO_MODEL,
        system_instruction=SYSTEM_INSTRUCTION,
        contents=[SAMPLE_PROMPT],
        generation_config=BALANCED_GEN_CONFIG,
        safety_settings=SAFETY_SETTINGS,
    )

    random_model_task = GenerativeModelGenerateContentOperator(
        task_id="random_model_task",
        project_id=PROJECT_ID,
        location=REGION,
        pretrained_model=PRO_MODEL,
        system_instruction=SYSTEM_INSTRUCTION,
        contents=[SAMPLE_PROMPT],
        generation_config=RANDOM_GEN_CONFIG,
        safety_settings=SAFETY_SETTINGS,
    )

    tuned_base_model_task = GenerativeModelGenerateContentOperator(
        task_id="tuned_base_model_task",
        project_id=PROJECT_ID,
        location=REGION,
        pretrained_model="{{ task_instance.xcom_pull(task_ids='sft_train_base_task', key='tuned_model_endpoint_name') }}",
        system_instruction=SYSTEM_INSTRUCTION,
        contents=[SAMPLE_PROMPT],
        safety_settings=SAFETY_SETTINGS,
    )

    grounded_model_task = GenerativeModelGenerateContentOperator(
        task_id="grounded_model_task",
        project_id=PROJECT_ID,
        location=REGION,
        pretrained_model=PRO_MODEL,
        system_instruction=SYSTEM_INSTRUCTION,
        contents=[SAMPLE_PROMPT],
        tools=TOOLS,
        safety_settings=SAFETY_SETTINGS,
    )

    # ----------------------------------------------------------------------------------------------
    # Storing results for future analysis
    # ----------------------------------------------------------------------------------------------

    def send_report_to_gcs(bucket_name, model_name, sample_prompt, evaluation, generation_config, system_instruction, tools):
        """
        Writes data to a Google Cloud Storage bucket.

        Args:
            bucket_name: The name of the GCS bucket.
            model_name: the evaluated model 
            sample_prompt: the prompt the model was evaluated against.
            sample_response: typical response from model.
            evaluation: evaluation metrics.
        """
        now_timestamp = datetime.now()
        report=""
        import json
        try:
            # Ensure data is a string (decode if necessary)
            if isinstance(evaluation, bytes):
                evaluation = evaluation.decode('utf-8', 'replace')  # Replace invalid characters

            evaluation = evaluation.replace("NaN", "null").replace("'", "\"")

            # Convert the string to a dictionary using json.loads
            data = json.loads(evaluation)

            # fix keys for bigquery
            bq_safe = {}
            for key, value in data.items():
                new_key = key.replace("/", "_") 
                bq_safe[new_key] = value

            # Add the new key-value pair
            bq_safe["eval_date"] = now_timestamp.strftime("%Y%m%d")
            bq_safe["evaluated_model"] = model_name
            bq_safe["sample_prompt"] = sample_prompt.replace("\n","")
            bq_safe["generation_config"] = generation_config
            bq_safe["system_instruction"] = system_instruction
            bq_safe["tools"] = tools

            # Convert the updated dictionary back to a string using json.dumps
            report = json.dumps(bq_safe, ensure_ascii=False)  # Prevent Unicode errors

        except json.JSONDecodeError:
            raise ValueError("Invalid JSON string provided.")
        
        logging.info(report)
        partition = now_timestamp.strftime("%Y%m%d")
        blob_name = f"sample_evals/{partition}/{uuid4()}.json"

        gcs_hook = GCSHook()
        gcs_hook.upload(
            bucket_name=bucket_name,
            object_name=blob_name,
            data=report.encode('utf-8'),  # Encode to bytes before uploading
        )

        print(f"Data written to gs://{bucket_name}/{blob_name}")


    tuned_base_send_report_task = PythonOperator(
        task_id=f"tuned_base_send_report_task",
        python_callable=send_report_to_gcs,
        op_kwargs={
            "bucket_name": BUCKET_NAME,
            "model_name": "{{ task_instance.xcom_pull(task_ids='sft_train_base_task', key='tuned_model_endpoint_name') }}",
            "sample_prompt": SAMPLE_PROMPT,
            "evaluation": "{{ task_instance.xcom_pull(task_ids='evaluate_tuned_base_model_task', key='return_value') }}",
            "system_instruction": SYSTEM_INSTRUCTION,
            "generation_config": "",
            "tools": "",
        },
    )

    pro_send_report_task = PythonOperator(
        task_id=f"pro_send_report_task",
        python_callable=send_report_to_gcs,
        op_kwargs={
            "bucket_name": BUCKET_NAME,
            "model_name": PRO_MODEL,
            "sample_prompt": SAMPLE_PROMPT,
            "evaluation": "{{ task_instance.xcom_pull(task_ids='evaluate_pro_model_task', key='return_value') }}",
            "system_instruction": SYSTEM_INSTRUCTION,
            "generation_config": "",
            "tools": "",
        },
    )

    flash_send_report_task = PythonOperator(
        task_id=f"flash_send_report_task",
        python_callable=send_report_to_gcs,
        op_kwargs={
            "bucket_name": BUCKET_NAME,
            "model_name": FLASH_MODEL,
            "sample_prompt": SAMPLE_PROMPT,
            "evaluation": "{{ task_instance.xcom_pull(task_ids='evaluate_flash_model_task', key='return_value') }}",
            "system_instruction": SYSTEM_INSTRUCTION,
            "generation_config": "",
            "tools": "",
        },
    )

    deterministic_send_report_task = PythonOperator(
        task_id=f"deterministic_send_report_task",
        python_callable=send_report_to_gcs,
        op_kwargs={
            "bucket_name": BUCKET_NAME,
            "model_name": PRO_MODEL,
            "sample_prompt": SAMPLE_PROMPT,
            "evaluation": "{{ task_instance.xcom_pull(task_ids='evaluate_deterministic_model_task', key='return_value') }}",
            "system_instruction": SYSTEM_INSTRUCTION,
            "generation_config": DETERMINISTIC_GEN_CONFIG,
            "tools": "",
        },
    )

    random_send_report_task = PythonOperator(
        task_id=f"random_send_report_task",
        python_callable=send_report_to_gcs,
        op_kwargs={
            "bucket_name": BUCKET_NAME,
            "model_name": PRO_MODEL,
            "sample_prompt": SAMPLE_PROMPT,
            "evaluation": "{{ task_instance.xcom_pull(task_ids='evaluate_random_model_task', key='return_value') }}",
            "system_instruction": SYSTEM_INSTRUCTION,
            "generation_config": RANDOM_GEN_CONFIG,
            "tools": "",
        },
    )

    balanced_send_report_task = PythonOperator(
        task_id=f"balanced_send_report_task",
        python_callable=send_report_to_gcs,
        op_kwargs={
            "bucket_name": BUCKET_NAME,
            "model_name": PRO_MODEL,
            "sample_prompt": SAMPLE_PROMPT,
            "evaluation": "{{ task_instance.xcom_pull(task_ids='evaluate_balanced_model_task', key='return_value') }}",
            "system_instruction": SYSTEM_INSTRUCTION,
            "generation_config": BALANCED_GEN_CONFIG,
            "tools": "",
        },
    )

    grounded_send_report_task = PythonOperator(
        task_id=f"grounded_send_report_task",
        python_callable=send_report_to_gcs,
        op_kwargs={
            "bucket_name": BUCKET_NAME,
            "model_name": PRO_MODEL,
            "sample_prompt": SAMPLE_PROMPT,
            "evaluation": "{{ task_instance.xcom_pull(task_ids='evaluate_grounded_model_task', key='return_value') }}",
            "system_instruction": SYSTEM_INSTRUCTION,
            "generation_config": "",
            "tools": "Google Search Retrieval",
        },
    )

    # ----------------------------------------------------------------------------------------------
    # Dependencies
    # ----------------------------------------------------------------------------------------------

    # TUNED MODEL PIPELINE
    training_data_exists_sensor >> count_tokens_task >> validate_tokens_task >> sft_train_base_task >> evaluate_tuned_base_model_task >> tuned_base_model_task >> tuned_base_send_report_task
    
    # PRO MODEL PIPELINE
    training_data_exists_sensor >> count_tokens_task >> validate_tokens_task >> evaluate_pro_model_task  >> pro_model_task >> pro_send_report_task
    
    # BALANCED PRO MODEL PIPELINE
    training_data_exists_sensor >> count_tokens_task >> validate_tokens_task >> evaluate_balanced_model_task >> balanced_model_task >> balanced_send_report_task
    
    # DETERMINISTIC PRO MODEL PIPELINE
    training_data_exists_sensor >> count_tokens_task >> validate_tokens_task >> evaluate_deterministic_model_task  >> deterministic_model_task >> deterministic_send_report_task
   
    # RANDOM PRO MODEL PIPELINE
    training_data_exists_sensor >> count_tokens_task >> validate_tokens_task >> evaluate_random_model_task >> random_model_task >> random_send_report_task
    
    # FLASH MODEL PIPELINE
    training_data_exists_sensor >> count_tokens_task >> validate_tokens_task >> evaluate_flash_model_task  >> flash_model_task >> flash_send_report_task
    
    # GROUNDED MODEL PIPELINE
    training_data_exists_sensor >> count_tokens_task >> validate_tokens_task >> evaluate_grounded_model_task >> grounded_model_task >> grounded_send_report_task
