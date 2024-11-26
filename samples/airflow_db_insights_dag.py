from datetime import datetime, timedelta
from typing import List, Optional, Dict
from vertexai.generative_models import Part
from airflow import models
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.providers.google.cloud.operators.vertex_ai.generative_model import GenerativeModelGenerateContentOperator

# Configuration
formatted_date = datetime.today().strftime("%Y_%m_%d")
PROJECT_ID = "cy-artifacts"
LOCATION = "us-central1"
MODEL = "gemini-1.5-pro-002"
BUCKET = "cy-genai"
GCS_OBJECT_PATH = f"composer-daily-reports/{formatted_date}"
POSTGRES_CONNECTION_ID = "airflow_db"
FILE_FORMAT = "csv"
LOOKBACK_DAYS = 1

# SQL Queries Dictionary
# Comprehensive dictionary of optimized Airflow monitoring queries
# Usage: LOOKBACK_DAYS should be defined (e.g., LOOKBACK_DAYS = 1)

QUERIES = {
    "dag_runs": f"""
        SELECT 
            dag_id,
            run_id,
            state,
            start_date,
            end_date,
            data_interval_start,
            data_interval_end,
            external_trigger,
            EXTRACT(EPOCH FROM (end_date - start_date)) as duration_seconds
        FROM dag_run 
        WHERE start_date > NOW() - INTERVAL '{LOOKBACK_DAYS} day'
            AND start_date < NOW() + INTERVAL '1 day'
        ORDER BY start_date DESC
        LIMIT 10000;
    """,
    
    "task_durations": f"""
    WITH base_durations AS (
        SELECT 
            dag_id,
            task_id,
            start_date,
            end_date,
            state,
            EXTRACT(EPOCH FROM (end_date - start_date)) as duration_seconds
        FROM task_instance
        WHERE start_date >= CURRENT_DATE - INTERVAL '{LOOKBACK_DAYS} day'
            AND start_date < CURRENT_DATE + INTERVAL '1 day'
            AND end_date IS NOT NULL
    ),
    task_stats AS (
        SELECT 
            dag_id,
            task_id,
            COUNT(*) as total_executions,
            COUNT(*) FILTER (WHERE duration_seconds > 0) as valid_duration_count,
            COUNT(*) FILTER (WHERE state = 'failed') as failure_count,
            MIN(duration_seconds) as min_duration,
            AVG(duration_seconds) as avg_duration_seconds,
            MAX(duration_seconds) as max_duration_seconds,
            percentile_cont(0.95) WITHIN GROUP (ORDER BY duration_seconds) as p95_duration
        FROM base_durations
        GROUP BY dag_id, task_id
    )
    SELECT 
        dag_id,
        task_id,
        total_executions,
        valid_duration_count,
        ROUND(min_duration::numeric, 2) as min_duration_seconds,
        ROUND(avg_duration_seconds::numeric, 2) as avg_duration_seconds,
        ROUND(max_duration_seconds::numeric, 2) as max_duration_seconds,
        ROUND(p95_duration::numeric, 2) as p95_duration_seconds,
        failure_count,
        ROUND((failure_count::decimal / NULLIF(total_executions, 0) * 100), 2) as failure_rate,
        CASE 
            WHEN avg_duration_seconds >= 3600 THEN ROUND((avg_duration_seconds/3600)::numeric, 2) || ' hours'
            WHEN avg_duration_seconds >= 60 THEN ROUND((avg_duration_seconds/60)::numeric, 2) || ' minutes'
            ELSE ROUND(avg_duration_seconds::numeric, 2) || ' seconds'
        END as avg_duration_readable
    FROM task_stats
    WHERE valid_duration_count > 0  -- Show any tasks with valid durations
    ORDER BY avg_duration_seconds DESC
    LIMIT 1000;
    """,

"failed_tasks": f"""
    WITH task_failures AS (
        SELECT 
            ti.dag_id,
            ti.task_id,
            COUNT(*) as total_runs,
            COUNT(*) FILTER (WHERE ti.state = 'failed') as failures,
            MAX(ti.start_date) FILTER (WHERE ti.state = 'failed') as latest_failure,
            MIN(ti.start_date) FILTER (WHERE ti.state = 'failed') as first_failure
        FROM task_instance ti
        WHERE ti.start_date >= CURRENT_DATE - INTERVAL '{LOOKBACK_DAYS} day'
            AND ti.start_date < CURRENT_DATE + INTERVAL '1 day'
        GROUP BY ti.dag_id, ti.task_id
        HAVING COUNT(*) FILTER (WHERE ti.state = 'failed') > 0
    )
    SELECT 
        dag_id,
        task_id,
        total_runs,
        failures,
        ROUND((failures::decimal / NULLIF(total_runs, 0) * 100)::decimal, 2) as failure_rate,
        latest_failure,
        first_failure,
        CASE 
            WHEN failures = total_runs THEN 'CRITICAL'
            WHEN (failures::decimal / total_runs * 100) > 50 THEN 'HIGH'
            WHEN (failures::decimal / total_runs * 100) > 20 THEN 'MEDIUM'
            ELSE 'LOW'
        END as severity
    FROM task_failures
    ORDER BY failure_rate DESC, failures DESC
    LIMIT 1000;
""",

    "scheduler_health": """
    WITH scheduler_metrics AS (
        SELECT 
            COUNT(DISTINCT dr.dag_id) FILTER (
                WHERE dr.start_date >= NOW() - INTERVAL '1 hour'
            ) as dags_last_hour,
            COUNT(DISTINCT d.dag_id) FILTER (
                WHERE d.is_active = true
            ) as total_active_dags,
            MAX(dr.start_date) as last_dag_start,
            (
                SELECT COUNT(*) 
                FROM task_instance 
                WHERE state = 'running'
            ) as running_tasks,
            (
                SELECT COUNT(*) 
                FROM task_instance 
                WHERE state = 'queued'
            ) as queued_tasks,
            (
                SELECT COUNT(DISTINCT dag_id) 
                FROM dag 
                WHERE is_paused = false
            ) as enabled_dags
        FROM dag d
        LEFT JOIN dag_run dr ON d.dag_id = dr.dag_id
        WHERE dr.start_date >= NOW() - INTERVAL '1 day'
            OR dr.start_date IS NULL  -- Include DAGs with no runs
    )
    SELECT 
        dags_last_hour,
        total_active_dags,
        enabled_dags,
        last_dag_start,
        running_tasks,
        queued_tasks,
        NOW() - last_dag_start as time_since_last_run,
        CASE 
            WHEN NOW() - last_dag_start > INTERVAL '30 minutes' THEN 'CRITICAL'
            WHEN NOW() - last_dag_start > INTERVAL '15 minutes' THEN 'WARNING'
            ELSE 'HEALTHY'
        END as scheduler_status,
        CASE
            WHEN queued_tasks > 100 THEN 'HIGH_QUEUE_DEPTH'
            WHEN queued_tasks > 50 THEN 'MODERATE_QUEUE_DEPTH'
            ELSE 'NORMAL_QUEUE_DEPTH'
        END as queue_status
    FROM scheduler_metrics;
""",

    "resource_pool_usage": f"""
        WITH pool_metrics AS (
            SELECT 
                COALESCE(ti.pool, 'default_pool') as pool_name,
                COUNT(*) FILTER (WHERE ti.state = 'running') as running_tasks,
                COUNT(*) FILTER (WHERE ti.state = 'queued') as queued_tasks,
                SUM(ti.pool_slots) FILTER (WHERE ti.state = 'running') as used_slots,
                MAX(sp.slots) as total_slots
            FROM task_instance ti
            LEFT JOIN slot_pool sp ON ti.pool = sp.pool
            WHERE ti.start_date >= CURRENT_DATE - INTERVAL '{LOOKBACK_DAYS} day'
                AND ti.start_date < CURRENT_DATE + INTERVAL '1 day'
            GROUP BY ti.pool
        )
        SELECT 
            pool_name,
            running_tasks,
            queued_tasks,
            used_slots,
            total_slots,
            ROUND((used_slots::numeric / NULLIF(total_slots, 0) * 100), 2) as utilization_percent,
            CASE 
                WHEN queued_tasks > 0 AND used_slots >= COALESCE(total_slots, 0) THEN 'BOTTLENECK'
                WHEN used_slots >= COALESCE(total_slots, 0) * 0.8 THEN 'NEAR_CAPACITY'
                ELSE 'HEALTHY'
            END as pool_status
        FROM pool_metrics
        ORDER BY queued_tasks DESC, utilization_percent DESC;
    """,

    "database_metrics": """
        SELECT 
            schemaname,
            relname as table_name,
            n_live_tup as row_count,
            n_dead_tup as dead_tuples,
            ROUND(100.0 * n_dead_tup / NULLIF(n_live_tup + n_dead_tup, 0), 2) as dead_tuple_ratio,
            last_vacuum,
            last_analyze,
            last_autoanalyze,
            pg_size_pretty(pg_total_relation_size(relid)) as total_size,
            pg_size_pretty(pg_table_size(relid)) as table_size,
            pg_size_pretty(pg_indexes_size(relid)) as index_size,
            CASE 
                WHEN n_dead_tup > 10000 THEN 'Needs VACUUM'
                ELSE 'OK'
            END as maintenance_status
        FROM pg_stat_user_tables
        WHERE schemaname = 'public'
        ORDER BY n_live_tup DESC;
    """,

    "zombie_tasks": f"""
        WITH long_running AS (
            SELECT 
                ti.dag_id,
                ti.task_id,
                ti.start_date,
                ti.state,
                ti.hostname,
                ti.operator,
                EXTRACT(EPOCH FROM (NOW() - ti.start_date))/3600 as running_hours,
                AVG(EXTRACT(EPOCH FROM (th.end_date - th.start_date)))/3600 as avg_historical_hours
            FROM task_instance ti
            LEFT JOIN task_instance_history th ON 
                ti.dag_id = th.dag_id 
                AND ti.task_id = th.task_id
                AND th.state = 'success'
            WHERE ti.state = 'running'
                AND ti.start_date < NOW() - INTERVAL '1 hour'
            GROUP BY ti.dag_id, ti.task_id, ti.start_date, ti.state, ti.hostname, ti.operator
        )
        SELECT 
            dag_id,
            task_id,
            operator,
            start_date,
            hostname,
            ROUND(running_hours::numeric, 2) as running_hours,
            ROUND(avg_historical_hours::numeric, 2) as avg_historical_hours,
            CASE 
                WHEN running_hours > COALESCE(avg_historical_hours * 3, 24) THEN 'CRITICAL'
                WHEN running_hours > COALESCE(avg_historical_hours * 2, 12) THEN 'WARNING'
                ELSE 'MONITOR'
            END as status
        FROM long_running
        WHERE running_hours > 1
        ORDER BY running_hours DESC
        LIMIT 100;
    """,

    "dag_dependencies": f"""
        WITH RECURSIVE dag_deps AS (
            SELECT DISTINCT
                dag_id as source_dag_id,
                regexp_replace(task_id, '^external_task_sensor_', '') as target_dag_id,
                1 as depth
            FROM task_instance
            WHERE task_id LIKE 'external_task_sensor_%'
                AND start_date >= CURRENT_DATE - INTERVAL '{LOOKBACK_DAYS} day'
            
            UNION
            
            SELECT 
                d.source_dag_id,
                regexp_replace(ti.task_id, '^external_task_sensor_', '') as target_dag_id,
                d.depth + 1
            FROM dag_deps d
            JOIN task_instance ti ON d.target_dag_id = ti.dag_id
            WHERE d.depth < 3
                AND ti.task_id LIKE 'external_task_sensor_%'
        )
        SELECT 
            dd.source_dag_id,
            dd.target_dag_id,
            dd.depth,
            d1.is_paused as source_paused,
            d2.is_paused as target_paused,
            EXISTS (
                SELECT 1 FROM task_instance 
                WHERE dag_id = dd.source_dag_id 
                AND state = 'failed'
                AND start_date >= CURRENT_DATE - INTERVAL '1 day'
            ) as source_has_failures,
            EXISTS (
                SELECT 1 FROM task_instance 
                WHERE dag_id = dd.target_dag_id 
                AND state = 'failed'
                AND start_date >= CURRENT_DATE - INTERVAL '1 day'
            ) as target_has_failures
        FROM dag_deps dd
        JOIN dag d1 ON dd.source_dag_id = d1.dag_id
        JOIN dag d2 ON dd.target_dag_id = d2.dag_id
        ORDER BY depth, source_dag_id;
    """,

    "sla_analysis": f"""
        WITH sla_stats AS (
            SELECT 
                dag_id,
                task_id,
                COUNT(*) as total_misses,
                MIN(timestamp) as first_miss,
                MAX(timestamp) as latest_miss,
                STRING_AGG(DISTINCT COALESCE(description, 'No description'), ' | ') as sla_descriptions
            FROM sla_miss
            WHERE timestamp >= CURRENT_DATE - INTERVAL '{LOOKBACK_DAYS} day'
            GROUP BY dag_id, task_id
        )
        SELECT 
            s.*,
            EXTRACT(epoch FROM (latest_miss - first_miss)) / 3600 as hours_between_misses,
            d.is_paused,
            d.is_active,
            CASE 
                WHEN total_misses > 10 THEN 'CRITICAL'
                WHEN total_misses > 5 THEN 'WARNING'
                ELSE 'MONITOR'
            END as status
        FROM sla_stats s
        JOIN dag d USING (dag_id)
        WHERE total_misses > 1
        ORDER BY total_misses DESC, latest_miss DESC
        LIMIT 1000;
    """,

    "xcom_analysis": f"""
        WITH xcom_metrics AS (
            SELECT 
                dag_id,
                task_id,
                COUNT(*) as xcom_count,
                AVG(LENGTH(value)) as avg_size_bytes,
                MAX(LENGTH(value)) as max_size_bytes,
                SUM(LENGTH(value)) as total_size_bytes
            FROM xcom
            WHERE timestamp >= CURRENT_DATE - INTERVAL '{LOOKBACK_DAYS} day'
            GROUP BY dag_id, task_id
            HAVING MAX(LENGTH(value)) > 1000
        )
        SELECT 
            *,
            ROUND(total_size_bytes / 1024.0 / 1024.0, 2) as total_size_mb,
            CASE 
                WHEN max_size_bytes > 1024*1024 THEN 'LARGE'
                WHEN max_size_bytes > 100*1024 THEN 'MEDIUM'
                ELSE 'SMALL'
            END as size_category
        FROM xcom_metrics
        ORDER BY total_size_bytes DESC
        LIMIT 1000;
    """
}

default_args = {
    "owner": "auditing",
    "depends_on_past": False,
    "email": [""],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=30)
}

class MetricsTaskGroup:
    """Helper class to create a group of related metrics tasks"""
    
    def __init__(self, metric_name: str, sql: str, dag: models.DAG):
        self.metric_name = metric_name
        self.sql = sql
        self.dag = dag
        
    def create_export_task(self) -> PostgresToGCSOperator:
        """Creates a task to export data from Postgres to GCS"""
        return PostgresToGCSOperator(
            task_id=f"export_{self.metric_name}",
            postgres_conn_id=POSTGRES_CONNECTION_ID,
            sql=self.sql,
            bucket=BUCKET,
            filename=f"{GCS_OBJECT_PATH}/export_{self.metric_name}.{FILE_FORMAT}",
            export_format=FILE_FORMAT,
            field_delimiter=",",
            gzip=False,
            use_server_side_cursor=False,
            execution_timeout=timedelta(minutes=15),
            dag=self.dag,
        )
    
    def create_check_task(self) -> GCSObjectsWithPrefixExistenceSensor:
        """Creates a task to check if the export file exists in GCS"""
        return GCSObjectsWithPrefixExistenceSensor(
            task_id=f"export_{self.metric_name}_check",
            bucket=BUCKET,
            prefix=f"{GCS_OBJECT_PATH}/export_{self.metric_name}.{FILE_FORMAT}",
            mode="poke",
            poke_interval=10,
            timeout=60,
            dag=self.dag,
        )
    
    def create_report_task(self) -> GenerativeModelGenerateContentOperator:
        """Creates a task to generate a report using Vertex AI"""
        return GenerativeModelGenerateContentOperator(
            task_id=f"{self.metric_name}_report",
            project_id=PROJECT_ID,
            location=LOCATION,
            contents=[
                """
As an Apache Airflow Metadata DB expert, analyze the provided PostgreSQL CSV data to generate a concise status report (100 words max). Your analysis should:

Examine key Airflow metadata metrics from the CSV
Identify performance issues or anomalies in the data
Highlight significant trends or patterns
Provide actionable recommendations for any issues found

Format your response in clear paragraphs, prioritizing critical findings first. Reference specific metrics from the CSV data to support your conclusions.
                """,
                Part.from_uri(
                    uri=f"gs://{BUCKET}/{GCS_OBJECT_PATH}/export_{self.metric_name}.{FILE_FORMAT}",
                    mime_type="text/csv"
                )
            ],
            pretrained_model=MODEL,
            dag=self.dag,
        )
    
    def create_task_group(self) -> List[models.BaseOperator]:
        """Creates and links all tasks in the group"""
        export_task = self.create_export_task()
        check_task = self.create_check_task()
        report_task = self.create_report_task()
        
        export_task >> check_task >> report_task
        
        return [export_task, check_task, report_task]

with models.DAG(
    f"optimized_gemini_insights_report_dag_v2",
    tags=["airflow_db"],
    description="Export contents of airflow metadata DB to GCS location and analyze with Gemini.",
    is_paused_upon_creation=True,
    catchup=False,
    start_date=datetime(2024, 1, 1),
    max_active_runs=1,
    default_args=default_args,
    schedule_interval="@once",  # change to daily
) as dag:
    
    # Create task groups for each metric
    task_groups = {
        metric_name: MetricsTaskGroup(metric_name, sql, dag).create_task_group()
        for metric_name, sql in QUERIES.items()
    }
    
    # Create final report task
    final_report = GenerativeModelGenerateContentOperator(
        task_id="final_report",
        project_id=PROJECT_ID,
        location=LOCATION,
        contents=[
            """
You are a Google Cloud Composer and Airflow Metadata Database expert.

Review the provided analysis of database queries covering: dag_runs, task_durations, scheduler_health, resource_pool_usage, database_metrics, zombie_tasks, dag_dependencies, sla analysis, and xcom analysis.

Write a daily status report as plain text with clear paragraph breaks. Do not use any markdown, formatting characters, bullet points, or special symbols.

Start your analysis with Resource Utilization by describing resource pool usage patterns and efficiency, CPU, memory, and database utilization trends, task duration and scheduling behavior, and database performance metrics.

Continue with Workflow Health analysis by describing DAG and task instance execution status, scheduler health and performance indicators, SLA compliance and performance, presence and impact of zombie tasks, DAG dependency execution patterns, and XCom usage and efficiency.

If you identify significant optimization opportunities, describe specific recommendations for Airflow configuration improvements, Composer infrastructure adjustments, cost optimization opportunities, and resource allocation refinements.

Present your complete analysis in clear paragraphs with natural breaks between topics. Your response should be suitable for SRE and Data Engineering teams, covering both healthy operational metrics and areas needing attention. Include recommendations only when meaningful improvements are possible.
            """
        ] + [
            f"{{{{ ti.xcom_pull(task_ids='{metric_name}_report') }}}}"
            for metric_name in QUERIES.keys()
        ],
        pretrained_model=MODEL,
        trigger_rule="all_done",
        dag=dag,
    )
    
    # Set dependencies for final report
    for task_group in task_groups.values():
        task_group[-1] >> final_report  # Link each report task to final report