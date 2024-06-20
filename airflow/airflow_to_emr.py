from datetime import datetime, timedelta
from airflow import DAG
import boto3
import json
# from airflow.providers.amazon.aws.operators.lambda_function import AwsLambdaInvokeFunctionOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.operators.emr import EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.operators.emr import EmrTerminateJobFlowOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator

# Function to invoke Lambda
def invoke_lambda_function(**kwargs):
    lambda_client = boto3.client('lambda')
    response = lambda_client.invoke(
        FunctionName='mock-web-logs',
        InvocationType='Event',  # or 'RequestResponse' if you need a response
    )
    print(response)

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'emr_to_redshift_dag',
    default_args=default_args,
    description='Run Spark job on EMR and load data from S3 to Redshift',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:
    
    # # Task to invoke the Lambda function
    # invoke_lambda = AwsLambdaInvokeFunctionOperator(
    #     task_id='invoke_lambda',
    #     function_name='mock-web-logs',  # Change to your Lambda function name
    #     invocation_type='Event',  # Change to 'Event' if you don't want to wait for response
    #     aws_conn_id='aws_default',  # Ensure this AWS connection ID exists in Airflow
    # )

    invoke_lambda = PythonOperator(
        task_id='invoke_lambda_function',
        python_callable=invoke_lambda_function,
        provide_context=True,
    )

    # Sensor to wait for the new file in S3
    wait_for_new_file = S3KeySensor(
        task_id='wait_for_new_file',
        bucket_name='web-logs-bucket-monthly',  # Change to your S3 bucket name
        bucket_key='/',  # Change to the S3 path where new files will be uploaded
        aws_conn_id='aws_default',  # Ensure this AWS connection ID exists in Airflow
        timeout=18*60*60,  # 18 hours
        poke_interval=60,  # 1 minute
        mode='poke'
    )

    # Define the EMR cluster configuration
    JOB_FLOW_OVERRIDES = {
        'Name': 'EMR-Spark-weblogs-cluster',
        'ReleaseLabel': 'emr-6.3.0',
        'Applications': [{'Name': 'Spark'}],
        'Instances': {
            'InstanceGroups': [
                {
                    'Name': 'Master nodes',
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'MASTER',
                    'InstanceType': 'm5.xlarge',
                    'InstanceCount': 1,
                },
                {
                    'Name': 'Core nodes',
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'CORE',
                    'InstanceType': 'm5.xlarge',
                    'InstanceCount': 2,
                },
            ],
            'KeepJobFlowAliveWhenNoSteps': False,
            'TerminationProtected': False,
        },
        'JobFlowRole': 'EMR_EC2_DefaultRole',
        'ServiceRole': 'EMR_DefaultRole',
    }

    # Create an EMR cluster
    create_emr_cluster = EmrCreateJobFlowOperator(
        task_id='create_emr_cluster',
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        aws_conn_id='aws_default',
    )

    # Define the Spark step to process data
    SPARK_STEPS = [
        {
            'Name': 'run_spark_job',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    'spark-submit',
                    '--deploy-mode', 'cluster',
                    's3://airflow-weblogs-spark-bucket/s3_input_s3_output.py',  # Path to your Spark script
                ],
            },
        },
    ]

    # Add the Spark step to the EMR cluster
    add_spark_step = EmrAddStepsOperator(
        task_id='add_spark_step',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        steps=SPARK_STEPS,
        aws_conn_id='aws_default',
    )

    # Wait for the Spark step to complete
    wait_for_spark_step = EmrStepSensor(
        task_id='wait_for_spark_step',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='add_spark_step', key='return_value')[0] }}",
        aws_conn_id='aws_default',
    )

    # Terminate the EMR cluster
    terminate_emr_cluster = EmrTerminateJobFlowOperator(
        task_id='terminate_emr_cluster',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        aws_conn_id='aws_default',
    )

    # Load processed data from S3 to Redshift
    s3_to_redshift = S3ToRedshiftOperator(
        task_id='s3_to_redshift',
        schema='logs_analysis',
        table='weblogs_monthly',
        s3_bucket='web-logs-processed-bucket',
        s3_key='/',  # Prefix or file processed by Spark
        copy_options=['csv'],
        aws_conn_id='aws_default',
        redshift_conn_id='redshift_default',
    )

    # Define task dependencies
    invoke_lambda >> wait_for_new_file >> create_emr_cluster >> add_spark_step >> wait_for_spark_step >> terminate_emr_cluster >> s3_to_redshift
