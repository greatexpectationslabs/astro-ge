# A sample DAG to show some functionality of the GE operator. Steps to run:
#
# 1. You'll first need to install this operator:
# `pip install great_expectations_airflow`
#
# 2. Make sure airflow is installed and your dags_folder is configured to point to this directory.
#
# 3. When running a checkpoint task, change the path to the data directory in great_expectations/checkpoint/*.yml
#
# 4. You can then test-run a single task in this DAG using:
# Airflow v1.x: `airflow test example_great_expectations_dag ge_batch_kwargs_pass 2020-01-01`
# Airflow v2.x: `airflow tasks test example_great_expectations_dag ge_batch_kwargs_pass 2020-01-01`
#
# Note: The tasks that don't set an explicit data_context_root_dir need to be run from within
# this examples directory, otherwise GE won't know where to find the data context.

import os
import airflow
from airflow import DAG
from great_expectations_provider.operators.great_expectations import GreatExpectationsOperator
from great_expectations.data_context.types.base import DataContextConfig, DatasourceConfig, S3StoreBackendDefaults, \
    FilesystemStoreBackendDefaults, LegacyDatasourceConfig
from great_expectations.data_context import BaseDataContext

default_args = {
    "owner": "Airflow",
    "start_date": airflow.utils.dates.days_ago(1)
}

dag = DAG(
    dag_id='example_great_expectations_dag',
    default_args=default_args
)

# This runs an expectation suite against a data asset that passes the tests

data_dir = '/usr/local/airflow/include/data/'
data_file = '/usr/local/airflow/include/data/yellow_tripdata_sample_2019-01.csv'
# We could set this with environment variables
ge_root_dir = '/usr/local/airflow/include/great_expectations'

data_dir_local = '/Users/sam/code/astro-ge/include/data'
data_file_local = '/Users/sam/code/astro-ge/include/data/yellow_tripdata_sample_2019-01.csv'
ge_root_dir_local = '/Users/sam/code/astro-ge/include/great_expectations'

# ge_batch_kwargs_pass = GreatExpectationsOperator(
#     task_id='ge_batch_kwargs_pass',
#     expectation_suite_name='taxi.demo',
#     batch_kwargs={
#         'path': data_file,
#         'datasource': 'data__dir'
#     },
#     data_context_root_dir=ge_root_dir,
#     # dag=dag
# )
#
# # This runs an expectation suite against a data asset that passes the tests
# ge_batch_kwargs_list_pass = GreatExpectationsOperator(
#     task_id='ge_batch_kwargs_list_pass',
#     assets_to_validate=[
#         {
#             'batch_kwargs': {
#                 'path': data_file,
#                 'datasource': 'data__dir'
#             },
#             'expectation_suite_name': 'taxi.demo'
#         }
#     ],
#     data_context_root_dir=ge_root_dir,
#     # dag=dag
# )
#
# # This runs a checkpoint that will pass. Make sure the checkpoint yml file has the correct path to the data file.
# ge_checkpoint_pass = GreatExpectationsOperator(
#     task_id='ge_checkpoint_pass',
#     run_name='ge_airflow_run',
#     checkpoint_name='taxi.pass.chk',
#     data_context_root_dir=ge_root_dir,
#     # dag=dag
# )
#
# # This runs a checkpoint that will fail, but we set a flag to exit the task successfully.
# # Make sure the checkpoint yml file has the correct path to the data file.
# ge_checkpoint_fail_but_continue = GreatExpectationsOperator(
#     task_id='ge_checkpoint_fail_but_continue',
#     run_name='ge_airflow_run',
#     checkpoint_name='taxi.fail.chk',
#     fail_task_on_validation_failure=False,
#     data_context_root_dir=ge_root_dir,
#     # dag=dag
# )
#
# # This runs a checkpoint that will fail. Make sure the checkpoint yml file has the correct path to the data file.
# ge_checkpoint_fail = GreatExpectationsOperator(
#     task_id='ge_checkpoint_fail',
#     run_name='ge_airflow_run',
#     checkpoint_name='taxi.fail.chk',
#     data_context_root_dir=ge_root_dir,
#     fail_task_on_validation_failure=True,
#     # dag=dag
# )

data_context_config_local = DataContextConfig(
    datasources={
        "data__dir": LegacyDatasourceConfig(
            class_name="PandasDatasource",
            batch_kwargs_generators={
                "subdir_reader": {
                    "class_name": "SubdirReaderBatchKwargsGenerator",
                    "base_directory": data_dir_local,
                }
            },
        )
    },
    # store_backend_defaults=S3StoreBackendDefaults(default_bucket_name="sam-webinar-demo")
    store_backend_defaults=FilesystemStoreBackendDefaults(root_directory=ge_root_dir_local)
)
data_context_local = BaseDataContext(project_config=data_context_config_local, context_root_dir=ge_root_dir_local)

# This task uses the in-code data context to load data f
ge_in_code_context_local = GreatExpectationsOperator(
    task_id='ge_in_code_context_local',
    # expectation_suite_name='taxi.demo',
    # batch_kwargs={
    #     'path': data_file_local,
    #     'datasource': 'my_datasource'
    # },
    checkpoint_name="taxi.pass.chk",
    data_context=data_context_local,
    dag=dag
)

# ge_batch_kwargs_pass >> ge_batch_kwargs_list_pass >> ge_checkpoint_pass >> ge_checkpoint_fail_but_continue >> \
# ge_in_code_context >> ge_checkpoint_fail

# ge_batch_kwargs_pass >> ge_in_code_context

# data_context_config_s3 = DataContextConfig(
#     datasources={
#         "data__dir": LegacyDatasourceConfig(
#             class_name="PandasDatasource",
#             batch_kwargs_generators={
#                 "subdir_reader": {
#                     "class_name": "SubdirReaderBatchKwargsGenerator",
#                     "base_directory": data_dir_local,
#                 }
#             },
#         )
#     },
#     store_backend_defaults=S3StoreBackendDefaults(default_bucket_name="sam-webinar-demo")
# )
# data_context_s3 = BaseDataContext(project_config=data_context_config_s3)
#
# # This task uses the in-code data context to load data f
# ge_in_code_context_s3 = GreatExpectationsOperator(
#     task_id='ge_in_code_context_s3',
#     expectation_suite_name='taxi.demo',
#     batch_kwargs={
#         'path': data_file_local,
#         'datasource': 'my_datasource'
#     },
#     data_context=data_context_s3,
#     dag=dag
# )
#


# data_context_config = DataContextConfig(
#     datasources={
#         "data__dir": LegacyDatasourceConfig(
#             class_name="PandasDatasource",
#             batch_kwargs_generators={
#                 "subdir_reader": {
#                     "class_name": "SubdirReaderBatchKwargsGenerator",
#                     "base_directory": data_dir,
#                 }
#             },
#         )
#     },
#     store_backend_defaults=FilesystemStoreBackendDefaults(root_directory=ge_root_dir)
# )
# data_context = BaseDataContext(project_config=data_context_config)
#
# ge_in_code_context = GreatExpectationsOperator(
#     task_id='ge_in_code_context',
#     expectation_suite_name='taxi.demo',
#     batch_kwargs={
#         'path': data_file,
#         'datasource': 'my_datasource'
#     },
#     data_context=data_context,
#     dag=dag
# )
