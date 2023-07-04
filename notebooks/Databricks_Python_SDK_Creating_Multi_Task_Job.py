# Databricks notebook source
# MAGIC %md
# MAGIC # How to create multi task jobs using the Databricks Pyhton SDK
# MAGIC Creating and maintaning jobs programatically is interesting when working with multiple environments, building up disaster recovery structures and even versioning your Databricks workflows.
# MAGIC
# MAGIC Using the Databricks SDK, we can create multi-task jobs without having to worry about the underlying Databricks APIs and their payload structures, leveraging the pre-built assets packed within the SDK.
# MAGIC
# MAGIC ####Limitations:
# MAGIC - This example does not implement job updates
# MAGIC - Assumes unique job names in your source workspace
# MAGIC - Tested with `databricks-sdk==0.1.12` and Databricks Runtime 13.1

# COMMAND ----------

# MAGIC %md
# MAGIC ###1. Installing the Databricks Python SDK

# COMMAND ----------

# MAGIC %sh
# MAGIC pip install databricks-sdk==0.1.12 --quiet --disable-pip-version-check

# COMMAND ----------

# MAGIC %pip show databricks-sdk

# COMMAND ----------

# MAGIC %md
# MAGIC ###2. Instantiating the Databricks Workspace Client

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import *
from databricks.sdk.service.compute import *
from typing import List

w = WorkspaceClient()
for c in w.clusters.list()[0:1]:
    print(c.cluster_name)
    print("The authentication was successfull")

# COMMAND ----------

# MAGIC %md
# MAGIC Checking the cloud we are running at

# COMMAND ----------

host = dbutils.notebook.entry_point.getDbutils().notebook().getContext().browserHostName().get()
if "azuredatabricks.net" in host:
    cloud = "AZURE"
elif "gcp.databricks.com" in host:
    cloud = "GCP"
else:
    cloud = "AWS"

# COMMAND ----------

# MAGIC %md
# MAGIC Creating widgets to define the job name and description

# COMMAND ----------

dbutils.widgets.text("job_name", "my_job", "01 - Job name")
dbutils.widgets.text("job_description", "Example for creating multi task jobs using the Databricks Python SDK", "02 - Job description")

# COMMAND ----------

# MAGIC %md
# MAGIC Defining the base path for our tasks

# COMMAND ----------

tasks_path = f"""{"/".join(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().split("/")[0:-1])}"""
print(tasks_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ###3. Defining the tasks and their dependencies

# COMMAND ----------

job_tasks = [
    Task(
        task_key="My_first_task",
        depends_on=None,
        description="My first task in this very simple example",
        email_notifications=None,
        job_cluster_key="Job_cluster",
        libraries=None,
        max_retries=None,
        min_retry_interval_millis=None,
        notebook_task=NotebookTask(
            notebook_path=f"{tasks_path}/Task_1",
            base_parameters=None,
            source=Source("WORKSPACE"),
        ),
        notification_settings=TaskNotificationSettings(
            alert_on_last_attempt=False,
            no_alert_for_canceled_runs=False,
            no_alert_for_skipped_runs=False,
        ),
        retry_on_timeout=None,
        timeout_seconds=0,
    ),
    Task(
        task_key="My_second_task",
        depends_on=[TaskDependency(task_key="My_first_task")],
        description="My second task in this very simple example",
        email_notifications=None,
        job_cluster_key="Job_cluster",
        libraries=None,
        max_retries=1,
        min_retry_interval_millis=60000,
        notebook_task=NotebookTask(
            notebook_path=f"{tasks_path}/Task_2",
            base_parameters={"my_parameter": "true"},
            source=Source("WORKSPACE"),
        ),
        notification_settings=TaskNotificationSettings(
            alert_on_last_attempt=False,
            no_alert_for_canceled_runs=False,
            no_alert_for_skipped_runs=False,
        ),
        retry_on_timeout=False,
        timeout_seconds=600,
    ),
    Task(
        task_key="My_third_task",
        depends_on=[TaskDependency(task_key="My_second_task")],
        description="My third task in this very simple example",
        email_notifications=None,
        existing_cluster_id=None,
        job_cluster_key="Job_cluster",
        libraries=None,
        max_retries=None,
        min_retry_interval_millis=None,
        notebook_task=NotebookTask(
            notebook_path=f"{tasks_path}/Task_3",
            base_parameters=None,
            source=Source("WORKSPACE"),
        ),
        notification_settings=TaskNotificationSettings(
            alert_on_last_attempt=False,
            no_alert_for_canceled_runs=False,
            no_alert_for_skipped_runs=False,
        ),
        retry_on_timeout=None,
        timeout_seconds=0,
    ),
]

# COMMAND ----------

# MAGIC %md
# MAGIC Choosing the smallest node type for each cloud

# COMMAND ----------

smallest = w.clusters.select_node_type(local_disk=True)
print(f"Using the smallest node available: {smallest}")

# COMMAND ----------

# MAGIC %md
# MAGIC Defining cloud specific availability parameters

# COMMAND ----------

cloud_attributes = {
    "aws_attributes": AwsAttributes(
        availability=AwsAvailability("SPOT_WITH_FALLBACK"), first_on_demand=1, spot_bid_price_percent=100
    ),
    "azure_attributes": AzureAttributes(
        availability=AzureAvailability("SPOT_WITH_FALLBACK_AZURE"),
        first_on_demand=1,
        spot_bid_max_price=-1.0,
    ),
    "gcp_attributes": GcpAttributes(
        availability=GcpAvailability("PREEMPTIBLE_WITH_FALLBACK_GCP"),
    ),
}
cloud = cloud.upper()

# COMMAND ----------

# MAGIC %md
# MAGIC ###4.Defining the shared cluster specifications for the job
# MAGIC We can leverage cluster policies to validate that the parameters are within the boundaries of a specific cluster policy, but, as of 2023-07-04, there's no way to `apply_policy_defaults` to the attributes we do not specify when creating a job cluster. There is a roadmap item to allow for the policy defaults to be used in this use case.

# COMMAND ----------

basic_cluster_job = ClusterSpec(
    driver_node_type_id=smallest,
    node_type_id=smallest,
    num_workers=0,
    enable_elastic_disk=False,
    autoscale=None,
    autotermination_minutes=None,
    aws_attributes= cloud_attributes["aws_attributes"] if cloud == "AWS" else None,
    azure_attributes= cloud_attributes["azure_attributes"] if cloud == "AZURE" else None,
    gcp_attributes= cloud_attributes["gcp_attributes"] if cloud == "GCP" else None,
    custom_tags={"ResourceClass": "SingleNode", "Cost_center": "ABC0213", "Project": "My super duper example", "Team": "Marketing"},
    data_security_mode=DataSecurityMode("SINGLE_USER"),
    driver_instance_pool_id=None,
    init_scripts=None,
    instance_pool_id=None,
    policy_id=None,
    runtime_engine=RuntimeEngine("STANDARD"),
    single_user_name=None,
    spark_conf={
        "spark.databricks.delta.preview.enabled": "true",
        "spark.master": "local[*, 4]",
        "spark.databricks.cluster.profile": "singleNode",
    },
    spark_env_vars=None,
    spark_version="13.1.x-scala2.12",
    ssh_public_keys=None,
    workload_type=None,
)

# COMMAND ----------

# MAGIC %md
# MAGIC Checking if the job exists (assumes unique naming for jobs in the workspace)

# COMMAND ----------

def check_if_job_exists(job_name: str, w: WorkspaceClient) -> dict:
    jobs = w.jobs.list()
    job_id = ""
    while True:
        try:
            current_job = next(jobs)
            current_job_id = current_job.job_id
            current_job_name = current_job.as_dict()["settings"]["name"]
            if current_job_name == job_name:
                return {"job_id": current_job_id, "current_job_name": current_job_name}
        except StopIteration:
            return {}


def create_job(job_name: str, w: WorkspaceClient) -> str:
    my_job = check_if_job_exists(job_name = job_name, w = w)
    if my_job.get("job_id") is None:
        j = w.jobs.create(
            name = job_name,
            job_clusters=[JobCluster(job_cluster_key="Job_cluster", new_cluster=basic_cluster_job)],
            tasks = job_tasks,
            trigger=None
        )
        print(f"Created the job at {w.config.host}/#job/{j.job_id}\n")
        return j.job_id
    else:
        print(f"""Job Exists at: {w.config.host}/#job/{my_job["job_id"]}\nJob update is not implemented in this example, but you can find examples here: https://github.com/databricks/databricks-sdk-py/blob/main/examples/jobs/update_jobs_api_full_integration.py""")
        return my_job["job_id"]

# COMMAND ----------

# MAGIC %md
# MAGIC Fetching the values for our widgets

# COMMAND ----------

job_name                = dbutils.widgets.get("job_name")
job_description         = dbutils.widgets.get("job_description")

# COMMAND ----------

# MAGIC %md
# MAGIC ###5.Creating or updating the job
# MAGIC In this step, we create the job if it does not exist, or we could update it, if it did. The update part is not implemented as part of this example, [but can be found here](https://github.com/databricks/databricks-sdk-py/blob/main/examples/jobs/update_jobs_api_full_integration.py)

# COMMAND ----------

my_job = create_job(job_name=job_name, w = w)

# COMMAND ----------

help(w.jobs.update)
