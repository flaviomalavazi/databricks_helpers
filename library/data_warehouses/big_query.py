# Databricks notebook source
# COMMAND ----------

import base64
import json
import re

from py4j.protocol import Py4JJavaError
from pyspark.sql import DataFrame


# Validating each parameter for the BigQuery Connection
class BigQuery:
    class color:
        BOLD = "\033[1m"
        END = "\033[0m"

    def __init__(self, project_options: dict, **kwargs) -> bool:
        self.project_options = project_options
        self.project_id = None
        self.valid = False
        self.dataset = kwargs.get("dataset", None)
        self.temp_schema = kwargs.get("temp_schema", None)
        self.valid_project_id = self.validate_project_id()
        self.valid_email = self.validate_email()
        self.valid_private_key_id = self.validate_private_key_id()
        self.valid_private_key = self.validate_private_key()
        self.valid_credentials = self.validate_credentials()
        self.valid_enablement_flag = self.validate_google_service_account_enable()
        self.can_read_bq = self.can_read_bq()
        self.success()

    def can_read_bq(self) -> bool:
        can_read_bq = False
        if self.project_options["parentProject"] != self.project_id:
            raise AssertionError(
                "The parentProject informed for reading big query is not the same as the"
                " one provided for authentication purposes, please check!"
            )
        else:
            self.big_query_project = project_options["project"]
            can_read_bq = True
        return can_read_bq

    def validate_regex(
        self,
        validation_parameter: str = None,
        validation_string: str = None,
        regex: str = None,
    ) -> bool:
        if re.fullmatch(regex, validation_string):
            return True
        else:
            print(
                self.color.BOLD
                + f"Please verify the {validation_parameter} parameter. The one provided"
                " is invalid!"
                + self.color.END
            )
            return False

    def check_existence(
        self, validation_parameter: str = None, validation_message: str = None
    ) -> str:
        try:
            current_value = spark.conf.get(validation_parameter)
            return current_value
        except Py4JJavaError as e:
            if f"java.util.NoSuchElementException: {validation_parameter}" in str(e):
                print(
                    self.color.BOLD
                    + f"""The {validation_parameter} was not defined in the cluster. Please go back to the cluster """
                    """configuration to set the parameter or set it using the """
                    """spark.conf.set("parameter_name", "parameter_value") syntax if you want to use it for this """
                    """session only""" + self.color.END
                )
                raise KeyError(
                    f"The parameter {validation_parameter} was not setup correctly!"
                )

    def validate_email(self) -> bool:
        # Make a regular expression
        # for validating an Email
        # Sufix specific email for service account: @project-id.iam.gserviceaccount.com
        valid_email_sufix = False
        email_regex = r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b"
        email_parameter = "spark.hadoop.fs.gs.auth.service.account.email"
        email_validation_message = "service account email"
        email = self.check_existence(
            validation_parameter=email_parameter,
            validation_message=email_validation_message,
        )
        regex_ok = self.validate_regex(
            validation_parameter=email_parameter,
            validation_string=email,
            regex=email_regex,
        )
        try:
            if f"{self.project_id}.iam.gserviceaccount.com" == email.split("@")[
                1
            ] and email.endswith("iam.gserviceaccount.com"):
                valid_email_sufix = True
            elif not (email.endswith("iam.gserviceaccount.com")):
                error_message = "the email provided is not from a service account."
            elif f"{self.project_id}.iam.gserviceaccount.com" != email.split("@")[1]:
                error_message = (
                    "the service account email is not in the same project as the one from"
                    " the project_options dictionary."
                )
            else:
                error_message = (
                    "the service account email is invalid! Can't determine the specific"
                    " error at this time."
                )
        except Exception as e:
            raise AssertionError(
                "The project_id for the service account is not the same as the"
                " project_options.project value, authentication will fail. Error"
                f" message: {str(e)}"
            )
        if regex_ok and valid_email_sufix:
            print(
                self.color.BOLD
                + "Service account email"
                + self.color.END
                + " seems to be ok!"
            )
            return True
        elif regex_ok and not (valid_email_sufix):
            raise AssertionError(
                "The service account email is a valid formatted email address but"
                f" {error_message}"
            )
        else:
            raise AssertionError(
                "The service account email provided to the Databricks cluster is not a"
                f" valid email format. {error_message}"
            )

    def validate_google_service_account_enable(self) -> bool:
        google_service_account_enablement_parameter = (
            "spark.hadoop.google.cloud.auth.service.account.enable"
        )
        google_service_account_enablement_validation_message = (
            "google cloud auth service account enable"
        )
        google_service_account_enablement = self.check_existence(
            validation_parameter=google_service_account_enablement_parameter,
            validation_message=google_service_account_enablement_validation_message,
        )
        if google_service_account_enablement == "true":
            print(
                self.color.BOLD
                + "Flag Google Cloud Auth Service Account"
                + self.color.END
                + " is ok!"
            )
            return True
        elif google_service_account_enablement == "false":
            print(
                self.color.BOLD
                + "Flag Google Cloud Auth Service Account"
                + self.color.END
                + " is set to false"
            )
            raise AssertionError(
                "The flag is currently set to `false`, the Databricks cluster requires it"
                " to be set to true"
            )
        else:
            raise AssertionError(
                "Google Cloud Auth Service Account Enablement Flag not provided"
            )

    def validate_project_id(self, project_id_for_query: str = None) -> bool:
        project_id_regex = (  # "^[a-z][a-z0-9-]{4,28}[a-z0-9]$"
            r"^[a-z][a-z0-9-]{4,28}[a-z0-9]$"
        )
        project_id_parameter = "spark.hadoop.fs.gs.project.id"
        project_id_validation_message = "project id"
        project_id = self.check_existence(
            validation_parameter=project_id_parameter,
            validation_message=project_id_validation_message,
        )
        if self.validate_regex(
            validation_parameter=project_id_parameter,
            validation_string=project_id,
            regex=project_id_regex,
        ):
            print(self.color.BOLD + "Project ID" + self.color.END + " seems to be ok!")
            self.project_id = project_id
            return True
        else:
            raise AssertionError(
                "Invalid Project ID provided! The project ID must be 6 to 30 characters"
                " in length, contain only lowercase letters, numbers and hyphens, start"
                " with a letter and not end with a hyphen."
            )

    def validate_private_key_id(self) -> bool:
        private_key_id_regex = r"^[a-zA-Z0-9]{40}$"
        private_key_id_parameter = (
            "spark.hadoop.fs.gs.auth.service.account.private.key.id"
        )
        private_key_id_validation_message = "Private key ID"
        private_key_id = self.check_existence(
            validation_parameter=private_key_id_parameter,
            validation_message=private_key_id_validation_message,
        )
        if self.validate_regex(
            validation_parameter=private_key_id_parameter,
            validation_string=private_key_id,
            regex=private_key_id_regex,
        ):
            print(
                self.color.BOLD + "Private key ID" + self.color.END + " seems to be ok!"
            )
            return True
        else:
            raise AssertionError(
                "Invalid private key ID provided! The key ID should be a 40 char"
                " alphanumeric string with no special characters"
            )

    def validate_private_key(self) -> bool:
        private_key_start = "-----BEGIN PRIVATE \nKEY-----"
        private_key_end = "-----END \nPRIVATE KEY-----\\n"
        private_key_parameter = "spark.hadoop.fs.gs.auth.service.account.private.key"
        private_key_validation_message = "private key"
        private_key = self.check_existence(
            validation_parameter=private_key_parameter,
            validation_message=private_key_validation_message,
        )
        if private_key.startswith(private_key_start) and private_key.endswith(
            private_key_end
        ):
            print(self.color.BOLD + "Private key" + self.color.END + " seems to be ok!")
            return True
        else:
            raise AssertionError(
                r"""Invalid private key provided! It should start with -----BEGIN PRIVATE KEY-----"""
                r""" and end with -----END PRIVATE KEY-----\n"""
            )

    def validate_credentials(self) -> bool:
        credentials_regex = r"^(?:[A-Za-z0-9+\/]{4})*(?:[A-Za-z0-9+\/]{4}|[A-Za-z0-9+\/]{3}=|[A-Za-z0-9+\/]{2}={2})$"
        credentials_parameter = "credentials"
        credentials_validation_message = "credentials"
        credentials = self.check_existence(
            validation_parameter=credentials_parameter,
            validation_message=credentials_validation_message,
        )
        if (
            self.validate_regex(
                validation_parameter=credentials_parameter,
                validation_string=credentials,
                regex=credentials_regex,
            )
            and credentials != ""
        ):
            print(self.color.BOLD + "Credentials" + self.color.END + " seem to be ok!")
            return True
        elif credentials == "":
            AssertionError(
                "Blank credentials provided! The parameter needs to be set to the base64"
                " encoding of the credentials JSON (it's a very long string)"
            )
        else:
            raise AssertionError("Invalid credentials provided!")

    def success(self) -> bool:

        if (
            self.valid_project_id
            and self.valid_email
            and self.valid_private_key_id
            and self.valid_private_key
            and self.valid_credentials
            and self.valid_enablement_flag
            and self.can_read_bq
        ):
            print(
                "\n\nThe Databricks Cluster parameters to access the BigQuery Warehouse"
                " seem Ok"
            )
            self.valid = True
        else:
            print("You have an error in our BigQuery Parameters, please check")

    def read_table_by_id(self, table_id: str) -> DataFrame:
        if self.valid:
            try:
                return (
                    spark.read.format("bigquery")
                    .option("table", table_id)  # Lendo uma tabela completa
                    .options(**self.project_options)
                    .load()
                )
            except Py4JJavaError as e:
                error_message = str(e.java_exception.getMessage())
                print(self.color.BOLD + "BigQuery: " + self.color.END + error_message)
                if f"Table {table_id} not found" in error_message:
                    raise KeyError(f"{table_id} does not exist!")
                elif "Unable to provision, see the following errors" in error_message:
                    if "Invalid Table ID" in error_message:
                        raise AssertionError(
                            f"BigQuery: the table ID: {table_id} was provided in the"
                            " wrong format, it must match the following regular"
                            " expression: '^(((\S+)[:.])?(\w+)\.)?([\S&&[^.:]]+)$$'"
                        )
                elif (
                    f"""Access Denied: Table {table_id.split(".")[0]}:{table_id.split(".")[1]}."""
                    f"""{table_id.split(".")[2]}: Permission bigquery.tables.get denied on table"""
                    f""" {table_id.split(".")[0]}:{table_id.split(".")[1]}.{table_id.split(".")[2]} """
                    """ (or it may not exist).""" in str(error_message)
                ):
                    raise Exception(error_message)
                else:
                    print(e)
        else:
            raise AssertionError(
                "Can not read from Big Query, parameter validation failed"
            )

    def read_query(self, query: str, temp_schema: str) -> DataFrame:
        if self.valid:
            try:
                return (
                    spark.read.format("bigquery")
                    .option("materializationDataset", temp_schema)
                    .option("query", query)
                    .options(**self.project_options)
                    .load()
                )
            except Py4JJavaError as e:
                error_message = str(e.java_exception.getMessage())
                print(self.color.BOLD + "BigQuery: " + self.color.END + error_message)
                if "Not found: Table " in error_message:
                    print(
                        """Please verify if the tables in the query exist and are on the same region """
                        f"""as the and the temp_schema dataset ({temp_schema})"""
                    )
                elif "was not found in location" in error_message:
                    print(
                        "Please verify if"
                        + self.color.BOLD
                        + f""" the datasets used by the query and the temp_schema dataset ({temp_schema})"""
                        + self.color.END
                        + " provided are in the same region (and BigQuery project)"
                    )
                elif (
                    "Access Denied: Dataset" in error_message
                    and "bigquery.tables.create" in error_message
                ):
                    print(
                        "The service account user does not have create table access on"
                        " the provided temp dataset (or it does not exist in the same"
                        " region as the data from the query)"
                    )
                elif "Access Denied: Table" in error_message:
                    print(
                        "Please check "
                        + self.color.BOLD
                        + "if you have access to all"
                        + self.color.END
                        + " the tables that you want to query and if they exists in the"
                        " same location as your temp_schema"
                    )
                elif "Syntax error" in error_message:
                    print(
                        self.color.BOLD
                        + "There's a syntax error on your query"
                        + self.color.END
                    )
                raise Exception(error_message)
        else:
            raise AssertionError(
                "Can not read from Big Query, parameter validation failed"
            )

    def read(self, **kwargs) -> DataFrame:
        table_id = kwargs.get("table_id", None)
        big_query_project = kwargs.get("big_query_project", self.big_query_project)
        dataset = kwargs.get("dataset", self.dataset)
        temp_schema = kwargs.get("temp_schema", self.temp_schema)
        table_name = kwargs.get("table", None)
        query = kwargs.get("query", None)
        if table_id is not None:  # Reading a full table with the whole table_id specified
            return self.read_table_by_id(table_id=table_id)
        elif (dataset is not None) and (table_name is not None):
            table_id = f"{big_query_project}.{dataset}.{table_name}"
            return self.read_table_by_id(table_id=table_id)
        elif (query is not None) and (temp_schema is not None):
            return self.read_query(query=query, temp_schema=temp_schema)
        elif (query is not None) and (temp_schema is None):
            print(
                "BigQuery does not allow for queries to run without a temp schema to"
                " store the results, please provide one"
            )
            raise KeyError("Missing temp_schema to save intermediate query results")
        else:
            raise KeyError(
                "Invalid combination of arguments, you must specify either: 1) A table_id"
                " 2) The Dataset and Table 3) The query and temp_schema"
            )


def key_generator(big_query_credentials=dict) -> None:
    json_bytes = json.dumps(big_query_credentials).encode("ascii")
    base64_bytes = base64.b64encode(json_bytes)
    base64_string = base64_bytes.decode("ascii")
    print(
        "O valor que você tem que colocar no parâmetro credentials do cluster Databricks"
        " é:\n\n"
        + f"{base64_string}"
    )
