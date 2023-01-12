# Databricks notebook source
# COMMAND ----------

import base64
import json
import re

from py4j.protocol import Py4JJavaError
from pyspark.sql import DataFrame


# Validating each parameter for the BigQuery Connection
class BigQuery:
    """This class provides the user with an object to interact with GCP BigQuery
    using a Databricks Cluster + Spark. It wraps several common java errors when
    trying to connect with BigQuery and tries to deliver more useful error messages
    so that the user can debug and get their code running.

    Args:
        project_options (dict): A dictionary containing the keys `project` and
        `parentProject` that represent the BigQuery project's name and its parent
        project. The `parentProject` must match the one from the service account
        that will be used to authenticate the connection.
    kwargs:
        dataset (str): If you intend to only read tables from the same dataset,
        you can pass it on init in order to avoid repeating the parameter for
        every function call.
        materialization_dataset (str): If you want to run queries on BigQuery,
        you need to provide a materialization dataset for it, this parameter can
        be passed on init in order to avoid repeating it for every query that you
        want to run. One should configure the materialization_dataset so that the
        tables within it expire after some time in order to avoid costs related to
        storing redundant data on BigQuery. It is also important that the dataset
        is located on the same region as the ones used on the query that will run.

    Returns:
        BigQuery object that validates the Databricks cluster's GCP BigQuery settings
        for formatting errors and missing parameters, as well as allows the user to
        interact with BigQuery to read tables and run queries on it.

    """

    class color:
        BOLD = "\033[1m"
        END = "\033[0m"

    def __init__(self, project_options: dict, **kwargs) -> None:
        """Initializes the BigQuery object receiving a dictionary containing the
        project parameters that are mandatory when connecting to GCP BigQuery

        Args:
            project_options (dict): A dictionary containing the keys `project` and
            `parentProject` that represent the BigQuery project's name and its parent
            project. The `parentProject` must match the one from the service account
            that will be used to authenticate the connection.
        kwargs:
            dataset (str): If you intend to only read tables from the same dataset,
            you can pass it on init in order to avoid repeating the parameter for
            every function call.
            materialization_dataset (str): If you want to run queries on BigQuery,
            you need to provide a materialization dataset for it, this parameter can
            be passed on init in order to avoid repeating it for every query that you
            want to run. One should configure the materialization_dataset so that the
            tables within it expire after some time in order to avoid costs related to
            storing redundant data on BigQuery. It is also important that the dataset
            is located on the same region as the ones used on the query that will run.

        Raises:
            AssertionError: If the `parentProject` provided on project_options
            does not match the `spark.hadoop.fs.gs.project.id` parameter set on
            the Databricks cluster.

        Returns:
            None
        """
        self.errors = {}
        self.project_options = project_options
        self.project_id = None
        self.valid = False
        self.dataset = kwargs.get("dataset", None)
        self.materialization_dataset = kwargs.get("materialization_dataset", None)
        self.valid_project_id = self.validate_project_id()
        self.valid_email = self.validate_email()
        self.valid_private_key_id = self.validate_private_key_id()
        self.valid_private_key = self.validate_private_key()
        self.valid_credentials = self.validate_credentials()
        self.valid_enablement_flag = self.validate_google_service_account_enable()
        self.parent_project_validation_flag = self.parent_project_validation()
        self.success()

    def parent_project_validation(self) -> bool:
        """Validates if the `parentProject` defined in the `project_options`
        dictionary matches the one used in the Databricks cluster parameter
        `spark.hadoop.fs.gs.project.id`

        Returns:
            bool: Flag that indicates if this validation passed.
        """
        parent_project_validation = False
        if self.project_options["parentProject"] != self.project_id:
            self.errors["parent_project_validation"] = (
                "The parentProject informed for reading big query is not the same as the"
                " one provided for authentication purposes, please check!"
            )
        else:
            self.big_query_project = project_options["project"]
            parent_project_validation = True
        return parent_project_validation

    def validate_regex(
        self,
        validation_parameter: str,
        validation_string: str,
        regex: str,
    ) -> bool:
        """Validates if a given string matches a regex and returns the result
        of that assertion.

        Args:
            validation_parameter (str): The name of the parameter that should be
            validated, used to build a more user friendly error message.
            validation_string (str): The string that should be validated against.
            the regex.
            regex (str): The regex that contains the rules that the string should
            abide by.

        Returns:
            bool: flag that indicates if this validation passed
        """
        if re.fullmatch(regex, validation_string):
            return True
        else:
            try:
                self.errors["validate_regex"][validation_parameter] = (
                    self.color.BOLD
                    + f"Please verify the {validation_parameter} parameter. The one"
                    " provided"
                    + f" is invalid as it does not match the regex: {regex}"
                    + self.color.END
                )
            except KeyError:
                self.errors["validate_regex"] = {}
                self.errors["validate_regex"][validation_parameter] = (
                    self.color.BOLD
                    + f"Please verify the {validation_parameter} parameter. The one"
                    " provided"
                    + f" is invalid as it does not match the regex: {regex}"
                    + self.color.END
                )
            return False

    def check_existence(self, validation_parameter: str) -> str:
        """Validates if a given parameter is correctly defined in the Databricks
        cluster.

        Args:
            validation_parameter (str): The name of the parameter that should be
            validated.

        Returns:
            current_value (str): string containing the value for the parameter in
            the Databricks cluster
        """
        try:
            current_value = spark.conf.get(validation_parameter)
            return current_value
        except Py4JJavaError as e:
            if f"java.util.NoSuchElementException: {validation_parameter}" in str(e):
                try:
                    self.errors["check_existence"][validation_parameter] = (
                        self.color.BOLD
                        + f"""The {validation_parameter} was not defined in the cluster. Please go back to the cluster """
                        """configuration to set the parameter or set it using the """
                        f"""spark.conf.set("{validation_parameter}", "parameter_value") syntax if you want to use it for this """
                        """session only""" + self.color.END
                    )
                except KeyError:
                    self.errors["check_existence"]
                    self.errors["check_existence"][validation_parameter] = (
                        self.color.BOLD
                        + f"""The {validation_parameter} was not defined in the cluster. Please go back to the cluster """
                        """configuration to set the parameter or set it using the """
                        f"""spark.conf.set("{validation_parameter}", "parameter_value") syntax if you want to use it for this """
                        """session only""" + self.color.END
                    )
                return None
            else:
                raise e

    def validate_email(self) -> bool:
        """Validates if the service account email was set correctly on the
        Databricks cluster by checking it against an email regex and also
        against the suffix for a service account within the project_id that
        was defined on the cluster.

        Args:
            None

        Returns:
            bool: flag that indicates if this validation passed
        """
        valid_email_sufix = False
        email_regex = r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b"
        email_parameter = "spark.hadoop.fs.gs.auth.service.account.email"
        error_message = ""
        email = self.check_existence(
            validation_parameter=email_parameter,
        )
        regex_ok = self.validate_regex(
            validation_parameter=email_parameter,
            validation_string=email,
            regex=email_regex,
        )
        try:
            # Sufix specific email for service account: @project-id.iam.gserviceaccount.com
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
            elif email == "":
                error_message = "the service account email provided is blank"
            else:
                error_message = (
                    "the service account email is invalid! Can't determine the specific"
                    " error at this time."
                )
        except Exception as e:
            self.errors["validate_email"] = (
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
            self.errors["validate_email"] = (
                "The service account email is a valid formatted email address but"
                f" {error_message}"
            )
            return False
        else:
            self.errors["validate_email"] = (
                "The service account email provided to the Databricks cluster is not a"
                f" valid email format. {error_message}"
            )
            return False

    def validate_google_service_account_enable(self) -> bool:
        """Validates if the flag `spark.hadoop.google.cloud.auth.service.account.enable`
        is set to true on the Databricks cluster.

        Args:
            None

        Returns:
            bool: flag that indicates if this validation passed
        """
        google_service_account_enablement_parameter = (
            "spark.hadoop.google.cloud.auth.service.account.enable"
        )
        google_service_account_enablement = self.check_existence(
            validation_parameter=google_service_account_enablement_parameter,
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
            self.errors["validate_google_service_account_enable"] = (
                self.color.BOLD
                + "Flag Google Cloud Auth Service Account"
                + self.color.END
                + " is set to false, you must set it to true."
                + " This parameter is the"
                + " spark.hadoop.google.cloud.auth.service.account.enable"
                + " on the Databricks cluster."
            )
            return False
        else:
            self.errors["validate_google_service_account_enable"] = (
                "Google Cloud Auth Service Account Enablement Flag not provided to the"
                " Databricks cluster this parameter is the"
                " spark.hadoop.google.cloud.auth.service.account.enable"
            )
            return False

    def validate_project_id(self) -> bool:
        """Validates if the project id was set correctly on the Databricks
        cluster by checking it against a regex that contains the same rules
        as the one GCP uses to validate their project IDs. It also sets the
        project ID for the BigQuery object to the same provided for the
        cluster in order to run the rest of the validations.

        Args:
            None

        Returns:
            bool: flag that indicates if this validation passed
        """
        project_id_regex = r"^[a-z][a-z0-9-]{4,28}[a-z0-9]$"
        project_id_parameter = "spark.hadoop.fs.gs.project.id"
        project_id = self.check_existence(
            validation_parameter=project_id_parameter,
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
            self.errors["validate_project_id"] = (
                "Invalid Project ID provided! The project ID must be 6 to 30 characters"
                " in length, contain only lowercase letters, numbers and hyphens, start"
                " with a letter and not end with a hyphen. This parameter is the"
                " spark.hadoop.fs.gs.project.id on the Databricks cluster."
            )
            return False

    def validate_private_key_id(self) -> bool:
        """Validates if the private key id was set correctly on the Databricks
        cluster by checking it against a regex that matches valid private key
        ids.

        Args:
            None

        Returns:
            bool: flag that indicates if this validation passed
        """
        private_key_id_regex = r"^[a-zA-Z0-9]{40}$"
        private_key_id_parameter = (
            "spark.hadoop.fs.gs.auth.service.account.private.key.id"
        )
        private_key_id = self.check_existence(
            validation_parameter=private_key_id_parameter,
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
            self.errors["validate_private_key_id"] = (
                "Invalid private key ID provided! The key ID should be a 40 char"
                " alphanumeric string with no special characters. This parameter"
                " is the: spark.hadoop.fs.gs.auth.service.account.private.key.id"
                " on the Databricks cluster"
            )
            return False

    def validate_private_key(self) -> bool:
        """Validates if the private key was set correctly on the Databricks
        cluster by checking if it begins with begin private key and ends with
        end private key (a very rudimentary check).

        Args:
            None

        Returns:
            bool: flag that indicates if this validation passed
        """
        # TODO: improve this validation
        private_key_start = "-----BEGIN PRIVATE \nKEY-----"
        private_key_end = "-----END \nPRIVATE KEY-----\\n"
        private_key_parameter = "spark.hadoop.fs.gs.auth.service.account.private.key"
        private_key = self.check_existence(
            validation_parameter=private_key_parameter,
        )
        if private_key.startswith(private_key_start) and private_key.endswith(
            private_key_end
        ):
            print(self.color.BOLD + "Private key" + self.color.END + " seems to be ok!")
            return True
        else:
            self.errors["validate_private_key"] = (
                self.color.BOLD
                + r"""Invalid private key provided! It should start with -----BEGIN PRIVATE KEY-----"""
                r""" and end with -----END PRIVATE KEY-----\n""" + self.color.END
            )
            return False

    def validate_credentials(self) -> bool:
        """Validates if the credentials provided to the Databricks cluster
        match a base64 regex.

        Args:
            None

        Returns:
            bool: flag that indicates if this validation passed
        """
        credentials_regex = r"^(?:[A-Za-z0-9+\/]{4})*(?:[A-Za-z0-9+\/]{4}|[A-Za-z0-9+\/]{3}=|[A-Za-z0-9+\/]{2}={2})$"
        credentials_parameter = "credentials"
        credentials = self.check_existence(
            validation_parameter=credentials_parameter,
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
            self.errors["validate_credentials"] = (
                "Blank credentials provided! The parameter needs to be set to the base64"
                " encoding of the credentials JSON (it's a very long string)"
            )
        else:
            self.errors["validate_credentials"] = "Invalid credentials provided!"
        return False

    def success(self) -> bool:
        """Checks if all the validations passed and sets the valid flag
        for the BigQuery object so that it is allowed to run queries on
        the Data Warehouse.

        Args:
            None

        Returns:
            bool: flag that indicates if this validation passed
        """
        if (
            self.valid_project_id
            and self.valid_email
            and self.valid_private_key_id
            and self.valid_private_key
            and self.valid_credentials
            and self.valid_enablement_flag
            and self.parent_project_validation_flag
        ):
            print(
                "\n\nThe Databricks Cluster parameters to access the BigQuery Warehouse"
                " seem Ok"
            )
            self.valid = True
        else:
            for dict_key in self.errors.keys():
                if (dict_key == "check_existence") or (dict_key == "validate_regex"):
                    for parameter in self.errors[dict_key].keys():
                        print(self.errors[dict_key][parameter])
                else:
                    print(self.errors[dict_key])
            raise AssertionError(
                "You have one or more errors in our BigQuery Parameters, please check"
                " above"
            )

    def read_table_by_id(self, table_id: str) -> DataFrame:
        """Reads a whole table from BigQuery to the Databricks
        cluster.

        Args:
            table_id (str): the table_id composed of a three level namespace
            built as `project_id.dataset.table_name`

        Returns:
            DataFrame: A Spark DataFrame with the data for the whole BQ
            table.
        """
        if self.valid:
            try:
                return (
                    spark.read.format("bigquery")
                    .option("table", table_id)  # Reading the full table
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

    def read_query(self, query: str, materialization_dataset: str) -> DataFrame:
        """Runs a query on GCP BigQuery and fetches the results to the Databricks
        cluster.

        Args:
            query (str): the query that needs to be run on BigQuery
            materialization_dataset (str): the dataset that will be used to materialize
            the query results before making them available to Databricks. Should be
            in the same region as the tables that will be queried.

        Returns:
            DataFrame: A Spark DataFrame with the data resulting from the query.
        """
        if self.valid:
            try:
                return (
                    spark.read.format("bigquery")
                    .option("materializationDataset", materialization_dataset)
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
                        f"""as the and the materialization_dataset dataset ({materialization_dataset})"""
                    )
                elif "was not found in location" in error_message:
                    print(
                        "Please verify if"
                        + self.color.BOLD
                        + f""" the datasets used by the query and the materialization_dataset dataset ({materialization_dataset})"""
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
                        " same location as your materialization_dataset"
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
        """Reads data from big query using a table_id or a query and a materialization
        dataset.

        kwargs:
            table_id (str): the table_id composed of a three level namespace
            built as `project_id.dataset.table_name`
            big_query_project (str): the name of the big query project where the dataset is
            located (if not passed, it will default to the one on the project_options used
            when initializing the BigQuery object).
            dataset (str): the name of the big query dataset where the table is
            located (if not passed, it will default to the one used when initializing the
            BigQuery object).
            table_name (str): the name of the big query table that you want to read, if dataset
            and big_query_project are defined on the BigQuery object, or if they're passed on to
            the read function, this parameter is used to build the table_id that will be passed
            to BigQuery.
            query (str): the query that needs to be run on BigQuery
            materialization_dataset (str): the dataset that will be used to materialize
            the query results before making them available to Databricks. Should be
            in the same region as the tables that will be queried.

        Returns:
            DataFrame: A Spark DataFrame with the data resulting from the query.
        """
        table_id = kwargs.get("table_id", None)
        big_query_project = kwargs.get("big_query_project", self.big_query_project)
        dataset = kwargs.get("dataset", self.dataset)
        materialization_dataset = kwargs.get(
            "materialization_dataset", self.materialization_dataset
        )
        table_name = kwargs.get("table", None)
        query = kwargs.get("query", None)
        if table_id is not None:  # Reading a full table with the whole table_id specified
            return self.read_table_by_id(table_id=table_id)
        elif (dataset is not None) and (table_name is not None):
            table_id = f"{big_query_project}.{dataset}.{table_name}"
            return self.read_table_by_id(table_id=table_id)
        elif (query is not None) and (materialization_dataset is not None):
            return self.read_query(
                query=query, materialization_dataset=materialization_dataset
            )
        elif (query is not None) and (materialization_dataset is None):
            print(
                "BigQuery does not allow for queries to run without a temp schema to"
                " store the results, please provide one"
            )
            raise KeyError(
                "Missing materialization_dataset to save intermediate query results"
            )
        else:
            raise KeyError(
                "Invalid combination of arguments, you must specify either: 1) A table_id"
                " 2) The Dataset and Table 3) The query and materialization_dataset"
            )


def credentials_generator(big_query_credentials: dict) -> None:
    """Helper method to generate the `credentials` parameter for the
    Databricks cluster

    Args:
        big_query_credentials (dict): the BigQuery credentials json for your service
        account passed as a python dictionary (just copy the json from the file and
        paste it on the notebook).
    """
    json_bytes = json.dumps(big_query_credentials).encode("ascii")
    base64_bytes = base64.b64encode(json_bytes)
    base64_string = base64_bytes.decode("ascii")
    print(
        "O valor que você tem que colocar no parâmetro credentials do cluster Databricks"
        " é:\n\n"
        + f"{base64_string}"
    )
