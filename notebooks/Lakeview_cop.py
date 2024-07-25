# Databricks notebook source
# MAGIC %md
# MAGIC # Lakeview cop
# MAGIC ## Version 0.1
# MAGIC ### Author: Flavio Malavazi
# MAGIC This notebook implements a simple class that monitors lakeview dashboards that have been published using embedded credentials and then, if needed, alters the published dashboard by removing the embedded credentials.

# COMMAND ----------

from typing import List
import requests
import itertools
from json import dumps, loads
from time import sleep

class lakeview_cop:

    def __init__(self, page_size: int = 10):
        """
        Initialize the lakeview cop by obtaining credentials and defining table size for API pagination.

        Parameters
        ----------
            page_size : int, number of objects per page in our request to the lakeview dashboards list API

        Returns
        -------
            None
        """
        self.__api_host = f"https://{spark.conf.get('spark.databricks.workspaceUrl')}"
        self.__headers = {'Authorization': 'Bearer {}'.format(dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None))}
        self.__page_size = page_size

    def list_lakeview_dashboard_ids(self) -> List:
        """
        List all the lakeview dashboard IDs for a given workspace

        Parameters
        ----------
            None

        Returns
        -------
            list[str]: List of dashboard IDs (uuid that is used to idenfity a lakeview dashboard).
        """
        list_of_dashboards = []
        endpoint = "api/2.0/lakeview/dashboards"
        body = {
            "page_size": self.__page_size,
            "page_token": None
        }
        keep_going = True
        while keep_going:
            response = requests.get(url=f"{self.__api_host}/{endpoint}", params=body, headers=self.__headers)
            if response.status_code == 200 and loads(response.text) != {}:
                response_dict = loads(response.text)
                try:
                    list_of_dashboards.append(response_dict["dashboards"])
                    if "page_token" in response_dict:
                        body["page_token"] = response_dict["next_page_token"]
                    else:
                        keep_going = False
                except KeyError:
                    raise Exception(f"Error fetching list of dashboards\nReceived: response.status_code = {response.status_code}\nAnd\nresponse.text = {response.text}")
            else:
                print("Finished correctly")
                keep_going = False
            sleep(1)
        list_of_dashboards = list(itertools.chain(*list_of_dashboards))
        list_of_dashboard_ids = [dashboard["dashboard_id"] for dashboard in list_of_dashboards]
        return list_of_dashboard_ids

    def __get_published_dashboard_with_embedded_credential(self, dashboard_id: str) -> dict:
        """
        Get the details for a dashboard that was published with embedded credentials.

        Parameters
        ----------
            dashboard_id : str, UUID that identifies the dashboard that will have its details fetched

        Returns
        -------
            dict: if the dashboard was published using embedded credentials, returns a dictionary object
            containing dashboard id, warehouse id to where it was published and its display name
        """
        endpoint = f"api/2.0/lakeview/dashboards/{dashboard_id}/published"
        response = requests.get(url=f"{self.__api_host}/{endpoint}", headers=self.__headers)
        if response.status_code == 404:
            return None
        else:
            response_dict = loads(response.text)
            if response_dict["embed_credentials"]:
                return {"dashboard_id": dashboard_id, "warehouse_id": response_dict["warehouse_id"], "display_name": response_dict["display_name"]}
            else:
                return None
            
    def get_dashboards_with_embedded_credentials(self, list_of_dashboard_ids: List = None) -> List:
        """
        Get the details for a dashboard that was published with embedded credentials.

        Parameters
        ----------
            *optional: list_of_dashboard_ids: list[str] list of UUIDs with the lakeview dashboards we want to check
            (if None, calls the list dashboards method and checks all dashboards in the workspace).

        Returns
        -------
            list[dict]: list of dictionary with the details of the dashboards that were published using embedded credentials.
        """
        list_of_dashboard_ids = self.list_lakeview_dashboard_ids()
        summary = []
        for dashboard in list_of_dashboard_ids:
            summary.append(self.__get_published_dashboard_with_embedded_credential(dashboard_id=dashboard))
            sleep(1)
        return list(filter(lambda item: item is not None, summary))
    
    def fix_out_of_compliance_dashboards(self, list_of_dashboards: list[dict]):
        """
        Republishes dashboards with no embedded credentials.

        Parameters
        ----------
            list_of_dashboards: list[dict] list of dictionaries with the details of the lakeview dashboards that will
            be republished with no embedded credentials.

        Returns
        -------
            list[dict]: list of dictionaries with the details of the dashboards and the status of their fix.
        """
        fixed_dashboards = []
        for dashboard in list_of_dashboards:
            success_unpublish = self.__unpublish_dashboard(dashboard)
            success_publish = self.__publish_dashboard(dashboard)
            if success_unpublish and success_publish:
                fixed_dashboards.append({"display_name": dashboard["display_name"], "dashboard_id": dashboard["dashboard_id"], "fixed": True})
            else:
                fixed_dashboards.append({"display_name": dashboard["display_name"], "dashboard_id": dashboard["dashboard_id"], "fixed": False})
            sleep(1)
        return fixed_dashboards

    def __publish_dashboard(self, dashboard_dict: dict):
        """
        Publish a lakeview dashboard with no embedded credentials.

        Parameters
        ----------
            dashboard_dict: dict Dictionary with the details of the lakeview dashboard that will be published.

        Returns
        -------
            bool: flag to indicate success in publishing the dashboard (true/false)
        """
        endpoint = f"api/2.0/lakeview/dashboards/{dashboard_dict['dashboard_id']}/published"
        body = {
            "embed_credentials": False,
            "warehouse_id": dashboard_dict['warehouse_id'] 
        }
        response = requests.post(url=f"{self.__api_host}/{endpoint}", data = dumps(body), headers=self.__headers)
        if response.status_code == 200:
            return True
        else:
            return False

    def __unpublish_dashboard(self, dashboard_dict: dict) -> bool:
        """
        Unublish a lakeview dashboard with no embedded credentials.

        Parameters
        ----------
            dashboard_dict: dict Dictionary with the details of the lakeview dashboard that will be unpublished.

        Returns
        -------
            bool: flag to indicate success in unpublishing the dashboard (true/false)
        """
        endpoint = f"api/2.0/lakeview/dashboards/{dashboard_dict['dashboard_id']}/published"
        response = requests.delete(url=f"{self.__api_host}/{endpoint}", headers=self.__headers)
        if response.status_code == 200:
            return True
        else:
            return False

# COMMAND ----------

my_cop = lakeview_cop(page_size=50)

# COMMAND ----------

dashboard_ids = my_cop.list_lakeview_dashboard_ids()

# COMMAND ----------

dashboards = my_cop.get_dashboards_with_embedded_credentials(dashboard_ids)

# COMMAND ----------

print(f"This is the list of dashboards that I'll fix: {dashboards}")

# COMMAND ----------

result = my_cop.fix_out_of_compliance_dashboards(dashboards)

# COMMAND ----------

print(f"This is the result of the fixes {result}")
