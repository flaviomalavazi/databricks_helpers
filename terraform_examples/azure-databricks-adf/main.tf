
// First we need to add the ADF service principal to the Azure databricks account
resource "databricks_service_principal" "adf_sp" {
  provider       = databricks.accounts
  display_name   = "ADF Service Principal"
  application_id = data.azuread_service_principal.adf_service_principal.client_id
}

// Then we assign the ADF service principal to the workspace as a common user (avoiding admin assignment)
resource "databricks_permission_assignment" "add_admin_spn" {
  provider     = databricks.workspace
  principal_id = databricks_service_principal.adf_sp.id
  permissions  = ["USER"]
  depends_on   = [databricks_service_principal.adf_sp]
}

// Now we fetch the service principal's ID for that workspace
data "databricks_service_principal" "adf_databricks_sp" {
  provider       = databricks.workspace
  application_id = data.azuread_service_principal.adf_service_principal.client_id
  depends_on     = [databricks_permission_assignment.add_admin_spn]
}

// And grant usage permission to the ADF service principal to our Unity Catalog Shared Cluster policy
resource "databricks_permissions" "adf_cluster_policy" {
  provider          = databricks.workspace
  cluster_policy_id = databricks_cluster_policy.uc_shared_cluster_policy.id
  access_control {
    service_principal_name = data.databricks_service_principal.adf_databricks_sp.application_id
    permission_level       = "CAN_USE"
  }
}

// Finally, we create the linked service using the custom service resource of the azurerm terraform provider
resource "azurerm_data_factory_linked_custom_service" "azure_databricks_linked_service_with_shared_uc_clusters" {
  name                 = "ADBLinkedServiceViaMSIUCSharedCompute"
  data_factory_id      = resource.azurerm_data_factory.this.id
  type                 = "AzureDatabricks"
  description          = "Demo Azure Databricks Linked Service using MSI and Unity Catalog Shared Compute"
  type_properties_json = <<JSON
          {
            "accessToken": null,
            "authentication": "MSI",
            "workspaceResourceId": "${data.azurerm_databricks_workspace.this.id}",
            "domain": "https://${data.azurerm_databricks_workspace.this.workspace_url}",
            "policyId": "${databricks_cluster_policy.uc_shared_cluster_policy.policy_id}",
            "newClusterDriverNodeType": "${data.databricks_node_type.this.id}",
            "newClusterNodeType": "${data.databricks_node_type.this.id}",
            "newClusterInitScripts": [],
            "newClusterLogDestination": "",
            "newClusterNumOfWorker": "1:2",
            "newClusterVersion": "${data.databricks_spark_version.latest_lts.id}",
            "newClusterCustomTags": {
                "managedBy": "Terraform",
                "usedBy": "ADF"
            }
        }
JSON

}
