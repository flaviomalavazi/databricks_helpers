// Fetching properties for existing databricks workspace
data "azurerm_databricks_workspace" "this" {
  name                = var.adb_workspace_name
  resource_group_name = var.rg_name
}

// Fetching a sample node type
data "databricks_node_type" "this" {
  provider              = databricks.workspace
  local_disk            = true
  photon_driver_capable = true
  photon_worker_capable = true
  min_cores             = 4
  min_memory_gb         = 15
}

// Fetching the latest LTS databricks runtime
data "databricks_spark_version" "latest_lts" {
  provider          = databricks.workspace
  long_term_support = true
}

// Defining cluster policies that force UC access mode to shared
variable "cluster_access_mode_policy" {
  type        = map(map(string))
  description = "value"
  default = {
    "data_security_mode" = {
      "type"  = "fixed",
      "value" = "USER_ISOLATION"
    }
  }
}

// Creating the policy in the workspace we are working with
resource "databricks_cluster_policy" "uc_shared_cluster_policy" {
  provider   = databricks.workspace
  name       = "UC shared cluster policy for ADF"
  definition = jsonencode(merge(var.cluster_access_mode_policy))
}
