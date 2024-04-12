variable "location" {
  type        = string
  description = "Location of the resources"
}

variable "rg_name" {
  type        = string
  description = "Resource group that contains our assets"
}

variable "adf_name" {
  type        = string
  description = "Name of the Data Factory workspace"
}

variable "adb_workspace_name" {
  type        = string
  description = "Name of the Azure Databricks workspace"
}

variable "databricks_account_id" {
  type        = string
  description = "Databricks account ID"
}
