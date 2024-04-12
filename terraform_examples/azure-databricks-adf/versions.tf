terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = ">=3.88.0"
    }
    databricks = {
      source  = "databricks/databricks"
      version = ">=1.39.0"
    }
  }
}
