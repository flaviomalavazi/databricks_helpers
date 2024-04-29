
provider "databricks" {
  auth_type = "azure-cli"
  alias     = "workspace"
  host      = data.azurerm_databricks_workspace.this.workspace_url
}

provider "azurerm" {
  # Configuration options
  features {}
}