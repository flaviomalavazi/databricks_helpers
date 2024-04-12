// Creating an Azure Data Factory resource
resource "azurerm_data_factory" "this" {
  name                = var.adf_name
  location            = var.location
  resource_group_name = var.rg_name
  identity {
    type = "SystemAssigned"
  }
}

// Getting the ADF Service principal application ID
data "azuread_service_principal" "adf_service_principal" {
  object_id = resource.azurerm_data_factory.this.identity[0].principal_id
  depends_on = [ resource.azurerm_data_factory.this ]
}
