# Configuring an Azure Databricks linked service on Azure Data Factory leveraging Unity Catalog Shared Compute

## Intro
This example showcases the integration between Azure Databricks and Azure Data Factory in a way that allows ADF to create new job clusters on Azure Databricks that use the Shared Access mode compute, hence being able to leverage several features enabled by this type of cluster and allowing workloads to be ran with the same access mode as they have been developed on an interactive shared compute.

We also use the Managed System Identity for ADF to authenticate with Databricks, thus avoiding handling PATs and allowing us to grant granular permissions to the factory.

## Contents
- [adf.tf](./adf.tf) -> Creates the ADF instance and retrieves its system generated identity
- [azure_databricks.tf](./azure_databricks.tf) -> Retrieves information on an _existing_ databricks workspace and create a cluster policies that enforce the shared compute access mode.
- [main.tf](./main.tf) -> Adds the ADF Service Principal to the Azure Databricks account, assigns it to our workspace, grants permission for it to use the cluster policy we created in the Databricks setup file and creates a custom ADF Linked Service using the `JSON` syntax to pass the policy ID that will enforce the access mode on clusters created by ADF when running jobs
- [providers.tf](./providers.tf) -> Initizalizes providers
- [variables.tf](./variables.tf) -> Creates variables
- [versions.tf](./versions.tf) -> Establishes the provider versions that are necessary to run this example

## How to run this example
One can run this example by populating the variables and running a simple `terraform apply`, provided that they have the following permissions/levels of access:
- Permission to create resources within an Azure Resource Group (for creating the ADF service)
- Account Admin privileges on Azure Databricks (so that you can add the ADF Service Principal to the account and assign it to a workspace)
- Azure Databricks Workspace Admin privileges (for creating the cluster policy and assigning it to the ADF SP.)

The ADF service principal will also need permissions to interact with the UC data (reading/writing to UC tables), that can be handled just as any other principal (users, groups, service principals) would, so it might be necessary to have permission to grant access to the assets that you want ADF to use.

