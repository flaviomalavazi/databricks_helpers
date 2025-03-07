#%%
import delta_sharing

# %%
share_file_path = "../delta_sharing_credential_files/config_mais_recente.share" # for public shares: 'https://databricks-datasets-oregon.s3-us-west-2.amazonaws.com/delta-sharing/share/open-datasets.share'

# Create a SharingClient
client = delta_sharing.SharingClient(share_file_path)

# List all shared tables.
client.list_all_tables()

# %%
shares = client.list_shares()

for share in shares:
  schemas = client.list_schemas(share)
  for schema in schemas:
    tables = client.list_tables(schema)
    for table in tables:
      print(f'name = {table.name}, share = {table.share}, schema = {table.schema}')

# %%
table_url = f"{share_file_path}#flavio_malavazi_delta_sharing.cadastro.tab_gerentes"

# Use delta sharing client to load data
pandas_df = delta_sharing.load_as_pandas(table_url)

pandas_df.head(10)

# COMMAND ----------
