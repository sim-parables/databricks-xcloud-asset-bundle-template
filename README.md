# Sim-Parables Databricks Asset Bundle Template

It's preferable to test Databricks on Azure given the reduced time to cluster creation/ SQL Table & Warehouse creation vs GCP/AWS.    

## Setup

To create a new Databricks Asset Bundle using this template, follow the instructions below
1. Move into parent directory of the DAB Template
```
mkdir ~/databricks
cd ~/databricks
```

2. Create a new Databricks Asset Bundle. Follow prompts for variable input.
```
databricks bundle init databricks_dab_template \
  --output-dir dab_solution \
  --profile=AZURE_WORKSPACE
```

3. Make required changes to package and validate the contents to verify the DAB will deploy proactively.
```
cd dab_solution
databricks bundle validate \
  --profile=AZURE_WORKSPACE \
  --var="databricks_cluster_id=$(DATABRICKS_CLUSTER_ID)"
```

4. After a succesful validation, deploy and run the DAB in Databricks.
```
databricks bundle deploy \
  --profile=AZURE_WORKSPACE \
  --var="databricks_cluster_id=$(DATABRICKS_CLUSTER_ID)"
```

5. For command options, use the help options
```
databricks bundle --help
```

# Appendix
 - **DAB First Customer Template Tutorial** https://docs.databricks.com/en/dev-tools/bundles/custom-bundle.html
 - **DAB Configuration Options** https://docs.databricks.com/en/dev-tools/bundles/templates.html
