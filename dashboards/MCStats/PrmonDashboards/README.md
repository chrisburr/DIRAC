# MCStats Kibana PRMON Dashboards

These are two dashboards for Gauss prmon metrics in MCStats

## Gauss Metrics per Job

This dashboard displays Gauss prmon Metrics per Job.

## Gauss Metrics per Production

This dashboard displays Gauss prmon Metrics per Production.

## PrmonVisualisations.json

This file contains all the visualizations used in the dashboards

# Uploading the dashboards

- First you need to create the index pattern 'lhcb-gaussmetrics-\*' which gathers all the the indices that contain Metrics data.
In order to simplify the process, choose as an id for the index pattern '45a31030-e609-11ea-afe9-41f15642af81'.
Of course you can choose a different one, but make sure to change it for all the visualizations in the json file.

- In Kibana -> Management -> Saved Objects, import the visualizations using the PrmonVisualizations.json file.

- In Kibana -> Management -> Saved Objects, import the dashboards using the PrmonDashboards.json file.
