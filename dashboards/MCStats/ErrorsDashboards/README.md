# MCStats Kibana Errors Dashboards

These are three dashboards for errors in MCStats

## MCStatsErrorsPerJob

This dashboard displays errors per job. By default, it is showing errors for jobs in the production '00095697'. It can be changed using the filter in the dashboard in Kibana or directly from the json file.

## MCStatsErrorsPerProduction

This dashboard shows errors on a production level. By default, it is showing errors from these 10 productions:
'0096481', '0096484', '0096487', '0096490', '0096868', '0096883', '0096907', '0096910', '0096998', '0097001' .
They can be changed using the filter in the dahsboard in Kibana or directly from the json file.

## MCStatsProductionsWithMostErrors

This dashboard displays productions with the highest number of errors.

## MCStatsVisualization.json

This file contains all the visualizations used in the dashboards

# Uploading the dashboards

- First you need to create the index pattern 'lhcb-mcstats-' which gathers all the the indices that contain Error data.
In order to simplify the process, choose as an id for the index pattern 'e0cc1ec0-496c-11e9-97b4-bda41e0e0b3d'.
Of course you can choose a different one, but make sure to change it for all the visualizations in the json file.

- In Kibana -> Management -> Saved Objects, import the visualizations using the MCStatsVisualizations.json file.

- In Kibana -> Management -> Saved Objects, import the three dashboards using the MCStatsErrorsPerJob.json, MCStatsErrorsPerProduction.json, MCStatsProductionsWithMostErrors.json files.

Note: Some dashboards may not contain some visualizations and display the message [Could not locate that visualization (id:------)] if the data don't contain the type of errors in the visualization.
