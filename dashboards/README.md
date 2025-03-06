# Autotask Data Sink Grafana Dashboards

This directory contains Grafana dashboard JSON definitions that can be imported into your Grafana instance.

## Available Dashboards

- **ticket_metrics.json**: Dashboard for ticket metrics including created vs. completed tickets, average resolution time, and tickets by resource.

## How to Import

1. Log in to your Grafana instance
2. Navigate to Dashboards > Import
3. Either upload the JSON file or paste the contents
4. Select the appropriate data source (should be your PostgreSQL database)
5. Click Import

## Customizing Dashboards

These dashboards are provided as starting points. You can customize them to fit your specific needs:

1. Add additional panels for other metrics
2. Modify existing queries to filter by specific companies, resources, or time periods
3. Create new dashboards for other entity types like time entries or contracts

## Creating New Dashboards

When creating new dashboards, make sure to:

1. Use the `autotask` schema for all queries
2. Leverage the views created by the application when possible
3. Export your dashboards as JSON and add them to this directory for version control 