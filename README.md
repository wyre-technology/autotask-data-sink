# Autotask Data Sink

A continuous service that extracts data from Kaseya Autotask PSA and stores it in a PostgreSQL database for analytics and reporting.

## Overview

This project provides a data pipeline that:

1. Connects to the Autotask API using the [autotask-go](https://github.com/asachs01/autotask-go) library
2. Extracts data with different sync frequencies based on entity type
3. Stores data in PostgreSQL with historical tracking
4. Enables analytics through Grafana dashboards

## Features

- Continuous syncing of Autotask entities
- Historical data tracking for all entities
- Optimized database schema for common query patterns
- Support for ticket metrics, time entry analysis, and contract profitability

## Requirements

- Go 1.18+
- PostgreSQL 13+
- Autotask API credentials

## Setup

1. Clone the repository
2. Create a PostgreSQL database
3. Configure the application (see Configuration section)
4. Build and run the service

```bash
# Build the application
go build -o autotask-data-sink ./cmd/autotask-data-sink

# Run the application
./autotask-data-sink
```

## Configuration

The application can be configured using a YAML file (`config.yaml`) or environment variables. For sensitive information like API keys and database passwords, environment variables are recommended.

### Configuration File

Create a `config.yaml` file in the root directory:

```yaml
autotask:
  username: "your-username"
  secret: "your-api-key"
  integration_code: "your-integration-code"

database:
  host: "localhost"
  port: 5432
  name: "autotask_data"
  user: "postgres"
  password: "your-password"
  ssl_mode: "disable"

sync:
  # Sync frequencies in minutes
  tickets: 15
  time_entries: 15
  companies: 1440  # Daily
  contacts: 1440   # Daily
```

### Environment Variables

All configuration options can be set using environment variables with the prefix `AUTOTASK_SINK_`. For example:

```bash
export AUTOTASK_SINK_AUTOTASK_USERNAME="your-username"
export AUTOTASK_SINK_DATABASE_HOST="localhost"
```

### PostgreSQL Environment Variables

The application also supports standard PostgreSQL environment variables:

```bash
export PGHOST="localhost"
export PGPORT="5432"
export PGDATABASE="autotask_data"
export PGUSER="postgres"
export PGPASSWORD="your-password"
export PGSSLMODE="disable"
```

### Configuration Reference

| Section  | Key              | Environment Variable                    | PostgreSQL Env Var | Description                                                | Default Value |
|----------|------------------|----------------------------------------|--------------------|------------------------------------------------------------|---------------|
| autotask | username         | AUTOTASK_SINK_AUTOTASK_USERNAME        | -                  | Autotask API username                                      | -             |
| autotask | secret           | AUTOTASK_SINK_AUTOTASK_SECRET          | -                  | Autotask API key/secret                                    | -             |
| autotask | integration_code | AUTOTASK_SINK_AUTOTASK_INTEGRATION_CODE| -                  | Autotask API integration code                              | -             |
| database | host             | AUTOTASK_SINK_DATABASE_HOST            | PGHOST             | PostgreSQL server hostname                                 | localhost     |
| database | port             | AUTOTASK_SINK_DATABASE_PORT            | PGPORT             | PostgreSQL server port                                     | 5432          |
| database | name             | AUTOTASK_SINK_DATABASE_NAME            | PGDATABASE         | PostgreSQL database name                                   | autotask_data |
| database | user             | AUTOTASK_SINK_DATABASE_USER            | PGUSER             | PostgreSQL username                                        | postgres      |
| database | password         | AUTOTASK_SINK_DATABASE_PASSWORD        | PGPASSWORD         | PostgreSQL password                                        | -             |
| database | ssl_mode         | AUTOTASK_SINK_DATABASE_SSL_MODE        | PGSSLMODE          | PostgreSQL SSL mode (disable, require, verify-ca, verify-full) | disable    |
| sync     | tickets          | AUTOTASK_SINK_SYNC_TICKETS             | -                  | Sync frequency for tickets in minutes                      | 15            |
| sync     | time_entries     | AUTOTASK_SINK_SYNC_TIME_ENTRIES        | -                  | Sync frequency for time entries in minutes                 | 15            |
| sync     | companies        | AUTOTASK_SINK_SYNC_COMPANIES           | -                  | Sync frequency for companies in minutes                    | 1440 (daily)  |
| sync     | contacts         | AUTOTASK_SINK_SYNC_CONTACTS            | -                  | Sync frequency for contacts in minutes                     | 1440 (daily)  |
| sync     | resources        | AUTOTASK_SINK_SYNC_RESOURCES           | -                  | Sync frequency for resources in minutes                    | 1440 (daily)  |
| sync     | contracts        | AUTOTASK_SINK_SYNC_CONTRACTS           | -                  | Sync frequency for contracts in minutes                    | 1440 (daily)  |
| log      | level            | AUTOTASK_SINK_LOG_LEVEL                | -                  | Log level (debug, info, warn, error)                       | info          |
| log      | format           | AUTOTASK_SINK_LOG_FORMAT               | -                  | Log format (json, console)                                 | json          |

## Command Line Flags

The application supports the following command line flags:

- `--config`: Path to config file
- `--once`: Run sync once and exit
- `--skip-initial`: Skip initial sync

## Usage

Once running, the service will automatically sync data based on the configured schedules.

## Grafana Integration

Connect Grafana to the PostgreSQL database and import the provided dashboards from the `dashboards` directory.

## Docker Deployment

A Docker Compose file is provided for easy deployment:

```bash
# Set required environment variables
export AUTOTASK_USERNAME="your-username"
export AUTOTASK_SECRET="your-api-key"
export AUTOTASK_INTEGRATION_CODE="your-integration-code"

# Start the services
docker-compose up -d
```

## License

MIT 