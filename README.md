# Data Lakehouse Project

<div align="center">

<p align="center">
  <img src="https://img.shields.io/badge/Trino-0E76A8?style=for-the-badge&logo=trino&logoColor=white" alt="Trino"/>
  <img src="https://img.shields.io/badge/dbt-FF694B?style=for-the-badge&logo=dbt&logoColor=white" alt="dbt"/>
  <img src="https://img.shields.io/badge/Apache_Iceberg-2C3E50?style=for-the-badge&logo=apache&logoColor=white" alt="Apache Iceberg"/>
  <img src="https://img.shields.io/badge/Nessie-3178C6?style=for-the-badge&logoColor=white" alt="Project Nessie"/>
  <img src="https://img.shields.io/badge/DuckDB-FFF000?style=for-the-badge&logo=duckdb&logoColor=black" alt="DuckDB"/>
  <img src="https://img.shields.io/badge/Snowflake-29B5E8?style=for-the-badge&logo=snowflake&logoColor=white" alt="Snowflake"/>
</p>

<p align="center">
  <i>An exploratory assignment on modern data lakehouse architectures for IF4044 Big Data Technology</i>
</p>

</div>

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [TPC-H Benchmark](#tpc-h-benchmark)

## Overview

This project implements a modern data lakehouse architecture using Apache Iceberg, Project Nessie, Trino, and dbt. It demonstrates data lakehouse capabilities using TPC-H benchmark data for performance evaluation and showcases data transformation workflows.

## Architecture

<p align="center">
  <img src="https://via.placeholder.com/800x400?text=Data+Lakehouse+Architecture" alt="Architecture Diagram"/>
</p>

The architecture consists of:
- **dbt**: Data transformation and modeling framework
- **DuckDB**: Embedded analytical database used for alternative TPC-H data generation
- **Snowflake**: Cloud data platform for comparison and integration scenarios
- **Object Storage**: S3-compatible storage for data files

We also tried:
- **Apache Iceberg**: Table format providing ACID transactions and schema evolution
- **Project Nessie**: Git-like version control for data
- **Trino**: Distributed SQL query engine for data access
However, we do not continue this architecture since we apparently Trino cannot create Iceberg table from Parquet files. The Iceberg x Nessie x Trino set-up is completed, though. You can see Trino UI and Nessie UI with these steps.

## Project Structure

```
.
├── csv_vs_parquet_comparison/ # contains queries for comparing csv and parquet
│   ├── queries/ 
├── if4044_tpch/               # dbt project for TPC-H analytics
│   ├── models/                # dbt models (staging, marts)
│   ├── macros/                # Custom SQL macros
│   ├── analyses/              # Ad-hoc analytical queries
│   ├── tests/                 # Data tests
│   └── dbt_project.yml        # dbt project configuration
│
├── trino-config/              # Trino server configuration
│   ├── catalog/               # Connector configurations
│   │   └── iceberg.properties # Iceberg catalog config
│   ├── config.properties      # Trino server properties
│   ├── jvm.config             # JVM configuration
│   ├── log.properties         # Trino log properties 
│   └── node.properties        # Node-specific configuration
│
├── tpc-h/                     # TPC-H data generation tools
│   ├── tpch-dbgen/            # TPC-H data generator
│   ├── scripts/               # Helper scripts for data loading
│   └── tpch-dbgen-to-aws-s3/  # S3 upload utilities
│
├── tpc-h-alt/                 # Alternative TPC-H generation with DuckDB
│   ├── upload-to-r2.sh        # Script for uploading to Cloudflare R2 
│   └── README.md              # Instructions for DuckDB-based TPC-H generation
│
├── docker-compose.yml         # Docker services definition
└── README.md                  # This file
```

## Prerequisites

- Docker
- S3-compatible storage (MinIO, AWS S3, Cloudflare R2, etc.)
- Python 3.8+

## How to Run

### 1. Initialize TPC-H data

```bash
cd tpc-h/tpch-dbgen-to-aws-s3/
chmod +x run.sh
./run.sh
```

### 2. Run dbt models

```bash
cd /if4044_tpch
dbt run
```

## TPC-H Benchmark

This project includes TPC-H benchmark data and queries to evaluate the performance of the data lakehouse. The TPC-H benchmark consists of a suite of business-oriented ad-hoc queries designed to represent real-world decision support systems.

### Alternatives for TPC-H Generation

This project provides two methods for generating TPC-H data:

1. **Main method** (in the `tpc-h` directory): Uses the official TPC-H dbgen tool and upload it to S3
2. **DuckDB-based method** (in the `tpc-h-alt` directory): An alternative approach that uses DuckDB's TPC-H extension for data generation, with tools to convert to CSV/Parquet and upload to Cloudflare R2 storage

## Trino x Iceberg x Nessie
### 1. Set environment variables

```bash
export S3_ACCESS_KEY=your_access_key
export S3_SECRET_KEY=your_secret_key
export S3_ENDPOINT=http://your-s3-endpoint
export WAREHOUSE_DIR=s3://your-bucket/warehouse
```

### 2. Start the infrastructure
In the project root directory, run:

```bash
docker-compose up -d
```
### 3. Check Trino UI and Nessie UI
Trino UI is deployed in `http://localhost:8082` and Nessie UI in `http://localhost:19120`


<div align="center">
  <sub>Built for the IF4044 Big Data Technology Course</sub>
</div>
