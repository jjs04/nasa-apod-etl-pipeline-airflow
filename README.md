# NASA APOD ETL Pipeline using Apache Airflow
## Project Overview
This project implements an automated ETL (Extract, Transform, Load) pipeline using Apache Airflow to ingest data from NASA’s Astronomy Picture of the Day (APOD) API and store it in a PostgreSQL database.

The pipeline is scheduled to run daily and is designed to be reliable, idempotent, and production-ready with retry logic and conflict handling.
