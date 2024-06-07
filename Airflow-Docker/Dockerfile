# Use the official Apache Airflow image as the base image
FROM apache/airflow:2.9.1

# Switch to the airflow user
USER airflow

# Install the required package
RUN pip install supabase

# Switch back to the root user if needed
USER root

