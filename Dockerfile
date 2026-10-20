# ==============================================================================
# Custom Apache Airflow Image for Country Analytics
# Base: Official Apache Airflow with Python 3.12
# ==============================================================================

FROM apache/airflow:2.9.3-python3.12

# Copy project dependencies
COPY requirements.txt /requirements.txt

# Install dependencies into user environment as 'airflow' user (non-root)
USER airflow
RUN pip install --no-cache-dir -r /requirements.txt
