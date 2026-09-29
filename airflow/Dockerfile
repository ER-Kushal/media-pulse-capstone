# Airflow + an isolated virtualenv holding the project's own dependencies (dbt, pandas, sklearn...)
# so they can never conflict with Airflow's pinned packages.
FROM apache/airflow:2.10.5-python3.12
USER airflow
COPY requirements.txt /tmp/project-requirements.txt
COPY api/requirements.txt /tmp/api/requirements.txt
RUN python -m venv /home/airflow/pipeline_venv \
 && /home/airflow/pipeline_venv/bin/pip install --no-cache-dir -r /tmp/project-requirements.txt
ENV PIPELINE_PYTHON=/home/airflow/pipeline_venv/bin/python
