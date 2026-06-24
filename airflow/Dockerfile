FROM apache/airflow:2.10.3-python3.11

# psycopg2-binary: needed by the DAG to talk directly to the vajra Postgres.
# Pre-installing here so containers start instantly (vs _PIP_ADDITIONAL_REQUIREMENTS
# which reinstalls on every container start).
RUN pip install --no-cache-dir \
    psycopg2-binary==2.9.9 \
    boto3==1.35.36 \
    pandas==2.2.3 \
    pyarrow==17.0.0
