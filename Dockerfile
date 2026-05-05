FROM apache/airflow:2.9.1-python3.11

USER root
# Install system-level dependencies required for dbt and building packages
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    build-essential \
    git \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
# Install uv for dependency resolution
RUN pip install uv

# Copy requirements files into the container
COPY pyproject.toml uv.lock ./

# Install packages Airflow needs
RUN uv pip install --system \
    apache-airflow-providers-google \
    dbt-bigquery \
    kagglehub \
    pandas \
    pyarrow \
    google-cloud-bigquery \
    python-dotenv \
    papermill==2.6.0 \
    ipykernel==6.29.0