FROM apache/spark-py:latest

USER root

# Install uv and other required tools
RUN pip install uv setuptools wheel

WORKDIR /opt/spark/work-dir

# Copy project files
COPY pyproject.toml .
COPY src/ src/
COPY main.py .
COPY data/ data/
COPY tests/ tests/

# Sync dependencies and install the project
RUN uv sync --all-groups
RUN uv pip install -e .[test]

# Activate the virtual environment and set it as default
ENV VIRTUAL_ENV=/opt/spark/work-dir/.venv
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

ENV PYTHONPATH=/opt/spark/work-dir

CMD ["python3", "main.py"]
