# Use an official Python runtime as a parent image
FROM python:3.10-slim as builder

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1
# Poetry specific env vars
ENV POETRY_NO_INTERACTION=1 \
    POETRY_VIRTUALENVS_CREATE=false \
    POETRY_CACHE_DIR='/var/cache/pypoetry' \
    POETRY_VERSION=1.8.3

# Install poetry
RUN pip install "poetry==${POETRY_VERSION}"

# Set work directory
WORKDIR /app

# Copy only dependency definition files to leverage Docker cache
COPY poetry.lock pyproject.toml ./

# Install dependencies using poetry export for smaller final image
# --only main excludes dev dependencies
RUN poetry install --no-root --only main

# --- Final Stage ---
FROM python:3.10-slim

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

WORKDIR /app

# Copy installed dependencies from builder stage
# Need to copy the actual installed packages, typically in site-packages
# Find the site-packages directory (this might vary slightly based on python version/base image)
# Example: Assume site-packages is /usr/local/lib/python3.10/site-packages
COPY --from=builder /usr/local/lib/python3.10/site-packages /usr/local/lib/python3.10/site-packages
# Copy the path env if needed (less common with slim images)
# COPY --from=builder /usr/local/bin /usr/local/bin

# Copy the application source code
COPY src/ ./src/

# Expose port if/when HTTP transport is implemented
# EXPOSE 8000

# Command to run the application
# Ensure the user running the container has DATABRICKS_HOST/TOKEN env vars set
CMD ["python", "-m", "src.databricks_mcp"] 