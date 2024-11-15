# Dockerfile for customer_behaviour_pipeline

FROM python:3.10-slim

# Set up working directory
WORKDIR /app

# Install dependencies
COPY pyproject.toml poetry.lock /app/
RUN pip install poetry && poetry install --no-root

# Copy project files
COPY . /app

# Set entry point
CMD ["poetry", "run", "python"]
