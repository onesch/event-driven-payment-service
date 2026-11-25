FROM python:3.12-alpine

WORKDIR /app

RUN pip install --no-cache-dir uv

COPY pyproject.toml uv.lock ./

COPY . .

ENV UV_PROJECT_ENVIRONMENT=/opt/venv

RUN uv sync --frozen
