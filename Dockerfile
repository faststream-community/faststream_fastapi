ARG PYTHON_VERSION=3.11

FROM python:$PYTHON_VERSION
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

ENV PYTHONUNBUFFERED=1

RUN mkdir app
WORKDIR /app

RUN touch /app/.coverage

COPY ./LICENSE /app/LICENSE
COPY ./README.md /app/README.md
COPY ./pyproject.toml /app/pyproject.toml
COPY ./uv.lock /app/uv.lock

RUN mkdir /app/src
RUN mkdir /app/src/faststream_fastapi
RUN touch /app/src/faststream_fastapi/__init__.py

RUN uv sync --group testing

COPY ./src/ /app/src
COPY ./tests /app/tests
COPY ./docs /app/docs

ENV PATH="/app/src/.venv/bin:$PATH"
