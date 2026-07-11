ARG PYTHON_VERSION=3.10

FROM python:$PYTHON_VERSION
COPY --from=ghcr.io/astral-sh/uv:0.7.13 /uv /uvx /bin/

ENV PYTHONUNBUFFERED=1

RUN mkdir app 
WORKDIR app

COPY ./pyproject.toml /app/pyproject.toml
COPY ./README.md /app/README.md
COPY ./LICENSE /app/LICENSE
COPY ./uv.lock /app/uv.lock

RUN mkdir /app/src
RUN mkdir /app/src/faststream_fastapi
RUN touch /app/src/faststream_fastapi/__init__.py

RUN uv sync --group dev

COPY ./src/ /app/src
COPY ./tests /app/tests
COPY ./docs /app/docs

ENV PATH="/src/.venv/bin:$PATH"
