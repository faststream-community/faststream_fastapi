# Cross-platform shell configuration
# Use PowerShell on Windows (higher precedence than shell setting)
set windows-shell := ["powershell.exe", "-NoLogo", "-Command"]
# Use sh on Unix-like systems
set shell := ["sh", "-c"]


[doc("All command information")]
default:
  @just --list --unsorted --list-heading $'faststream_fastapi  commands…\n'

# Infra
[doc("Init infra")]
[group("infra")]
init python="3.10":
  uv sync --group dev -p {{python}}

[doc("Build container")]
[group("infra")]
build python="3.10":
  docker build . --build-arg PYTHON_VERSION={{python}}

[doc("Run all containers")]
[group("infra")]
up:
  docker compose up -d  --remove-orphans

[doc("Stop all containers")]
[group("infra")]
stop:
  docker compose stop

[doc("Down all containers")]
[group("infra")]
down:
  docker compose down

[doc("Run fast tests")]
[group("tests")]
test +param="tests/": up
  docker compose exec faststream_fastapi uv run pytest {{param}} -m "not slow and not connected" -n auto

[doc("Run all tests")]
[group("tests")]
test-all +param="tests/": up
  docker compose exec faststream_fastapi uv run pytest {{param}} -m "all" -n auto

[doc("Run fast tests with coverage")]
[group("tests")]
test-coverage +param="tests/": up
  -docker compose exec faststream_fastapi uv run sh -c "pytest {{param}} -m 'not slow and not connected' -n auto --cov --cov-report=term-missing"

[doc("Run all tests with coverage")]
[group("tests")]
test-coverage-all +param="tests/": up
  -docker compose exec faststream_fastapi uv run sh -c "pytest {{param}} -m 'all' -n auto --cov --cov-report=term-missing"


# Docs
[doc("Build docs")]
[group("docs")]
docs-build params="":
  uv run zensical build {{params}}

[doc("Serve docs")]
[group("docs")]
docs-serve:
  uv run zensical serve -a 127.0.0.1:8000


# Linter
_linter *params:
  uv run --no-dev --group linters --frozen {{params}}

[doc("Ruff format")]
[group("linter")]
ruff-format *params:
  just _linter ruff format {{params}}

[doc("Ruff check")]
[group("linter")]
ruff-check *params:
  just _linter ruff check --exit-non-zero-on-fix {{params}}

_codespell:
  just _linter codespell -L Dependant,dependant --skip "./site"

[doc("Check typos")]
[group("linter")]
typos: _codespell
  just _linter typos

alias lint := linter

[doc("Linter run")]
[group("linter")]
linter: ruff-format ruff-check _codespell


# Static analysis
_static *params:
  uv run --frozen {{params}}

[doc("Mypy check")]
[group("static analysis")]
mypy *params:
  just _static mypy {{params}}

[doc("Bandit check")]
[group("static analysis")]
bandit:
  just _static bandit -c pyproject.toml -r src/faststream_fastapi

[doc("Semgrep check")]
[group("static analysis")]
semgrep:
  just _static semgrep scan --config auto --error --skip-unknown-extensions src/faststream_fastapi

[doc("Zizmor check")]
[group("static analysis")]
zizmor:
  just _static zizmor .

[doc("Static analysis check")]
[group("static analysis")]
static-analysis: mypy bandit semgrep



# Kafka
[doc("Run kafka container")]
[group("kafka")]
kafka-up:
  docker compose up -d kafka

[doc("Stop kafka container")]
[group("kafka")]
kafka-stop:
  docker compose stop kafka

[doc("Show kafka logs")]
[group("kafka")]
kafka-logs:
  docker compose logs -f kafka

[doc("Run kafka memory tests")]
[group("kafka")]
[group("tests")]
test-kafka +param="tests/":
  docker compose exec faststream_fastapi uv run pytest {{param}} -m "kafka and not connected and not slow" -n auto

[doc("Run kafka all tests")]
[group("kafka")]
[group("tests")]
test-kafka-all +param="tests/":
  docker compose exec faststream_fastapi uv run pytest {{param}} -m "kafka or (kafka and slow)" -n auto

[doc("Run confluent memory tests")]
[group("kafka")]
[group("tests")]
test-confluent +param="tests/":
  docker compose exec faststream_fastapi uv run pytest {{param}} -m "confluent and not connected and not slow" -n auto

[doc("Run confluent all tests")]
[group("confluent")]
[group("tests")]
test-confluent-all +param="tests/":
  docker compose exec faststream_fastapi uv run pytest {{param}} -m "confluent or (confluent and slow)" -n auto


# RabbitMQ
[doc("Run rabbitmq container")]
[group("rabbitmq")]
rabbit-up:
  docker compose up -d rabbitmq

[doc("Stop rabbitmq container")]
[group("rabbitmq")]
rabbit-stop:
  docker compose stop rabbitmq

[doc("Show rabbitmq logs")]
[group("rabbitmq")]
rabbit-logs:
  docker compose logs -f rabbitmq

[doc("Run rabbitmq memory tests")]
[group("rabbitmq")]
[group("tests")]
test-rabbit +param="tests/":
  docker compose exec faststream_fastapi uv run pytest {{param}} -m "rabbit and not connected and not slow" -n auto

[doc("Run rabbitmq all tests")]
[group("rabbitmq")]
[group("tests")]
test-rabbit-all +param="tests/":
  docker compose exec faststream_fastapi uv run pytest {{param}} -m "rabbit or (rabbit and slow)" -n auto


# Redis
[doc("Run redis container")]
[group("redis")]
redis-up:
  docker compose up -d redis

[doc("Stop redis container")]
[group("redis")]
redis-stop:
  docker compose stop redis

[doc("Show redis logs")]
[group("redis")]
redis-logs:
  docker compose logs -f redis

[doc("Run redis memory tests")]
[group("redis")]
[group("tests")]
test-redis +param="tests/":
  docker compose exec faststream_fastapi uv run pytest {{param}} -m "redis and not connected and not slow" -n auto

[doc("Run redis all tests")]
[group("redis")]
[group("tests")]
test-redis-all +param="tests/":
  docker compose exec faststream_fastapi uv run pytest {{param}} -m "redis or (redis and slow)" -n auto


# Redis Cluster
[doc("Run redis-cluster container")]
[group("redis-cluster")]
redis-cluster-up:
  docker compose up -d redis-cluster

[doc("Stop redis-cluster container")]
[group("redis-cluster")]
redis-cluster-stop:
  docker compose stop redis-cluster

[doc("Show redis-cluster logs")]
[group("redis-cluster")]
redis-cluster-logs:
  docker compose logs -f redis-cluster

[doc("Run redis-cluster fast tests")]
[group("redis-cluster")]
[group("tests")]
test-redis-cluster +param="tests/":
  docker compose exec faststream_fastapi uv run pytest {{param}} -m "redis_cluster and not connected and not slow" -n auto

[doc("Run redis-cluster all tests")]
[group("redis-cluster")]
[group("tests")]
test-redis-cluster-all +param="tests/":
  docker compose exec faststream_fastapi uv run pytest {{param}} -m "redis_cluster or (redis_cluster and slow)" -n auto


# Nats
[doc("Run nats container")]
[group("nats")]
nats-up:
  docker compose up -d nats

[doc("Stop nats container")]
[group("nats")]
nats-stop:
  docker compose stop nats

[doc("Show nats logs")]
[group("nats")]
nats-logs:
  docker compose logs -f nats

[doc("Run nats memory tests")]
[group("nats")]
[group("tests")]
test-nats +param="tests/":
  docker compose exec faststream_fastapi uv run pytest {{param}} -m "nats and not connected and not slow" -n auto

[doc("Run nats all tests")]
[group("nats")]
[group("tests")]
test-nats-all +param="tests/":
  docker compose exec faststream_fastapi uv run pytest {{param}} -m "nats or (nats and slow)" -n auto
