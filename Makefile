# Front door for the registration case. Maven (each module's mvnw) is still the build tool and
# Docker Compose still owns the infrastructure; this file only gives the commands short names.
#
# Works with GNU make on macOS, Linux and Windows (winget install ezwinports.make; Git for Windows
# provides the bash the recipes run in, from cmd, PowerShell or Git Bash alike).
# Run `make` or `make help` for the list.

ifeq ($(OS),Windows_NT)
  # In cmd/PowerShell "bash" resolves to the WSL stub in System32, so point at Git Bash explicitly
  # (8.3 short path: GNU make does not like spaces in SHELL). Falls back to bash on PATH otherwise.
  GIT_BASH := $(wildcard C:/Program\ Files/Git/bin/bash.exe)
  SHELL := $(if $(GIT_BASH),C:/PROGRA~1/Git/bin/bash.exe,bash)
else
  SHELL := bash
endif
.SHELLFLAGS := -eu -o pipefail -c
.DEFAULT_GOAL := help

COMPOSE        := docker compose -f infra/compose.yml
REGISTRY_MVN   := user_registry/mvnw -f user_registry/pom.xml
COMPLIANCE_MVN := compliance/mvnw -f compliance/pom.xml
COUNT          ?= 50
KAFKA_CLI      := MSYS_NO_PATHCONV=1 $(COMPOSE) exec kafka /opt/kafka/bin

help: ## Show this list
	@awk 'BEGIN {FS = ":.*## "} /^[a-zA-Z0-9_-]+:.*## / {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

check: ## Verify Java 25 and Docker are available
	@java -version 2>&1 | head -n 1
	@java -version 2>&1 | grep -q '"25' || { echo "Java 25 is required (JAVA_HOME points elsewhere)"; exit 1; }
	@docker info > /dev/null 2>&1 && echo "docker: ok" || { echo "Docker is not running"; exit 1; }

# ---- infrastructure --------------------------------------------------------------------------

up: ## Start PostgreSQL, RabbitMQ, Kafka, Debezium, Prometheus, Grafana and run the init containers
	$(COMPOSE) up -d --wait
	$(COMPOSE) --profile init up --force-recreate kafka-init connect-init

down: ## Stop the infrastructure, keep the data volumes
	$(COMPOSE) down

clean: ## Stop the infrastructure and wipe all data volumes
	$(COMPOSE) down -v

ps: ## Show container status
	$(COMPOSE) ps

connector: ## Show the Debezium connector status
	@curl -s localhost:8083/connectors/registry-outbox/status; echo

lag: ## Show the Compliance consumer-group lag per partition
	$(KAFKA_CLI)/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group compliance

# ---- build and test ---------------------------------------------------------------------------

build: ## Package both services without running tests
	$(REGISTRY_MVN) -q -DskipTests package
	$(COMPLIANCE_MVN) -q -DskipTests package

test: ## Run all tests of both services (needs Docker for Testcontainers)
	$(REGISTRY_MVN) test
	$(COMPLIANCE_MVN) test

verify: ## Full Maven verify of both services
	$(REGISTRY_MVN) verify
	$(COMPLIANCE_MVN) verify

# ---- run the services (each blocks its terminal) ---------------------------------------------

registry: ## Run the Registry service on port 8080
	$(REGISTRY_MVN) spring-boot:run

compliance: ## Run the Compliance service on port 8081
	$(COMPLIANCE_MVN) spring-boot:run

# ---- try it -----------------------------------------------------------------------------------

register: ## Submit one registration with a fresh Idempotency-Key
	@key=$$(printf '%08x-%04x-4%03x-%04x-%012x' $$((RANDOM*RANDOM)) $$RANDOM $$((RANDOM%4096)) $$RANDOM $$((RANDOM*RANDOM*RANDOM))); \
	curl -i -X POST localhost:8080/registrations -H 'Content-Type: application/json' -H "Idempotency-Key: $$key" \
	  -d "{\"fullName\":\"Ada Lovelace\",\"email\":\"ada-$$key@example.com\",\"countryCode\":\"NL\"}"; echo

smoke: ## End-to-end check: COUNT registrations through every stage (default COUNT=50)
	COUNT=$(COUNT) infra/smoke-test.sh

k6: ## Fire the k6 burst only (10,000 by default; ITERATIONS and VUS override)
	load-tests/run-k6.sh

k6-sync: ## Load the synchronous commit endpoint (requires the updated Registry running)
	MODE=sync bash load-tests/run-k6.sh

sync-experiment-a: ## Synchronous experiment: 1 processing permit, DB pool 4
	bash load-tests/sync-experiment.sh S1 1

sync-experiment-b: ## Synchronous experiment: 4 processing permits, DB pool 4
	bash load-tests/sync-experiment.sh S4 4

sync-experiment-c: ## Synchronous experiment: 16 processing permits, DB pool 4
	bash load-tests/sync-experiment.sh S16 16

sync-experiments: sync-experiment-a sync-experiment-b sync-experiment-c ## Compare bounded synchronous parallelism

experiment-a: ## Experiment A: 1 consumer, 10,000 registrations (stop any running Registry first)
	load-tests/experiment.sh A 1

experiment-b: ## Experiment B: 4 consumers
	load-tests/experiment.sh B 4

experiment-c: ## Experiment C: 16 consumers, pool still 4
	load-tests/experiment.sh C 16

experiment-d: ## Experiment D (optional): 4 consumers on platform threads
	load-tests/experiment.sh D 4 platform

experiments: experiment-a experiment-b experiment-c ## Run A, B and C back to back

urls: ## Print the local URLs
	@echo "Registry health     http://localhost:8080/actuator/health"
	@echo "Compliance health   http://localhost:8081/actuator/health"
	@echo "RabbitMQ UI         http://localhost:15672  (guest / guest)"
	@echo "Debezium Connect    http://localhost:8083/connectors/registry-outbox/status"
	@echo "Grafana             http://localhost:3000/d/registration-case"

.PHONY: help check up down clean ps connector lag build test verify registry compliance register smoke k6 \
        k6-sync sync-experiment-a sync-experiment-b sync-experiment-c sync-experiments \
        experiment-a experiment-b experiment-c experiment-d experiments urls
