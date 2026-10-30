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

# Optional positional form: make k6 cont 200 (or make k6-sync cont 200).
# Only these exact three goals are accepted, before any recipes can run.
K6_RUN_ENV :=
ifneq ($(filter cont,$(MAKECMDGOALS)),)
  ifneq ($(words $(MAKECMDGOALS)),3)
    $(error Usage: make k6 cont <requests-per-second> or make k6-sync cont <requests-per-second>)
  endif
  ifeq ($(filter k6 k6-sync,$(word 1,$(MAKECMDGOALS))),)
    $(error cont is only supported after k6 or k6-sync)
  endif
  ifneq ($(word 2,$(MAKECMDGOALS)),cont)
    $(error Usage: make k6 cont <requests-per-second> or make k6-sync cont <requests-per-second>)
  endif
  K6_RATE := $(word 3,$(MAKECMDGOALS))
  # Remove decimal digits without evaluating the argument in a shell.
  K6_RATE_REMAINDER := $(subst 0,,$(subst 1,,$(subst 2,,$(subst 3,,$(subst 4,,$(subst 5,,$(subst 6,,$(subst 7,,$(subst 8,,$(subst 9,,$(K6_RATE)))))))))))
  ifneq ($(strip $(K6_RATE_REMAINDER)),)
    $(error Request rate must be a positive integer)
  endif
  ifeq ($(subst 0,,$(K6_RATE)),)
    $(error Request rate must be a positive integer)
  endif
  override K6_RUN_ENV := CONTINUOUS=1 RATE=$(K6_RATE)
  .PHONY: cont $(K6_RATE)
  cont $(K6_RATE):
	@:
endif

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

k6: ## Async load: 10,000 requests; k6 cont <rate> runs until Ctrl+C
	MODE=async $(K6_RUN_ENV) bash load-tests/run-k6.sh

k6-sync: ## Direct sync load: 10,000 requests; k6-sync cont <rate> runs until Ctrl+C
	MODE=sync $(K6_RUN_ENV) bash load-tests/run-k6.sh

urls: ## Print the local URLs
	@echo "Registry health     http://localhost:8080/actuator/health"
	@echo "Compliance health   http://localhost:8081/actuator/health"
	@echo "RabbitMQ UI         http://localhost:15672  (guest / guest)"
	@echo "Debezium Connect    http://localhost:8083/connectors/registry-outbox/status"
	@echo "Grafana             http://localhost:3000/d/registration-case"

.PHONY: help check up down clean ps connector lag build test verify registry compliance register smoke k6 \
        k6-sync urls
