# syntax=docker/dockerfile:1
FROM eclipse-temurin:25.0.5_8-jdk-noble@sha256:0939ed943f8107b943b6a6ac2e21d24568c24e4443d496e4d8b8e8da1ccdde1c AS build
WORKDIR /build
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl unzip \
    && rm -rf /var/lib/apt/lists/*

COPY pom.xml mvnw ./
COPY .mvn ./.mvn
RUN sed -i 's/\r$//' mvnw && chmod +x mvnw
RUN --mount=type=cache,target=/root/.m2,sharing=locked \
    ./mvnw -B -ntp -DskipTests dependency:go-offline
COPY src ./src
RUN --mount=type=cache,target=/root/.m2,sharing=locked \
    ./mvnw -B -ntp -Dmaven.test.skip=true package

FROM eclipse-temurin:25.0.5_8-jre-noble@sha256:39d8a64f84d4c0aab4639757e1502264da350be831df4bb25d182d6a514113c2
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 app \
    && useradd --uid 10001 --gid app --home-dir /app --no-create-home --shell /usr/sbin/nologin app \
    && mkdir -p /app/config \
    && chown app:app /app /app/config
WORKDIR /app
COPY --from=build --chown=10001:10001 /build/target/*.jar /app/app.jar
USER 10001:10001
EXPOSE 8081
HEALTHCHECK --interval=10s --timeout=5s --start-period=60s --retries=12 \
    CMD curl --fail --silent --show-error --max-time 4 http://localhost:8081/actuator/health || exit 1
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
