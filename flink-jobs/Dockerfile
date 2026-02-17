# Multi-stage Dockerfile for building and deploying the Flink TransactionPipeline job.
# Stage 1: Build the uber JAR with Maven
# Stage 2: Copy JAR into Flink 1.20 image for job submission

# --- Stage 1: Build ---
FROM maven:3.9-eclipse-temurin-17 AS builder

WORKDIR /build

# Copy pom.xml first for dependency caching
COPY pom.xml .
COPY .mvn .mvn
COPY mvnw .

# Download dependencies (cached if pom.xml unchanged)
RUN mvn dependency:go-offline -B

# Copy source and build uber JAR
COPY src src
RUN mvn clean package -DskipTests -B -q

# --- Stage 2: Flink runtime ---
FROM flink:1.20-java17

# Copy the uber JAR to Flink's usrlib directory (auto-loaded on submission)
COPY --from=builder /build/target/pipeline-1.0-SNAPSHOT.jar /opt/flink/usrlib/pipeline.jar

# Hadoop config needed by Iceberg REST catalog
ENV HADOOP_CLASSPATH=""
