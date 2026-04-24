# Kafka Sandbox Consumer

This project demonstrates a local Kafka setup using Docker + Spring Boot consumer.

We use:
- Apache Kafka (Docker)
- Kafka UI (browser-based management)
- Spring Boot (Java 21)
- Spring Kafka

---

# 🧱 Architecture

- Kafka broker runs in Docker
- Kafka UI runs in Docker
- Spring Boot app runs on host machine
- Topics are auto-created via Spring Boot

---

# 🚀 Prerequisites

Make sure you have installed:

- Docker Desktop
- Java 21+
- Gradle (or use IDE Gradle wrapper)
- IntelliJ IDEA (optional)

---

# 🐳 1. Start Kafka + Kafka UI

Run containers:

```bash
docker compose up -d
```

Run spring application:
```bash
./gradlew bootRun
