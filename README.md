# Real-Time Event Analytics Platform

Streams live Wikimedia edit events through Kafka into PostgreSQL, with aggregation and analytics on top.

## Architecture
Wikimedia SSE → Spring Boot producer → Kafka → Spring Boot consumer → PostgreSQL

## Run locally
    cd infra
    docker compose up -d

## Status
Work in progress.