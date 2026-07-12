# ADR 0001: Package-by-Feature Modular Monolith

## Status
Accepted

## Context
MuleGraph requires a structured foundation that demonstrates production-ready patterns without the premature complexity of distributed microservices.

## Decision
We will build MuleGraph as a single Git repository containing a single Java 21 Spring Boot application. 
The codebase will use a package-by-feature modular monolith structure. 
Different runtime responsibilities (API, projections, stream processing) will be separated by Spring profiles rather than isolated repositories.

## Consequences
- Single build pipeline for all components.
- Simplified local development via unified Docker Compose.
- Future separation into independent microservices is possible but requires a new ADR.
