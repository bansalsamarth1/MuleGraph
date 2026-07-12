# Phase 1A Evidence: Spring Boot Foundation

Below is the verification output for the Spring Boot application endpoints in Phase 1A.

**Date:** 2026-07-12
**Command:** `./scripts/demo/phase-1a.sh`

```text
=== MuleGraph Phase 1A Verification ===
1. Running unit tests...
...
[INFO] Tests run: 1, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 1.913 s -- in com.mulegraph.MulegraphApplicationTests
[INFO] 
[INFO] Results:
[INFO] 
[INFO] Tests run: 1, Failures: 0, Errors: 0, Skipped: 0
[INFO] 
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
...

2. Starting application...
...
2026-07-13T02:24:19.462+05:30  INFO 8651 --- [mulegraph] [           main] o.s.b.a.e.web.EndpointLinksResolver      : Exposing 2 endpoints beneath base path '/actuator'
2026-07-13T02:24:19.493+05:30  INFO 8651 --- [mulegraph] [           main] o.s.b.w.embedded.tomcat.TomcatWebServer  : Tomcat started on port 8080 (http) with context path '/'
...
Waiting for application to become healthy...
3. Testing /actuator/health
Health OK
4. Testing /actuator/health/liveness
Liveness OK
5. Testing /actuator/health/readiness
Readiness OK
6. Stopping application...
=== End Verification ===
```
