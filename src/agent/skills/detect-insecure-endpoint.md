# Skill: Detect Insecure Endpoint

## When to use
When an HTTP endpoint has no detected authentication scheme. The goal is to
assess whether the endpoint is vulnerable to path traversal, IDOR, injection,
or other attacks, and whether it is exposed to the internet.

## Investigation steps

1. **Inspect the endpoint:**
   ```
   get_node_details(urn=<endpoint_function_urn>)
   ```
   Note the `http_method`, `route_path`, and whether the route accepts user
   input (path parameters, query parameters, request body).

2. **Read the source code:**
   Use `Read` to examine the function's implementation. Look for:
   - Path traversal risks (user input used in file paths without sanitization)
   - IDOR vulnerabilities (user input used to look up resources without authorization checks)
   - Injection risks (user input passed to database queries, system commands, or template engines)
   - Missing input validation or sanitization

3. **Check deployment exposure:**
   Trace the codebase to see if it is deployed and internet-facing:
   - Codebase → Dockerfile (file with `dockerfile_base_images` metadata)
   - Dockerfile → ECR image repository (`builds` edge)
   - ECR → ECS task definition (`references` edge)
   - ECS task definition → ECS service → ECS cluster
   - Check security groups and NACLs for public ingress rules

4. **Assess blast radius:**
   ```
   blast_radius(urn=<endpoint_function_urn>)
   ```
   Check what data stores, tables, or other services the endpoint can reach.

## Risk assessment

Assign a risk level based on the combination of factors:

- **CRITICAL** — Endpoint accepts user input in path/query + no auth + confirmed
  vulnerability (path traversal, IDOR, injection) + deployed to internet-facing service
- **HIGH** — Confirmed vulnerability pattern but deployment status unknown, or
  deployed but vulnerability is limited in scope
- **MEDIUM** — Suspicious patterns but no confirmed vulnerability, or vulnerability
  present but endpoint is not deployed
- **LOW** — No significant vulnerability patterns detected, or endpoint is internal-only

## Output

After completing the investigation, call `update_node_metadata` with:
```
update_node_metadata(
    urn=<endpoint_function_urn>,
    metadata='{"insecure_endpoint_last_evaluated_at": "<ISO timestamp>", "insecure_endpoint_risk": "<CRITICAL|HIGH|MEDIUM|LOW>", "insecure_endpoint_finding": "<brief description of finding>"}'
)
```

Always set `insecure_endpoint_last_evaluated_at` even if no vulnerability is found.
