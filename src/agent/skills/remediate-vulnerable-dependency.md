# Skill: Remediate Vulnerable Dependency

## When to use
When a dependency node has `cve_ids` metadata, indicating known vulnerabilities.
The goal is to assess the impact, find a fixed version, and document findings.

## Investigation steps

1. **Inspect the dependency:**
   ```
   get_node_details(urn=<dependency_urn>)
   ```
   Note the `package_name`, `package_version`, `package_ecosystem`, and `cve_ids`.

2. **Look up CVE details:**
   Use the OSV API to get vulnerability details:
   ```
   Bash: curl -s "https://api.osv.dev/v1/query" -d '{"package":{"name":"<package_name>","ecosystem":"<ecosystem>"},"version":"<version>"}'
   ```
   Note: ecosystem should be "PyPI" for Python, "npm" for JavaScript, "Go" for Go, etc.

3. **Determine the fixed version:**
   From the OSV response, find the `fixed` version in the `affected[].ranges[].events[]`
   array. This is the minimum version that patches the vulnerability.

4. **Assess blast radius:**
   ```
   blast_radius(urn=<dependency_urn>)
   ```
   Identify what code, endpoints, and services depend on this package. Pay special
   attention to:
   - HTTP endpoints that import the vulnerable package
   - Functions that use the vulnerable functionality
   - Services that are internet-facing

5. **Check for breaking changes:**
   Use `Read` and `Grep` to examine how the package is used in the codebase.
   Determine if upgrading to the fixed version would introduce breaking changes.

## Output

After completing the investigation, call `update_node_metadata` with:
```
update_node_metadata(
    urn=<dependency_urn>,
    metadata='{"vulnerable_dependency_last_evaluated_at": "<ISO timestamp>", "vulnerable_dependency_fixed_version": "<version or null>", "vulnerable_dependency_finding": "<brief description>"}'
)
```

Always set `vulnerable_dependency_last_evaluated_at` even if no fix is available.
