# Skill: Link Dockerfile to Entrypoint Code File

## When to use
When a Dockerfile has an ENTRYPOINT or CMD instruction but automated stitching
could not resolve it to a concrete code file. This happens when the entrypoint
references a shell script, uses variable substitution, or the file path doesn't
match any indexed file in the codebase.

## Investigation steps

1. **Read the Dockerfile content** using the source URN:
   ```
   get_node_details(urn=<dockerfile_urn>)
   ```
   Look at the `dockerfile_entrypoint` and `dockerfile_cmd` metadata fields.

2. **Identify the entrypoint target.** Parse the ENTRYPOINT/CMD to determine
   what file it ultimately runs:
   - If it's a shell script (e.g., `/docker-entrypoint.sh`), search for that
     script in the codebase and read it to find what application it launches.
   - If it uses variable substitution (e.g., `${APP_MODULE}`), check for
     environment variables or `.env` files that define the value.
   - If it's a Python module notation (e.g., `app.main:app`), convert to a
     file path (`app/main.py`).

3. **Find the target file** in the codebase:
   ```
   search_nodes(query=<filename>, node_type="file")
   ```

4. **Verify the match** by checking:
   - Does the file exist in the same codebase as the Dockerfile?
   - Is the file path consistent with the WORKDIR and COPY instructions?
   - Does the file contain a runnable entry point (e.g., `if __name__`,
     `app = FastAPI()`, `createServer`)?

5. **Choose a confidence level:**
   - `VERY_HIGH` — direct file reference that exists in the codebase
   - `HIGH` — shell script wrapper that clearly launches a specific file
   - `MEDIUM` — variable substitution resolved via environment config
   - `LOW` — indirect evidence or multiple possible targets

## Important
- The `from_urn` must be the **Dockerfile** file node.
- The `to_urn` must be the **target code file** node.
- Use `edge_type="executes"` when creating the soft link.
- Always include a `note` explaining your evidence so the link can be reviewed.
