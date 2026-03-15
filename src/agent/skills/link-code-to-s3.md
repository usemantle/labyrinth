# Skill: Link Code to S3 Bucket

## When to use
When code in the codebase reads from or writes to an S3 bucket but no automated
edge exists between the code node and the S3 data node. The boto3 S3 plugin
tags functions with `aws_s3_operations` metadata but does not create edges to
specific bucket nodes because bucket names are often dynamic or configured at
runtime.

## Investigation steps

1. **Find code nodes with S3 operations**:
   ```
   search_nodes(metadata_key="aws_s3_operations")
   ```
   This returns functions tagged with S3 API calls (put_object, get_object, etc.).

2. **Find S3 bucket nodes** (if any exist in the graph):
   ```
   search_nodes(node_type="s3_bucket")
   ```

3. **Examine the code node's source** to determine which bucket it operates on:
   ```
   get_node_details(urn=<function_urn>)
   ```
   Look at metadata for `aws_s3_operations` (read/write/delete) and
   `aws_s3_operation_type` to understand the access pattern.

4. **Check if an edge already exists**:
   ```
   get_neighbors(urn=<function_urn>, direction="outgoing", edge_type="reads")
   get_neighbors(urn=<function_urn>, direction="outgoing", edge_type="writes")
   ```

5. **Determine the correct edge type** based on the operation:
   - `reads` — function calls get_object, download_file, list_objects, head_object
   - `writes` — function calls put_object, upload_file, copy_object
   - `references` — function references the bucket but access pattern is unclear

6. **Choose a confidence level:**
   - `VERY_HIGH` — bucket name is hardcoded in the source and matches the bucket node
   - `HIGH` — bucket name comes from a config file that is also in the codebase
   - `MEDIUM` — bucket name is passed as a parameter but contextual evidence (variable names, comments, IaC) suggests a match
   - `LOW` — circumstantial evidence only (e.g., naming convention)

7. **Create the soft link:**
   ```
   add_soft_link(
       from_urn=<function_urn>,
       to_urn=<s3_bucket_urn>,
       edge_type="writes",
       confidence="HIGH",
       note="upload_report() calls put_object with bucket name from config.REPORTS_BUCKET; Terraform defines this as 'acme-reports-prod'"
   )
   ```

8. **Mark evaluation complete:**
   After completing the investigation (whether or not you created a soft link),
   mark the node as evaluated so it is not re-investigated:
   ```
   update_node_metadata(
       urn=<function_urn>,
       metadata='{"<heuristic_name>_last_evaluated_at": "<current ISO timestamp>"}'
   )
   ```
   The `heuristic_name` is provided in the investigation prompt.

## Important
- Create **one edge per function-to-bucket relationship**, using the most specific edge type (reads, writes, or references).
- If a function both reads and writes to the same bucket, create two separate edges.
- Always include the evidence in the `note` field.
