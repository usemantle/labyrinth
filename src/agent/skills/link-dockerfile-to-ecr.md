# Skill: Link Dockerfile to ECR Repository

## When to use
When a Dockerfile in the codebase should be linked to an ECR image repository
but automated stitching did not create the `builds` edge. This happens when
there are no OCI labels on the image and the name heuristic cannot match the
codebase name to the ECR repository name.

## Investigation steps

1. **Find Dockerfile nodes** in the graph:
   ```
   search_nodes(node_type="file", metadata_key="dockerfile_base_images")
   ```

2. **Find ECR image repositories**:
   ```
   search_nodes(node_type="image_repository")
   ```

3. **Check if a `builds` edge already exists** between them:
   ```
   get_neighbors(urn=<dockerfile_urn>, direction="outgoing", edge_type="builds")
   ```

4. **Determine the correct match.** Gather evidence by examining:
   - The Dockerfile's parent codebase (`get_neighbors(urn=<dockerfile_urn>, direction="incoming", edge_type="contains")`)
   - The ECR repository name and the codebase name — do they refer to the same application?
   - ECS task definitions that reference the ECR repository (`get_neighbors(urn=<ecr_urn>, direction="incoming", edge_type="references")`) — this shows what services consume the image
   - Any IaC files (Terraform, CDK) in the codebase that define the ECR repository

5. **Choose a confidence level:**
   - `VERY_HIGH` — IaC in the same repo defines the ECR repository, or OCI labels confirm it
   - `HIGH` — ECS service name matches the codebase name and there is only one Dockerfile
   - `MEDIUM` — naming patterns suggest a match but no hard evidence
   - `LOW` — weak or circumstantial evidence only

6. **Create the soft link:**
   ```
   add_soft_link(
       from_urn=<dockerfile_file_urn>,
       to_urn=<ecr_repository_urn>,
       edge_type="builds",
       confidence="HIGH",
       note="Dockerfile in testapp codebase builds the labyrinth-test-app ECR image; confirmed via ECS task definition referencing this ECR repo"
   )
   ```

7. **Mark evaluation complete:**
   After completing the investigation (whether or not you created a soft link),
   mark the node as evaluated so it is not re-investigated:
   ```
   update_node_metadata(
       urn=<dockerfile_file_urn>,
       metadata='{"<heuristic_name>_last_evaluated_at": "<current ISO timestamp>"}'
   )
   ```
   The `heuristic_name` is provided in the investigation prompt.

## Important
- The `from_urn` must be the **file** node (the Dockerfile), not the codebase node.
- The `to_urn` must be the **image_repository** node, not an individual image node.
- Always include a `note` explaining your evidence so the link can be reviewed later.
