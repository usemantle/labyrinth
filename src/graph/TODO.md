# Graph TODO

We need a typed system for nodes and edges that is interoperable and defines relationships more explicitly. The current relation types are too coarse:

- `CODE_TO_CODE` should distinguish between `CALLS_FUNCTION`, `INHERITS_FROM`, `IMPORTS`, etc.
- `CODE_TO_DATA` should distinguish between `READS_FROM`, `WRITES_TO`, `DELETES_FROM`, etc.
- `DATA_TO_DATA` should distinguish between `FOREIGN_KEY`, `REPLICATES_TO`, `DERIVED_FROM`, etc.
- `PRINCIPAL_TO_DATA` should distinguish between `CAN_READ`, `CAN_WRITE`, `CAN_DELETE`, `OWNS`, etc.

Node types should also be formalized into a typed hierarchy rather than relying on string classification and metadata key presence.
