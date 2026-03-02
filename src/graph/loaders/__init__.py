from src.graph.loaders.codebase.filesystem_codebase_loader import FileSystemCodebaseLoader
from src.graph.loaders.codebase.git_codebase_loader import GitCodebaseLoader
from src.graph.loaders.loader import ConceptLoader
from src.graph.loaders.object_store.s3.loader import S3BucketLoader
from src.graph.loaders.postgres.onprem_postgres_loader import OnPremPostgresLoader

LOADER_REGISTRY: list[type[ConceptLoader]] = [
    OnPremPostgresLoader,
    S3BucketLoader,
    FileSystemCodebaseLoader,
    GitCodebaseLoader,
]
