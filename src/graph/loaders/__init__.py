from src.graph.loaders.aws.loader import AwsAccountLoader
from src.graph.loaders.codebase.filesystem_codebase_loader import FileSystemCodebaseLoader
from src.graph.loaders.codebase.git_codebase_loader import GitCodebaseLoader
from src.graph.loaders.loader import ConceptLoader
from src.graph.loaders.postgres.onprem_postgres_loader import OnPremPostgresLoader

LOADER_REGISTRY: list[type[ConceptLoader]] = [
    OnPremPostgresLoader,
    AwsAccountLoader,
    FileSystemCodebaseLoader,
    GitCodebaseLoader,
]
