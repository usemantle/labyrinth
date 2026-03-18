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


def register_loader(loader_cls: type[ConceptLoader]) -> None:
    """Register an external loader class so the scanner can dispatch to it."""
    LOADER_REGISTRY.append(loader_cls)
