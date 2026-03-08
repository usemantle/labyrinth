from src.graph.graph_models import Edge, Node
from src.graph.sinks import Sink


class SqliteSink(Sink):
    """Write graph data to a JSON file."""

    def __init__(self, _: str):
        # TODO: pass db identifier
        pass

    def write(self, nodes: list[Node], edges: list[Edge]) -> None:
        pass
