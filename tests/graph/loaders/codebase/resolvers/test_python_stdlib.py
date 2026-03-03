"""Unit tests for Python stdlib ingress/egress detection."""

import uuid

from src.graph.graph_models import NodeMetadataKey
from src.graph.loaders.codebase.filesystem_codebase_loader import FileSystemCodebaseLoader

ORG_ID = uuid.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
NK = NodeMetadataKey


def _load(tmp_path, filename, source):
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / filename).write_text(source)
    loader = FileSystemCodebaseLoader(organization_id=ORG_ID)
    nodes, _ = loader.load(str(repo))
    return nodes


def _find_func(nodes, name):
    return next(n for n in nodes if n.metadata.get(NK.FUNCTION_NAME) == name)


def test_os_environ_ingress_env(tmp_path):
    nodes = _load(tmp_path, "app.py", (
        "def read_config():\n"
        "    return os.environ['DB_HOST']\n"
    ))
    fn = _find_func(nodes, "read_config")
    assert fn.metadata[NK.IO_DIRECTION] == "ingress"
    assert fn.metadata[NK.IO_TYPE] == "env"


def test_os_getenv_ingress_env(tmp_path):
    nodes = _load(tmp_path, "app.py", (
        "def read_config():\n"
        "    return os.getenv('DB_HOST')\n"
    ))
    fn = _find_func(nodes, "read_config")
    assert fn.metadata[NK.IO_DIRECTION] == "ingress"
    assert fn.metadata[NK.IO_TYPE] == "env"


def test_sys_argv_ingress_cli(tmp_path):
    nodes = _load(tmp_path, "app.py", (
        "def main():\n"
        "    path = sys.argv[1]\n"
    ))
    fn = _find_func(nodes, "main")
    assert fn.metadata[NK.IO_DIRECTION] == "ingress"
    assert fn.metadata[NK.IO_TYPE] == "cli"


def test_sys_stdin_ingress_file(tmp_path):
    nodes = _load(tmp_path, "app.py", (
        "def read_input():\n"
        "    return sys.stdin.read()\n"
    ))
    fn = _find_func(nodes, "read_input")
    assert fn.metadata[NK.IO_DIRECTION] == "ingress"
    assert fn.metadata[NK.IO_TYPE] == "file"


def test_subprocess_run_egress(tmp_path):
    nodes = _load(tmp_path, "app.py", (
        "def execute():\n"
        "    subprocess.run(['ls', '-la'])\n"
    ))
    fn = _find_func(nodes, "execute")
    assert fn.metadata[NK.IO_DIRECTION] == "egress"
    assert fn.metadata[NK.IO_TYPE] == "subprocess"


def test_subprocess_popen_egress(tmp_path):
    nodes = _load(tmp_path, "app.py", (
        "def execute():\n"
        "    p = subprocess.Popen(['cmd'])\n"
    ))
    fn = _find_func(nodes, "execute")
    assert fn.metadata[NK.IO_DIRECTION] == "egress"
    assert fn.metadata[NK.IO_TYPE] == "subprocess"


def test_argparse_ingress_cli(tmp_path):
    nodes = _load(tmp_path, "app.py", (
        "def parse():\n"
        "    parser = ArgumentParser()\n"
        "    parser.add_argument('--verbose')\n"
        "    return parser.parse_args()\n"
    ))
    fn = _find_func(nodes, "parse")
    assert fn.metadata[NK.IO_DIRECTION] == "ingress"
    assert fn.metadata[NK.IO_TYPE] == "cli"


def test_socket_bind_ingress_network(tmp_path):
    nodes = _load(tmp_path, "app.py", (
        "def serve():\n"
        "    s = socket.socket()\n"
        "    s.bind(('0.0.0.0', 8080))\n"
        "    s.listen(5)\n"
    ))
    fn = _find_func(nodes, "serve")
    assert fn.metadata[NK.IO_DIRECTION] == "ingress"
    assert fn.metadata[NK.IO_TYPE] == "network"


def test_socket_connect_egress_network(tmp_path):
    nodes = _load(tmp_path, "app.py", (
        "def call_remote():\n"
        "    s = socket.socket()\n"
        "    s.connect(('api.example.com', 443))\n"
    ))
    fn = _find_func(nodes, "call_remote")
    assert fn.metadata[NK.IO_DIRECTION] == "egress"
    assert fn.metadata[NK.IO_TYPE] == "network"


def test_subprocess_call_egress(tmp_path):
    nodes = _load(tmp_path, "app.py", (
        "def run_cmd():\n"
        "    subprocess.call(['git', 'status'])\n"
    ))
    fn = _find_func(nodes, "run_cmd")
    assert fn.metadata[NK.IO_DIRECTION] == "egress"
    assert fn.metadata[NK.IO_TYPE] == "subprocess"


def test_plain_function_no_io(tmp_path):
    nodes = _load(tmp_path, "app.py", (
        "def add(a, b):\n"
        "    return a + b\n"
    ))
    fn = _find_func(nodes, "add")
    assert NK.IO_DIRECTION not in fn.metadata
    assert NK.IO_TYPE not in fn.metadata
