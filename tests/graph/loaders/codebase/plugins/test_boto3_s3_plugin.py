"""
Unit tests for the Boto3 S3 codebase plugin.

Verifies detection of S3 client creation and operation tagging.
"""

import uuid

from src.graph.graph_models import (
    NodeMetadataKey,
)
from src.graph.loaders.codebase.filesystem_codebase_loader import FileSystemCodebaseLoader
from src.graph.loaders.codebase.plugins import Boto3S3Plugin

ORG_ID = uuid.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")

NK = NodeMetadataKey


def _make_s3_loader(*plugins):
    return FileSystemCodebaseLoader(
        organization_id=ORG_ID,
        plugins=list(plugins) if plugins else [Boto3S3Plugin()],
    )


def test_boto3_s3_class_with_client(tmp_path):
    """Class containing boto3.client('s3') gets AWS_S3_CLIENT tag."""
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "storage.py").write_text(
        'class Storage:\n'
        '    def __init__(self):\n'
        '        self.client = boto3.client("s3")\n'
    )

    nodes, _ = _make_s3_loader().load(str(repo))
    cls = next(n for n in nodes if n.metadata.get(NK.CLASS_NAME) == "Storage")
    assert cls.metadata[NK.AWS_S3_CLIENT] is True


def test_boto3_s3_class_with_resource(tmp_path):
    """Class containing boto3.resource('s3') gets AWS_S3_CLIENT tag."""
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "storage.py").write_text(
        "class Storage:\n"
        "    def __init__(self):\n"
        "        self.s3 = boto3.resource('s3')\n"
    )

    nodes, _ = _make_s3_loader().load(str(repo))
    cls = next(n for n in nodes if n.metadata.get(NK.CLASS_NAME) == "Storage")
    assert cls.metadata[NK.AWS_S3_CLIENT] is True


def test_boto3_s3_class_with_session_client(tmp_path):
    """Class containing session.client('s3') gets AWS_S3_CLIENT tag."""
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "storage.py").write_text(
        'class Storage:\n'
        '    def __init__(self, session):\n'
        '        self.client = session.client("s3")\n'
    )

    nodes, _ = _make_s3_loader().load(str(repo))
    cls = next(n for n in nodes if n.metadata.get(NK.CLASS_NAME) == "Storage")
    assert cls.metadata[NK.AWS_S3_CLIENT] is True


def test_boto3_s3_function_put_object(tmp_path):
    """Function with .put_object( gets write operation tag."""
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "upload.py").write_text(
        'def upload(client, data):\n'
        '    client.put_object(Bucket="b", Key="k", Body=data)\n'
    )

    nodes, _ = _make_s3_loader().load(str(repo))
    fn = next(n for n in nodes if n.metadata.get(NK.FUNCTION_NAME) == "upload")
    assert fn.metadata[NK.AWS_S3_OPERATIONS] == "put_object"
    assert fn.metadata[NK.AWS_S3_OPERATION_TYPE] == "write"


def test_boto3_s3_function_get_object(tmp_path):
    """Function with .get_object( gets read operation tag."""
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "download.py").write_text(
        'def download(client):\n'
        '    return client.get_object(Bucket="b", Key="k")\n'
    )

    nodes, _ = _make_s3_loader().load(str(repo))
    fn = next(n for n in nodes if n.metadata.get(NK.FUNCTION_NAME) == "download")
    assert fn.metadata[NK.AWS_S3_OPERATIONS] == "get_object"
    assert fn.metadata[NK.AWS_S3_OPERATION_TYPE] == "read"


def test_boto3_s3_function_delete_object(tmp_path):
    """Function with .delete_object( gets delete operation tag."""
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "cleanup.py").write_text(
        'def cleanup(client):\n'
        '    client.delete_object(Bucket="b", Key="k")\n'
    )

    nodes, _ = _make_s3_loader().load(str(repo))
    fn = next(n for n in nodes if n.metadata.get(NK.FUNCTION_NAME) == "cleanup")
    assert fn.metadata[NK.AWS_S3_OPERATIONS] == "delete_object"
    assert fn.metadata[NK.AWS_S3_OPERATION_TYPE] == "delete"


def test_boto3_s3_function_mixed_ops(tmp_path):
    """Function with get + put gets both operations sorted."""
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "sync.py").write_text(
        'def sync(client):\n'
        '    data = client.get_object(Bucket="b", Key="k")\n'
        '    client.put_object(Bucket="b2", Key="k2", Body=data)\n'
    )

    nodes, _ = _make_s3_loader().load(str(repo))
    fn = next(n for n in nodes if n.metadata.get(NK.FUNCTION_NAME) == "sync")
    assert fn.metadata[NK.AWS_S3_OPERATIONS] == "get_object,put_object"
    assert fn.metadata[NK.AWS_S3_OPERATION_TYPE] == "read,write"


def test_boto3_s3_function_paginator(tmp_path):
    """Function with get_paginator('list_objects_v2') gets read tag."""
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "lister.py").write_text(
        'def list_keys(client):\n'
        '    paginator = client.get_paginator("list_objects_v2")\n'
        '    for page in paginator.paginate(Bucket="b"):\n'
        '        yield page\n'
    )

    nodes, _ = _make_s3_loader().load(str(repo))
    fn = next(n for n in nodes if n.metadata.get(NK.FUNCTION_NAME) == "list_keys")
    assert "list_objects_v2" in fn.metadata[NK.AWS_S3_OPERATIONS]
    assert fn.metadata[NK.AWS_S3_OPERATION_TYPE] == "read"


def test_boto3_s3_function_upload_download(tmp_path):
    """Function with upload_file + download_file gets read,write type."""
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "transfer.py").write_text(
        'def transfer(client):\n'
        '    client.upload_file("/tmp/a", "bucket", "key")\n'
        '    client.download_file("bucket", "key", "/tmp/b")\n'
    )

    nodes, _ = _make_s3_loader().load(str(repo))
    fn = next(n for n in nodes if n.metadata.get(NK.FUNCTION_NAME) == "transfer")
    assert fn.metadata[NK.AWS_S3_OPERATIONS] == "download_file,upload_file"
    assert fn.metadata[NK.AWS_S3_OPERATION_TYPE] == "read,write"


def test_boto3_s3_function_client_and_ops(tmp_path):
    """Function creating client AND calling ops gets both tags."""
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "inline.py").write_text(
        'def store():\n'
        '    client = boto3.client("s3")\n'
        '    client.put_object(Bucket="b", Key="k", Body="data")\n'
    )

    nodes, _ = _make_s3_loader().load(str(repo))
    fn = next(n for n in nodes if n.metadata.get(NK.FUNCTION_NAME) == "store")
    assert fn.metadata[NK.AWS_S3_CLIENT] is True
    assert fn.metadata[NK.AWS_S3_OPERATIONS] == "put_object"
    assert fn.metadata[NK.AWS_S3_OPERATION_TYPE] == "write"


def test_boto3_s3_class_methods_tagged_independently(tmp_path):
    """Each method in a class is tagged independently."""
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "multi.py").write_text(
        'class Store:\n'
        '    def __init__(self):\n'
        '        self.client = boto3.client("s3")\n'
        '\n'
        '    def save(self):\n'
        '        self.client.put_object(Bucket="b", Key="k", Body="x")\n'
        '\n'
        '    def process(self):\n'
        '        return 42\n'
    )

    nodes, _ = _make_s3_loader().load(str(repo))

    init = next(n for n in nodes if n.metadata.get(NK.FUNCTION_NAME) == "__init__")
    assert init.metadata[NK.AWS_S3_CLIENT] is True
    assert NK.AWS_S3_OPERATIONS not in init.metadata

    save = next(n for n in nodes if n.metadata.get(NK.FUNCTION_NAME) == "save")
    assert save.metadata[NK.AWS_S3_OPERATIONS] == "put_object"

    process = next(n for n in nodes if n.metadata.get(NK.FUNCTION_NAME) == "process")
    assert NK.AWS_S3_CLIENT not in process.metadata
    assert NK.AWS_S3_OPERATIONS not in process.metadata


def test_boto3_s3_ignores_non_s3_service(tmp_path):
    """boto3.client('sqs') should not trigger S3 tagging."""
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "queue.py").write_text(
        'class QueueClient:\n'
        '    def __init__(self):\n'
        '        self.client = boto3.client("sqs")\n'
    )

    nodes, _ = _make_s3_loader().load(str(repo))
    cls = next(n for n in nodes if n.metadata.get(NK.CLASS_NAME) == "QueueClient")
    assert NK.AWS_S3_CLIENT not in cls.metadata


def test_boto3_s3_ignores_javascript(tmp_path):
    """S3-like patterns in JavaScript should not be tagged."""
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "storage.js").write_text(
        'class Storage {\n'
        '    constructor() {\n'
        '        this.client = new S3Client({});\n'
        '        // boto3.client("s3") lookalike\n'
        '    }\n'
        '}\n'
    )

    nodes, _ = _make_s3_loader().load(str(repo))
    cls = next(n for n in nodes if n.metadata.get(NK.CLASS_NAME) == "Storage")
    assert NK.AWS_S3_CLIENT not in cls.metadata


def test_boto3_s3_no_tags_on_plain_class(tmp_path):
    """A class with no S3 patterns gets no S3 tags."""
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "plain.py").write_text(
        'class PlainService:\n'
        '    def run(self):\n'
        '        return True\n'
    )

    nodes, _ = _make_s3_loader().load(str(repo))
    cls = next(n for n in nodes if n.metadata.get(NK.CLASS_NAME) == "PlainService")
    assert NK.AWS_S3_CLIENT not in cls.metadata
    assert NK.AWS_S3_OPERATIONS not in cls.metadata
