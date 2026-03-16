"""Tests for find_public_attack_surface MCP tool."""

import json
import os
import uuid

from mcp.server.fastmcp import FastMCP

from src.mcp.graph_store import GraphStore
from src.mcp.tools.security import register

ORG_ID = str(uuid.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"))

EDGE_NS = uuid.uuid5(uuid.NAMESPACE_URL, "dsec:graph:edge")


def _edge_uuid(from_urn, to_urn, label):
    return str(uuid.uuid5(EDGE_NS, f"{from_urn}:{to_urn}:{label}"))


def _write_graph(tmp_path, nodes, edges):
    path = os.path.join(str(tmp_path), "graph.json")
    data = {
        "generated_at": "2024-01-01T00:00:00Z",
        "node_count": len(nodes),
        "edge_count": len(edges),
        "nodes": nodes,
        "edges": edges,
    }
    with open(path, "w") as f:
        json.dump(data, f)
    return path


def _build_full_chain_graph(tmp_path):
    """Build: public DNS -> internet-facing ALB -> BG -> ECS -> task def -> IAM role."""
    dns_urn = "urn:aws:route53:123::Z1/api.example.com/A"
    lb_urn = "urn:aws:elb:123:us-east-1:my-alb"
    bg_urn = "urn:aws:elb:123:us-east-1:my-alb/bg/my-tg"
    svc_urn = "urn:aws:ecs:123:us-east-1:prod/api"
    td_urn = "urn:aws:ecs:123:us-east-1:taskdef/api:5"
    role_urn = "urn:aws:iam:123::role/api-task-role"
    sg_urn = "urn:aws:vpc:123:us-east-1:myvpc/sg/sg-123"

    nodes = [
        {"urn": dns_urn, "organization_id": ORG_ID, "parent_urn": None,
         "node_type": "dns_record",
         "metadata": {"dns_record_name": "api.example.com", "dns_record_type": "A",
                       "dns_zone_private": False, "dns_values": ["my-alb-123.us-east-1.elb.amazonaws.com"]}},
        {"urn": lb_urn, "organization_id": ORG_ID, "parent_urn": None,
         "node_type": "load_balancer",
         "metadata": {"lb_type": "alb", "lb_scheme": "internet-facing",
                       "lb_dns_name": "my-alb-123.us-east-1.elb.amazonaws.com"}},
        {"urn": bg_urn, "organization_id": ORG_ID, "parent_urn": None,
         "node_type": "backend_group",
         "metadata": {"bg_name": "my-tg", "bg_backend_type": "aws_target_group"}},
        {"urn": svc_urn, "organization_id": ORG_ID, "parent_urn": None,
         "node_type": "ecs_service",
         "metadata": {"ecs_service_name": "api"}},
        {"urn": td_urn, "organization_id": ORG_ID, "parent_urn": None,
         "node_type": "ecs_task_definition",
         "metadata": {"ecs_task_family": "api", "ecs_task_revision": 5}},
        {"urn": role_urn, "organization_id": ORG_ID, "parent_urn": None,
         "node_type": "iam_role",
         "metadata": {"role_name": "api-task-role"}},
        {"urn": sg_urn, "organization_id": ORG_ID, "parent_urn": None,
         "node_type": "security_group",
         "metadata": {"sg_name": "api-sg", "sg_rules_ingress": [{"cidr": "0.0.0.0/0", "port": 443}]}},
    ]

    edges = [
        {"uuid": _edge_uuid(dns_urn, lb_urn, "resolves_to"), "from_urn": dns_urn, "to_urn": lb_urn,
         "edge_type": "resolves_to", "organization_id": ORG_ID, "metadata": {}},
        {"uuid": _edge_uuid(lb_urn, bg_urn, "routes_to"), "from_urn": lb_urn, "to_urn": bg_urn,
         "edge_type": "routes_to", "organization_id": ORG_ID, "metadata": {}},
        {"uuid": _edge_uuid(bg_urn, svc_urn, "routes_to"), "from_urn": bg_urn, "to_urn": svc_urn,
         "edge_type": "routes_to", "organization_id": ORG_ID, "metadata": {}},
        {"uuid": _edge_uuid(svc_urn, td_urn, "references"), "from_urn": svc_urn, "to_urn": td_urn,
         "edge_type": "references", "organization_id": ORG_ID, "metadata": {}},
        {"uuid": _edge_uuid(td_urn, role_urn, "assumes"), "from_urn": td_urn, "to_urn": role_urn,
         "edge_type": "assumes", "organization_id": ORG_ID, "metadata": {}},
        {"uuid": _edge_uuid(lb_urn, sg_urn, "protected_by"), "from_urn": lb_urn, "to_urn": sg_urn,
         "edge_type": "protected_by", "organization_id": ORG_ID, "metadata": {}},
    ]

    return _write_graph(tmp_path, nodes, edges)


class TestFindPublicAttackSurface:
    def test_full_chain(self, tmp_path):
        """Public DNS -> internet-facing ALB -> ECS -> IAM role: full chain reported."""
        path = _build_full_chain_graph(tmp_path)
        store = GraphStore(path)
        mcp = FastMCP("test")
        register(mcp, store)

        tool = mcp._tool_manager._tools.get("find_public_attack_surface")
        assert tool is not None

        result = tool.fn(include_internal=False)

        assert "api.example.com" in result
        assert "alb" in result
        assert "internet-facing" in result
        assert "api" in result  # ECS service name
        assert "api-task-role" in result  # IAM role
        assert "api-sg" in result  # SG name
        assert "PUBLIC" in result  # 0.0.0.0/0 ingress detected
        store.stop_watcher()

    def test_internal_dns_excluded(self, tmp_path):
        """Private DNS zone records are excluded by default."""
        nodes = [
            {"urn": "urn:aws:route53:123::Z2/internal.corp/A", "organization_id": ORG_ID,
             "parent_urn": None, "node_type": "dns_record",
             "metadata": {"dns_record_name": "internal.corp", "dns_record_type": "A",
                           "dns_zone_private": True, "dns_values": ["10.0.0.1"]}},
        ]
        path = _write_graph(tmp_path, nodes, [])
        store = GraphStore(path)
        mcp = FastMCP("test")
        register(mcp, store)

        tool = mcp._tool_manager._tools.get("find_public_attack_surface")
        result = tool.fn(include_internal=False)
        assert "No public attack surface found" in result
        store.stop_watcher()

    def test_internal_dns_included_when_flag_set(self, tmp_path):
        """Private DNS zone records are included when include_internal=True."""
        lb_urn = "urn:aws:elb:123:us-east-1:internal-alb"
        dns_urn = "urn:aws:route53:123::Z2/internal.corp/A"

        nodes = [
            {"urn": dns_urn, "organization_id": ORG_ID, "parent_urn": None,
             "node_type": "dns_record",
             "metadata": {"dns_record_name": "internal.corp", "dns_record_type": "A",
                           "dns_zone_private": True, "dns_values": ["internal.elb.amazonaws.com"]}},
            {"urn": lb_urn, "organization_id": ORG_ID, "parent_urn": None,
             "node_type": "load_balancer",
             "metadata": {"lb_type": "alb", "lb_scheme": "internet-facing",
                           "lb_dns_name": "internal.elb.amazonaws.com"}},
        ]
        edges = [
            {"uuid": _edge_uuid(dns_urn, lb_urn, "resolves_to"),
             "from_urn": dns_urn, "to_urn": lb_urn,
             "edge_type": "resolves_to", "organization_id": ORG_ID, "metadata": {}},
        ]
        path = _write_graph(tmp_path, nodes, edges)
        store = GraphStore(path)
        mcp = FastMCP("test")
        register(mcp, store)

        tool = mcp._tool_manager._tools.get("find_public_attack_surface")
        result = tool.fn(include_internal=True)
        assert "internal.corp" in result
        store.stop_watcher()

    def test_ecs_public_ip_direct_exposure(self, tmp_path):
        """ECS service with public IP reported as directly exposed."""
        nodes = [
            {"urn": "urn:aws:ecs:123:us-east-1:prod/worker", "organization_id": ORG_ID,
             "parent_urn": None, "node_type": "ecs_service",
             "metadata": {"ecs_service_name": "worker", "ecs_public_ip": True}},
        ]
        path = _write_graph(tmp_path, nodes, [])
        store = GraphStore(path)
        mcp = FastMCP("test")
        register(mcp, store)

        tool = mcp._tool_manager._tools.get("find_public_attack_surface")
        result = tool.fn(include_internal=False)
        assert "Directly exposed" in result
        assert "worker" in result
        store.stop_watcher()

    def test_no_public_dns(self, tmp_path):
        """No public DNS records returns empty result."""
        path = _write_graph(tmp_path, [], [])
        store = GraphStore(path)
        mcp = FastMCP("test")
        register(mcp, store)

        tool = mcp._tool_manager._tools.get("find_public_attack_surface")
        result = tool.fn(include_internal=False)
        assert "No public attack surface found" in result
        store.stop_watcher()
