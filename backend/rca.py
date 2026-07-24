"""
Given a sealed trace (events from possibly several services), determines
which service failed first and caused the others to fail.

Two steps:
1. Build a graph: one node per service, one edge (cause -> effect) whenever
   an event's parent_span_id points at another service's event.
2. Topologically sort that graph with Kahn's algorithm: repeatedly remove
   nodes with no incoming edges. Whatever comes out first has nothing
   causing it -- that's the root cause.
"""

SEVERITY_RANK = {"INFO": 0, "WARNING": 1, "ERROR": 2, "CRITICAL": 3}

ERROR_TYPE_KEYWORDS = {
    "deadlock": "deadlock",
    "connectionerror": "resource_exhaustion",
    "workerlost": "resource_exhaustion",
    "timeout": "timeout",
}


def _is_degraded(node):
    return SEVERITY_RANK[node["max_severity"]] >= SEVERITY_RANK["ERROR"]


def build_graph(events):
    """Returns (nodes, edges).
    nodes: service -> {max_severity, error_classes, first_error_time}
    edges: set of (cause_service, effect_service) pairs
    """
    nodes = {}
    for event in events:
        service = event["service"]
        if service not in nodes:
            nodes[service] = {"max_severity": "INFO", "error_classes": set(), "first_error_time": None}
        _update_node(nodes[service], event)

    span_to_service = {event["span_id"]: event["service"] for event in events}
    edges = _build_edges(events, span_to_service)

    return nodes, edges


def _update_node(node, event):
    severity = event["severity"]
    if SEVERITY_RANK[severity] > SEVERITY_RANK[node["max_severity"]]:
        node["max_severity"] = severity
    if event.get("error_class"):
        node["error_classes"].add(event["error_class"])
    if severity in ("ERROR", "CRITICAL") and node["first_error_time"] is None:
        node["first_error_time"] = event["timestamp"]


def _build_edges(events, span_to_service):
    edges = set()
    for event in events:
        parent_span_id = event.get("parent_span_id")
        if not parent_span_id or parent_span_id not in span_to_service:
            continue
        cause_service = span_to_service[parent_span_id]
        effect_service = event["service"]
        if cause_service != effect_service:
            edges.add((cause_service, effect_service))
    return edges


def rank_root_causes(nodes, edges):
    """Ranks failed services from most-likely-root-cause to most-likely-symptom."""
    degraded = [service for service, node in nodes.items() if _is_degraded(node)]

    if len(degraded) <= 1:
        return degraded

    relevant_edges = [(cause, effect) for cause, effect in edges if cause in degraded and effect in degraded]

    if not relevant_edges:
        # No causal link between the failures -- best guess is whichever failed first.
        return sorted(degraded, key=lambda service: nodes[service]["first_error_time"] or float("inf"))

    return _kahn_topological_order(degraded, relevant_edges)


def _kahn_topological_order(degraded, relevant_edges):
    in_degree = {service: 0 for service in degraded}
    effects_of = {service: [] for service in degraded}
    for cause, effect in relevant_edges:
        in_degree[effect] += 1
        effects_of[cause].append(effect)

    ordered = []
    queue = sorted(service for service in degraded if in_degree[service] == 0)
    while queue:
        queue.sort()  # deterministic pick when several nodes are ready at once
        current = queue.pop(0)
        ordered.append(current)
        for effect in effects_of[current]:
            in_degree[effect] -= 1
            if in_degree[effect] == 0:
                queue.append(effect)

    # A node that never reaches in-degree 0 is in a cycle (A caused B, B
    # caused A) -- order between cycle members is undecidable, so they're
    # all treated as root causes.
    leftover = sorted(service for service in degraded if service not in ordered)
    return leftover + ordered


def classify_error_type(error_classes):
    combined_text = " ".join(error_classes).lower()
    for keyword, error_type in ERROR_TYPE_KEYWORDS.items():
        if keyword in combined_text:
            return error_type
    return "failure"


def analyze(events):
    nodes, edges = build_graph(events)
    ranked_root_causes = rank_root_causes(nodes, edges)
    degraded_services = [service for service, node in nodes.items() if _is_degraded(node)]

    return {
        "nodes": nodes,
        "edges": edges,
        "ranked_root_causes": ranked_root_causes,
        "degraded_services": degraded_services,
    }
