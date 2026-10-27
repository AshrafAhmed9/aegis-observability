"""
Root-cause analysis: given all the events of one request (a "trace"), work out
which service broke first and caused the others to fail.

How it works, in two steps:
1. Draw a graph. Each service is a dot. If an event says "I only happened
   because that other service's event happened" (its parent_span_id), draw an
   arrow from the cause to the effect.
2. Find the dot with no arrows pointing into it. Nothing caused it, so it is
   the root cause. Kahn's algorithm does this by repeatedly taking out dots
   that have no incoming arrows.
"""

# Higher number = worse. Used to compare severities.
SEVERITY_RANK = {"INFO": 0, "WARNING": 1, "ERROR": 2, "CRITICAL": 3}


def _is_degraded(node):
    """A service counts as failed if its worst event was ERROR or CRITICAL."""
    return SEVERITY_RANK[node["max_severity"]] >= SEVERITY_RANK["ERROR"]


def build_graph(events):
    """Returns (nodes, edges).
    nodes: service name -> {max_severity, first_error_time}
    edges: set of (cause_service, effect_service) pairs
    """
    nodes = {}
    for event in events:
        service = event["service"]
        if service not in nodes:
            nodes[service] = {"max_severity": "INFO", "first_error_time": None}
        _update_node(nodes[service], event)

    # A parent_span_id is just an id like "span-14", so first build a lookup
    # from span id to the service that owns it.
    span_to_service = {event["span_id"]: event["service"] for event in events}
    edges = _build_edges(events, span_to_service)

    return nodes, edges


def _update_node(node, event):
    """Folds one event into its service's summary."""
    severity = event["severity"]
    if SEVERITY_RANK[severity] > SEVERITY_RANK[node["max_severity"]]:
        node["max_severity"] = severity
    if severity in ("ERROR", "CRITICAL") and node["first_error_time"] is None:
        node["first_error_time"] = event["timestamp"]


def _build_edges(events, span_to_service):
    """One arrow (cause -> effect) for every event whose parent belongs to a
    different service."""
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
    """Lists the failed services, most likely root cause first."""
    degraded = [service for service, node in nodes.items() if _is_degraded(node)]

    if len(degraded) <= 1:
        return degraded

    # Only arrows between two failed services help explain the failure.
    relevant_edges = [(cause, effect) for cause, effect in edges if cause in degraded and effect in degraded]

    if not relevant_edges:
        # No link between the failures, so the best guess is whoever failed first.
        return sorted(degraded, key=lambda service: nodes[service]["first_error_time"] or float("inf"))

    return _kahn_topological_order(degraded, relevant_edges)


def _kahn_topological_order(degraded, relevant_edges):
    """Kahn's algorithm. "in_degree" is how many arrows point into a service.
    Services with 0 come out first, because nothing caused them."""
    in_degree = {service: 0 for service in degraded}
    effects_of = {service: [] for service in degraded}
    for cause, effect in relevant_edges:
        in_degree[effect] += 1
        effects_of[cause].append(effect)

    ordered = []
    queue = sorted(service for service in degraded if in_degree[service] == 0)
    while queue:
        queue.sort()  # alphabetical, so ties always resolve the same way
        current = queue.pop(0)
        ordered.append(current)
        # Remove this service's arrows. Anything left with none pointing at
        # it is now free to come out.
        for effect in effects_of[current]:
            in_degree[effect] -= 1
            if in_degree[effect] == 0:
                queue.append(effect)

    # Services that never reached 0 are stuck in a loop (A caused B and B
    # caused A). There is no way to say which came first, so all of them are
    # reported as root causes.
    leftover = sorted(service for service in degraded if service not in ordered)
    return leftover + ordered


def analyze(events):
    """Main entry point. An empty ranked_root_causes list means nothing
    failed in this trace."""
    nodes, edges = build_graph(events)
    return {
        "nodes": nodes,
        "edges": edges,
        "ranked_root_causes": rank_root_causes(nodes, edges),
    }
