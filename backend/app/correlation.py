from collections import defaultdict, deque
from typing import List, Dict, Any, Set


class CorrelationEngine:

    sev_levels = {"INFO": 0, "WARNING": 1, "ERROR": 2, "CRITICAL": 3}

    @staticmethod
    def build_propagation_graph(trace_events: List[Dict[str, Any]]):
        nodes = {}
        for event in trace_events:
            service = event.get("service")
            if not service:
                continue
            severity = event.get("severity", "INFO")
            err_class = event.get("err_class")
            if service not in nodes:
                nodes[service] = {
                    "service_name": service,
                    "max_severity": severity,
                    "error_classes": set(),
                    "first_error_ts": None,
                    "latency_ms": 0.0,
                    "event_count": 0
                }
            nodes[service]["event_count"] += 1
            if err_class:
                nodes[service]["error_classes"].add(err_class)
            current_max = nodes[service]["max_severity"]
            if CorrelationEngine.sev_levels.get(severity, 0) > CorrelationEngine.sev_levels.get(current_max, 0):
                nodes[service]["max_severity"] = severity
            if severity in ("ERROR", "CRITICAL") and nodes[service]["first_error_ts"] is None:
                nodes[service]["first_error_ts"] = event.get("timestamp", "")
            latency = event.get("latency_ms")
            if latency is not None:
                nodes[service]["latency_ms"] = max(nodes[service]["latency_ms"], float(latency))

        span_to_service = {}
        for event in trace_events:
            span_id = event.get("span_id")
            service = event.get("service")
            if span_id and service:
                span_to_service[span_id] = service

        edges = []
        for event in trace_events:
            parent_span_id = event.get("parent_span_id")
            service = event.get("service")
            if parent_span_id and service:
                parent_service = span_to_service.get(parent_span_id)
                if parent_service and parent_service != service:
                    edge = {
                        "source_service": parent_service,
                        "target_service": service,
                        "propagation_type": "RPC"
                    }
                    if event.get("severity") in ["ERROR", "CRITICAL"]:
                        edge["propagation_type"] = event.get("err_class", "CRITICAL_PROPAGATION")
                    if edge not in edges:
                        edges.append(edge)

        if not edges and len(nodes) > 1:
            services_ordered = []
            seen = set()
            for event in trace_events:
                svc = event.get("service")
                if svc and svc not in seen:
                    services_ordered.append(svc)
                    seen.add(svc)
            for i in range(len(services_ordered) - 1):
                edges.append({
                    "source_service": services_ordered[i],
                    "target_service": services_ordered[i + 1],
                    "propagation_type": "TEMPORAL"
                })

        for service in nodes:
            nodes[service]["error_classes"] = list(nodes[service]["error_classes"])

        return list(nodes.values()), edges

    @staticmethod
    def estimate_blast_radius(trace_events: List[Dict[str, Any]], nodes: List[Dict[str, Any]]) -> Dict[str, Any]:
        degraded_services = [node["service_name"] for node in nodes if node["max_severity"] in ["ERROR", "CRITICAL"]]
        base_rate = 1450
        duration_minutes = 2.5
        multiplier = len(degraded_services) * 1.8
        estimated_affected = int(base_rate * duration_minutes * multiplier) if degraded_services else 0
        return {
            "estimated_affected_requests": estimated_affected,
            "degraded_services": degraded_services,
            "system_status": "DEGRADED" if degraded_services else "HEALTHY",
            "blast_radius_percentage": min(100.0, float(len(degraded_services)) / max(1.0, float(len(nodes))) * 100.0)
        }

    @staticmethod
    def classify_root_cause(nodes: List[Dict[str, Any]], edges: List[Dict[str, Any]]) -> Dict[str, Any]:
        sev = CorrelationEngine.sev_levels
        nodes_by_name = {n["service_name"]: n for n in nodes}
        all_services = set(nodes_by_name.keys())

        degraded = {n["service_name"] for n in nodes if sev.get(n["max_severity"], 0) >= 2}

        if not degraded:
            return {
                "roles": {},
                "ranked_root_causes": [],
                "cycle": {"detected": False, "members": [], "kind": None},
                "topo_order": sorted(all_services),
                "edge_basis": "NONE",
            }

        temporal_only = bool(edges) and all(e["propagation_type"] == "TEMPORAL" for e in edges)
        edge_basis = "TEMPORAL" if temporal_only else ("SPAN" if edges else "NONE")

        # Build caller/callee adjacency
        callees_of = {s: set() for s in all_services}
        callers_of = {s: set() for s in all_services}
        for e in edges:
            caller, callee = e["source_service"], e["target_service"]
            if caller in all_services and callee in all_services and caller != callee:
                callees_of[caller].add(callee)
                callers_of[callee].add(caller)

        # Check which degraded nodes are connected to other degraded nodes via edges
        def degraded_neighbors(service):
            connected = callees_of[service] | callers_of[service]
            return connected & degraded

        # Find degraded nodes with NO edge to any other degraded node = disconnected
        disconnected_degraded = {s for s in degraded if not degraded_neighbors(s)}

        # If all degraded nodes are disconnected (no edges between them),
        # use earliest-error-time ordering: earliest = root, rest = symptoms
        if disconnected_degraded == degraded and len(degraded) > 1:
            edge_basis = "EVENT_TIME"
            degraded_by_time = sorted(
                degraded,
                key=lambda s: nodes_by_name[s].get("first_error_ts") or "9999"
            )
            earliest = degraded_by_time[0]

            roles = {}
            for s in degraded:
                if s == earliest:
                    roles[s] = "ROOT_CAUSE"
                else:
                    roles[s] = "SYMPTOM"

            def classify_error(error_classes):
                joined = " ".join(error_classes).lower()
                if "deadlock" in joined:
                    return "deadlock"
                if "connection" in joined or "exhausted" in joined or "pool" in joined:
                    return "resource_exhaustion"
                if "timeout" in joined:
                    return "timeout"
                return "failure"

            n = nodes_by_name[earliest]
            ranked = [{
                "service": earliest,
                "root_cause_class": classify_error(n.get("error_classes", [])),
                "score": round(0.7 * 0.85, 3),
                "max_severity": n["max_severity"],
                "error_classes": n.get("error_classes", []),
                "causal_depth_rank": 0,
            }]

            return {
                "roles": roles,
                "ranked_root_causes": ranked,
                "cycle": {"detected": False, "members": [], "kind": None},
                "topo_order": degraded_by_time,
                "edge_basis": edge_basis,
            }

        # Standard path: edges exist between degraded nodes
        causal_indeg = {s: len(callees_of[s]) for s in all_services}

        queue = deque(sorted(s for s, d in causal_indeg.items() if d == 0))
        topo_order = []
        while queue:
            node = queue.popleft()
            topo_order.append(node)
            for neighbor in sorted(callers_of[node]):
                causal_indeg[neighbor] -= 1
                if causal_indeg[neighbor] == 0:
                    queue.append(neighbor)
            queue = deque(sorted(queue))

        residual = {s for s, d in causal_indeg.items() if d > 0}

        deadlock_nodes = set()
        for n in nodes:
            err_classes = n.get("error_classes", [])
            if any("deadlock" in (ec or "").lower() for ec in err_classes):
                deadlock_nodes.add(n["service_name"])

        cycle_members = residual | deadlock_nodes

        def has_degraded_dependency(service, visited=None):
            if visited is None:
                visited = set()
            visited.add(service)
            for callee in callees_of.get(service, set()):
                if callee in visited:
                    continue
                if callee in degraded and callee not in cycle_members:
                    return True
                if has_degraded_dependency(callee, visited):
                    return True
            return False

        roles = {}
        for s in degraded:
            if s in cycle_members:
                roles[s] = "CYCLE_MEMBER"
            elif has_degraded_dependency(s):
                roles[s] = "SYMPTOM"
            else:
                roles[s] = "ROOT_CAUSE"

        depth_rank = {s: i for i, s in enumerate(topo_order)}
        max_depth = max(depth_rank.values()) if depth_rank else 1
        roots = [s for s, r in roles.items() if r in ("ROOT_CAUSE", "CYCLE_MEMBER")]

        def classify_error(error_classes):
            joined = " ".join(error_classes).lower()
            if "deadlock" in joined:
                return "deadlock"
            if "connection" in joined or "exhausted" in joined or "pool" in joined:
                return "resource_exhaustion"
            if "timeout" in joined:
                return "timeout"
            return "failure"

        ranked = []
        for s in roots:
            n = nodes_by_name[s]
            depth_term = 1.0 - (depth_rank.get(s, 0) / max(max_depth, 1))
            sev_term = sev.get(n["max_severity"], 0) / 3.0
            err_term = min(1.0, len(n.get("error_classes", [])) / 3.0)
            cycle_term = 1.0 if s in cycle_members else 0.0
            score = 0.40 * depth_term + 0.30 * sev_term + 0.20 * err_term + 0.10 * cycle_term
            if temporal_only:
                score *= 0.7
            ranked.append({
                "service": s,
                "root_cause_class": classify_error(n.get("error_classes", [])),
                "score": round(score, 3),
                "max_severity": n["max_severity"],
                "error_classes": n.get("error_classes", []),
                "causal_depth_rank": depth_rank.get(s, -1),
            })

        ranked.sort(key=lambda d: (-d["score"], d["service"]))

        cycle_kind = None
        if deadlock_nodes:
            cycle_kind = "deadlock"
        elif residual:
            cycle_kind = "service_cycle"

        return {
            "roles": roles,
            "ranked_root_causes": ranked,
            "cycle": {
                "detected": bool(cycle_members),
                "members": sorted(cycle_members),
                "kind": cycle_kind,
            },
            "topo_order": topo_order,
            "edge_basis": edge_basis,
        }
