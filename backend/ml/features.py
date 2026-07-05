"""Shared feature extraction for training and runtime inference.

Used by both ml/ training scripts and app/ runtime detectors so the
features a model was trained on are computed by the exact same code at
inference time (train/serve-skew defense).

Deliberately contains NO service-identity features (no names, no
one-hots) — models must learn structural/telemetry patterns, not
memorize which service maps to which fault.
"""

from typing import Dict, List, Optional

METRICS = ("connection_pool_usage", "queue_depth", "latency_ms", "active_connections")

_STAT_NAMES = ("last", "mean", "std", "min", "max", "delta", "slope")
_GLOBAL_NAMES = ("error_count", "warning_count", "event_count", "event_rate")

FAILURE_FEATURES: List[str] = [
    f"{metric}__{stat}" for metric in METRICS for stat in _STAT_NAMES
] + list(_GLOBAL_NAMES)

RCA_FEATURES: List[str] = [
    "severity_rank",
    "error_count",
    "error_ratio",
    "first_error_order",
    "out_degree",
    "in_degree",
    "topo_depth",
    "latency_ratio",
    "has_deadlock_class",
    "has_timeout_class",
    "has_connection_class",
    "degraded_neighbor_fraction",
]

SEV_LEVELS = {"INFO": 0, "WARNING": 1, "ERROR": 2, "CRITICAL": 3}

def _slope(points):
    n = len(points)
    if n < 2:
        return 0.0
    x0 = points[0][0]
    xs = [p[0] - x0 for p in points]
    ys = [p[1] for p in points]
    mean_x = sum(xs) / n
    mean_y = sum(ys) / n
    sxx = sum((x - mean_x) ** 2 for x in xs)
    if sxx < 1e-9:
        return 0.0
    sxy = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys))
    return sxy / sxx

def window_features(events: List[dict], window_end: float,
                    window_seconds: float = 60.0) -> Optional[Dict[str, float]]:
    """Feature vector for ONE service's events in (window_end - W, window_end].

    Events must carry `_event_ts`. Returns None when the window is too
    sparse to be meaningful (< 3 events).
    """
    window_start = window_end - window_seconds
    in_window = [e for e in events
                 if e.get("_event_ts") is not None
                 and window_start < e["_event_ts"] <= window_end]
    if len(in_window) < 3:
        return None

    features: Dict[str, float] = {}
    for metric in METRICS:
        points = [(e["_event_ts"], float(e[metric]))
                  for e in in_window if e.get(metric) is not None]
        if points:
            values = [v for _, v in points]
            n = len(values)
            mean = sum(values) / n
            var = sum((v - mean) ** 2 for v in values) / n
            features[f"{metric}__last"] = values[-1]
            features[f"{metric}__mean"] = mean
            features[f"{metric}__std"] = var ** 0.5
            features[f"{metric}__min"] = min(values)
            features[f"{metric}__max"] = max(values)
            features[f"{metric}__delta"] = values[-1] - values[0]
            features[f"{metric}__slope"] = _slope(points)
        else:
            for stat in _STAT_NAMES:
                features[f"{metric}__{stat}"] = 0.0

    severities = [e.get("severity", "INFO") for e in in_window]
    features["error_count"] = float(sum(1 for s in severities if s in ("ERROR", "CRITICAL")))
    features["warning_count"] = float(sum(1 for s in severities if s == "WARNING"))
    features["event_count"] = float(len(in_window))
    features["event_rate"] = len(in_window) / window_seconds
    return features

def candidate_features(node: dict, nodes: List[dict], edges: List[dict],
                       topo_order: List[str]) -> Dict[str, float]:
    """Per-candidate-service features for the RCA ranker.

    Inputs are the same graph the deterministic engine sees — NOT its
    output roles, so the ranker isn't trained on the baseline's answer.
    """
    name = node["service_name"]
    degraded = {n["service_name"] for n in nodes
                if SEV_LEVELS.get(n["max_severity"], 0) >= 2}

    out_degree = sum(1 for e in edges if e["source_service"] == name)
    in_degree = sum(1 for e in edges if e["target_service"] == name)
    max_degree = max(1, max(
        (sum(1 for e in edges if e["source_service"] == n["service_name"])
         + sum(1 for e in edges if e["target_service"] == n["service_name"]))
        for n in nodes
    ))

    error_ts = sorted(
        (n["first_error_ts"] or "9999", n["service_name"])
        for n in nodes if n["service_name"] in degraded
    )
    order_lookup = {svc: i for i, (_, svc) in enumerate(error_ts)}
    first_error_order = (order_lookup.get(name, len(error_ts))
                         / max(1, len(error_ts) - 1)) if len(error_ts) > 1 else 0.0

    depth = (topo_order.index(name) / max(1, len(topo_order) - 1)
             if name in topo_order and len(topo_order) > 1 else 0.0)

    latencies = sorted(n["latency_ms"] for n in nodes)
    median_latency = latencies[len(latencies) // 2] if latencies else 0.0
    latency_ratio = (node["latency_ms"] / median_latency) if median_latency > 0 else 0.0

    err_text = " ".join(node.get("error_classes", [])).lower()

    neighbors = set()
    for e in edges:
        if e["source_service"] == name:
            neighbors.add(e["target_service"])
        elif e["target_service"] == name:
            neighbors.add(e["source_service"])
    degraded_neighbors = len(neighbors & degraded)

    return {
        "severity_rank": SEV_LEVELS.get(node["max_severity"], 0) / 3.0,
        "error_count": float(len(node.get("error_classes", []))),
        "error_ratio": (node.get("event_count", 0) and
                        len(node.get("error_classes", [])) / node["event_count"]) or 0.0,
        "first_error_order": first_error_order,
        "out_degree": out_degree / max_degree,
        "in_degree": in_degree / max_degree,
        "topo_depth": depth,
        "latency_ratio": min(latency_ratio, 100.0),
        "has_deadlock_class": 1.0 if "deadlock" in err_text else 0.0,
        "has_timeout_class": 1.0 if "timeout" in err_text else 0.0,
        "has_connection_class": 1.0 if ("connection" in err_text or "pool" in err_text
                                         or "exhaust" in err_text) else 0.0,
        "degraded_neighbor_fraction": (degraded_neighbors / len(neighbors)) if neighbors else 0.0,
    }

def incident_signature(nodes: List[dict], rca_result: dict,
                       incident_events: List[dict]) -> Optional[Dict[str, object]]:
    """Text signature summarizing an incident for similarity search.

    Used identically by the offline corpus builder (ml/build_incident_corpus.py)
    and the live pipeline (app/pipeline.py) — deliberately excludes raw
    timestamps so similarity reflects "what kind of incident", not "when".
    """
    degraded = [n for n in nodes if SEV_LEVELS.get(n["max_severity"], 0) >= 2]
    if not degraded:
        return None

    top = rca_result["ranked_root_causes"][0] if rca_result["ranked_root_causes"] else None
    fault_class = top["root_cause_class"] if top else "unknown"
    root_cause_service = top["service"] if top else None
    err_classes = sorted({c for n in degraded for c in n.get("error_classes", [])})
    messages = [e["msg"] for e in incident_events
                if e.get("severity") in ("ERROR", "CRITICAL") and e.get("msg")][:3]
    degraded_services = sorted(n["service_name"] for n in degraded)

    parts = [
        f"root cause class: {fault_class}",
        f"degraded services: {', '.join(degraded_services)}",
        f"error classes: {', '.join(err_classes)}",
    ]
    if messages:
        parts.append("messages: " + " | ".join(messages))

    return {
        "fault_class": fault_class,
        "root_cause_service": root_cause_service,
        "degraded_services": degraded_services,
        "signature": ". ".join(parts),
    }
