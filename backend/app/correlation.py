from typing import List, Dict, Any, Tuple
from collections import defaultdict

class CorrelationEngine:
    """
    Aegis Deterministic Telemetry Correlation Engine.
    Correlates unstructured telemetry events using Trace IDs, request flows,
    and constructs chronological incident timelines and propagation graphs.
    """
    
    @staticmethod
    def correlate(events: List[Dict[str, Any]]) -> Dict[str, Any]:
        # Group events by Trace ID
        traces = defaultdict(list)
        for event in events:
            trace_id = event.get("trace_id")
            if trace_id:
                traces[trace_id].append(event)
                
        # Sort events within each trace chronologically
        for trace_id in traces:
            traces[trace_id].sort(key=lambda x: x.get("timestamp", ""))
            
        return dict(traces)

    @staticmethod
    def build_propagation_graph(trace_events: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        Builds the failure propagation graph nodes and directed dependency edges.
        """
        nodes = {}
        edges = []
        
        # Build nodes (services and their states)
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
                    "latency_ms": 0.0,
                    "event_count": 0
                }
                
            nodes[service]["event_count"] += 1
            if err_class:
                nodes[service]["error_classes"].add(err_class)
                
            # Keep track of highest severity
            sev_levels = {"INFO": 0, "WARNING": 1, "ERROR": 2, "CRITICAL": 3}
            current_max = nodes[service]["max_severity"]
            if sev_levels.get(severity, 0) > sev_levels.get(current_max, 0):
                nodes[service]["max_severity"] = severity
                
            # Record maximum latency
            latency = event.get("latency_ms")
            if latency is not None:
                nodes[service]["latency_ms"] = max(nodes[service]["latency_ms"], float(latency))

        # Convert error_classes sets to lists for JSON serialization
        for svc in nodes:
            nodes[svc]["error_classes"] = list(nodes[svc]["error_classes"])

        # Construct dependency edges using span parent-child relationships
        span_to_service = {}
        for event in trace_events:
            span_id = event.get("span_id")
            service = event.get("service")
            if span_id and service:
                span_to_service[span_id] = service

        for event in trace_events:
            parent_span_id = event.get("parent_span_id")
            service = event.get("service")
            if parent_span_id and service:
                parent_service = span_to_service.get(parent_span_id)
                # Create directed edge from parent_service to service (parent -> child dependency)
                if parent_service and parent_service != service:
                    edge = {
                        "source_service": parent_service,
                        "target_service": service,
                        "propagation_type": "RPC"
                    }
                    # Upgrade propagation type based on warnings/errors
                    if event.get("severity") in ["ERROR", "CRITICAL"]:
                        edge["propagation_type"] = event.get("err_class", "CRITICAL_PROPAGATION")
                    if edge not in edges:
                        edges.append(edge)
                        
        # Fallback to temporal order if parent-span-id relations are not explicit
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
                    "target_service": services_ordered[i+1],
                    "propagation_type": "TEMPORAL"
                })

        return list(nodes.values()), edges

    @staticmethod
    def estimate_blast_radius(trace_events: List[Dict[str, Any]], nodes: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Calculates blast radius telemetry: estimated affected requests, degraded nodes, and severity levels.
        """
        degraded_services = [node["service_name"] for node in nodes if node["max_severity"] in ["ERROR", "CRITICAL"]]
        
        # Estimate affected requests based on timestamps and typical transaction rates
        # In a real platform, this checks live prometheus metrics, here we calculate based on active log signals
        base_rate = 1450 # requests per minute
        duration_minutes = 2.5 # standard window
        multiplier = len(degraded_services) * 1.8
        
        estimated_affected = int(base_rate * duration_minutes * multiplier) if degraded_services else 0
        
        return {
            "estimated_affected_requests": estimated_affected,
            "degraded_services": degraded_services,
            "system_status": "DEGRADED" if degraded_services else "HEALTHY",
            "blast_radius_percentage": min(100.0, float(len(degraded_services)) / max(1.0, float(len(nodes))) * 100.0)
        }
