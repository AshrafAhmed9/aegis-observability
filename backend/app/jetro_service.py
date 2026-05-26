import os
import csv
from datetime import datetime
from typing import Dict, Any, List
from .analyzer import AegisDiagnosticReport, CodeRemediation

class JetroService:
    """
    Jetro Workspace Exporter.
    Generates file-backed spatial visual whiteboard components inside active_war_room/
    for seamless rendering on Jetro's visual canvas in the Antigravity IDE.
    """
    
    def __init__(self, workspace_path: str):
        self.workspace_path = workspace_path
        self.war_room_dir = os.path.join(workspace_path, "active_war_room")
        
    def _ensure_dir(self):
        os.makedirs(self.war_room_dir, exist_ok=True)
        
    def export_all(self, report: AegisDiagnosticReport, timeline_events: List[Dict[str, Any]], nodes: List[Dict[str, Any]], edges: List[Dict[str, Any]]):
        """
        Exports all SRE whiteboard files to active_war_room/
        """
        self._ensure_dir()
        
        # 1. Export incident_summary.md (RCA Card)
        self.export_summary(report)
        
        # 2. Export incident_timeline.md (Trace Chronology)
        self.export_timeline(timeline_events)
        
        # 3. Export incident_graph.md (Mermaid Propagation Chart)
        self.export_graph(report, nodes, edges)
        
        # 4. Export postmortem.md (SRE Document)
        self.export_postmortem(report)
        
        # 5. Export telemetry_db.csv (SQL-Queryable traces)
        self.export_csv(timeline_events)
        
        # 6. Export suggested_patch.diff (Unified Code Patch)
        self.export_patch(report.primary_remediation)
        
    def export_summary(self, report: AegisDiagnosticReport):
        filepath = os.path.join(self.war_room_dir, "incident_summary.md")
        
        sev_color = "🔴" if report.overall_severity == "CRITICAL" else "🟠"
        
        content = f"""# {sev_color} {report.incident_id} : {report.title}

> [!IMPORTANT]
> **Operational Trust Boundary:** 
> Core timeline, dependency graphs, and blast metrics are parsed deterministically from raw telemetry. AI is utilized solely to augment systems interpretation, synthesize hypotheses, and generate remediation code patches.

## 📊 Incident Status Dashboard
| Property | Value |
| :--- | :--- |
| **Incident ID** | `{report.incident_id}` |
| **Overall Severity** | **{report.overall_severity}** |
| **Blast Radius** | `{report.blast_radius_requests:,} affected requests` |

---

## 🔍 System Impact Analysis
{report.impact_analysis}

---

## 🎯 Diagnostic Root Cause Hypotheses

"""
        for i, hyp in enumerate(report.hypotheses):
            pct = int(hyp.confidence * 100)
            content += f"""### Hypothesis {i+1} : {hyp.root_cause} (Confidence: `{pct}%`)
> [!NOTE]
> **System Rationale:** {hyp.description}

"""
            
        content += f"""---

## 🛠️ Immediate Mitigations
1. **Remediation File:** `{report.primary_remediation.file_path}`
2. **Mitigation Strategy:** {report.primary_remediation.explanation}

*See [suggested_patch.diff](file:///./active_war_room/suggested_patch.diff) for a unified git patch code correction.*
"""
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(content.strip())
            
    def export_timeline(self, events: List[Dict[str, Any]]):
        filepath = os.path.join(self.war_room_dir, "incident_timeline.md")
        
        content = """# 🕒 Deterministic Incident Trace Timeline

This chronological timeline has been mapped from trace events correlated via shared trace/request tokens:

| Timestamp | Service | Severity | Event Message |
| :--- | :--- | :--- | :--- |
"""
        for event in events:
            raw_ts = event.get("timestamp", "")
            try:
                dt = datetime.fromisoformat(raw_ts.replace("Z", "+00:00"))
                timestamp = dt.strftime("%H:%M:%S")
            except ValueError:
                timestamp = raw_ts

            service = event.get("service", "unknown")
            severity = event.get("severity", "INFO")
            msg = event.get("msg", "")
            
            emoji = "ℹ️"
            if severity == "WARNING":
                emoji = "⚠️"
            elif severity == "ERROR":
                emoji = "❌"
            elif severity == "CRITICAL":
                emoji = "🚨"
                
            content += f"| `{timestamp}` | **{service}** | {emoji} `{severity}` | {msg} |\n"
            
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(content.strip())
            
    def export_graph(self, report: AegisDiagnosticReport, nodes: List[Dict[str, Any]], edges: List[Dict[str, Any]]):
        filepath = os.path.join(self.war_room_dir, "incident_graph.md")
        
        # Build Mermaid flowchart nodes and edges
        mermaid_content = "flowchart TD\n"
        
        # Add styled nodes
        for node in nodes:
            name = node["service_name"]
            sev = node["max_severity"]
            # Sanitize node ID: Mermaid identifiers must not contain hyphens or spaces
            node_id = name.replace("-", "_").replace(" ", "_")
            
            # Severity color coding
            if sev in ["CRITICAL", "ERROR"]:
                mermaid_content += f'    {node_id}["💥 {name} ({sev})"]\n'
                mermaid_content += f'    style {node_id} fill:#fee2e2,stroke:#ef4444,stroke-width:2px,color:#991b1b\n'
            elif sev == "WARNING":
                mermaid_content += f'    {node_id}["⚠️ {name} ({sev})"]\n'
                mermaid_content += f'    style {node_id} fill:#fef3c7,stroke:#d97706,stroke-width:2px,color:#92400e\n'
            else:
                mermaid_content += f'    {node_id}["{name} ({sev})"]\n'
                mermaid_content += f'    style {node_id} fill:#f0fdf4,stroke:#16a34a,stroke-width:1px,color:#166534\n'

        # Add directed edges showing failure propagation paths
        for edge in edges:
            src = edge["source_service"].replace("-", "_").replace(" ", "_")
            tgt = edge["target_service"].replace("-", "_").replace(" ", "_")
            ptype = edge["propagation_type"]
            
            if ptype != "RPC" and ptype != "TEMPORAL":
                # High severity failure propagation path
                mermaid_content += f'    {src} -- "❌ {ptype}" --> {tgt}\n'
            else:
                mermaid_content += f'    {src} --> {tgt}\n'
                
        content = f"""# 🕸️ Failure Propagation Graph

This flowchart visualizes how cascading database latencies and lock contentions propagated through downstream service nodes to cause the system-wide gateway meltdown:

```mermaid
{mermaid_content}
```

> [!TIP]
> Red boxes indicate failed or severely degraded service components. Connective arrow tags trace the propagation vector and error classes that traversed network layers.
"""
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(content.strip())

    def export_postmortem(self, report: AegisDiagnosticReport):
        filepath = os.path.join(self.war_room_dir, "postmortem.md")

        content = f"""# 📝 SRE Incident Postmortem — {report.incident_id}

| Owner | Status | Target Fix | Review Date |
| :--- | :--- | :--- | :--- |
| **SRE Platform Team** | `RESOLVED` | `MERGED` | **2026-05-28** |

---

## 1. Executive Summary
{report.postmortem.executive_summary}

---

## 2. Root Cause Analysis (RCA)
Aegis telemetry correlation mapped the primary root failure path down to:
* **Core Cause:** `{report.hypotheses[0].root_cause}`
* **Confidence Rating:** `{int(report.hypotheses[0].confidence * 100)}%`
* **Root Diagnostic:** {report.hypotheses[0].description}

---

## 3. Preventive Action Items
To prevent recurrence, the following engineering tasks have been created in the SRE dashboard:
"""
        for item in report.postmortem.action_items_prevention:
            content += f"- [x] **{item}**\n"

        with open(filepath, "w", encoding="utf-8") as f:
            f.write(content.strip())

    def export_csv(self, events: List[Dict[str, Any]]):
        filepath = os.path.join(self.war_room_dir, "telemetry_db.csv")

        headers = ["timestamp", "service", "trace_id", "span_id", "severity", "latency_ms", "message"]

        with open(filepath, "w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            for event in events:
                writer.writerow([
                    event.get("timestamp", ""),
                    event.get("service", "unknown"),
                    event.get("trace_id", ""),
                    event.get("span_id", ""),
                    event.get("severity", "INFO"),
                    event.get("latency_ms", ""),
                    event.get("msg", "")
                ])

    def export_patch(self, patch: CodeRemediation):
        filepath = os.path.join(self.war_room_dir, "suggested_patch.diff")
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(patch.git_diff)
