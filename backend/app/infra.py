import os
import socket
import time

import requests
from prometheus_client.parser import text_string_to_metric_families

KAFKA_ADDR = os.environ.get("AEGIS_KAFKA_ADDR", "localhost:9092")
CONSUMER_METRICS_URL = os.environ.get("AEGIS_CONSUMER_METRICS_URL", "http://localhost:9095/metrics")
PROMETHEUS_URL = os.environ.get("AEGIS_PROMETHEUS_URL", "http://localhost:9091")
GRAFANA_URL = os.environ.get("AEGIS_GRAFANA_URL", "http://localhost:3000")

PROBE_TIMEOUT = 1.0
CACHE_TTL = 5.0

_cache: dict = {}


def _cached(key, fn):
    now = time.monotonic()
    hit = _cache.get(key)
    if hit is not None and (now - hit[0]) < CACHE_TTL:
        return hit[1]
    value = fn()
    _cache[key] = (now, value)
    return value


def _probe_tcp(addr: str) -> bool:
    host, _, port = addr.partition(":")
    try:
        with socket.create_connection((host, int(port or 9092)), timeout=PROBE_TIMEOUT):
            return True
    except OSError:
        return False


def _probe_http(url: str) -> bool:
    try:
        resp = requests.get(url, timeout=PROBE_TIMEOUT)
        return resp.status_code < 500
    except requests.RequestException:
        return False


def probe_kafka() -> bool:
    return _cached("kafka", lambda: _probe_tcp(KAFKA_ADDR))


def probe_consumer() -> bool:
    return _cached("consumer", lambda: _probe_http(CONSUMER_METRICS_URL))


def probe_prometheus() -> bool:
    return _cached("prometheus", lambda: _probe_http(f"{PROMETHEUS_URL}/-/ready"))


def probe_grafana() -> bool:
    return _cached("grafana", lambda: _probe_http(f"{GRAFANA_URL}/api/health"))


def status() -> dict:
    return {
        "api": True,
        "kafka": probe_kafka(),
        "consumer": probe_consumer(),
        "prometheus": probe_prometheus(),
        "grafana": probe_grafana(),
        "urls": {
            "api_docs": "http://localhost:8010/docs",
            "prometheus": PROMETHEUS_URL,
            "grafana": GRAFANA_URL,
            "consumer_metrics": CONSUMER_METRICS_URL,
        },
    }


def parse_aegis_metrics(text: str) -> dict:
    """Flatten a Prometheus exposition into {sample_name: value} for aegis_*
    series. Uses sample names (the parser strips _total from *family* names
    but samples keep it). Labelled samples get a ``name{k=v,...}`` key."""
    stats: dict = {}
    for family in text_string_to_metric_families(text):
        for sample in family.samples:
            name = sample.name
            if not name.startswith("aegis_"):
                continue
            if name.endswith("_created") or name.endswith("_bucket"):
                continue
            if sample.labels:
                label_str = ",".join(f"{k}={v}" for k, v in sorted(sample.labels.items()))
                key = f"{name}{{{label_str}}}"
            else:
                key = name
            stats[key] = sample.value
    return stats


def consumer_stats() -> dict:
    try:
        resp = requests.get(CONSUMER_METRICS_URL, timeout=PROBE_TIMEOUT)
        resp.raise_for_status()
    except requests.RequestException:
        return {"online": False, "stats": {}}
    return {"online": True, "stats": parse_aegis_metrics(resp.text)}
