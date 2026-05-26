import re
from typing import Dict, Any, Optional

# Matches timestamps at the beginning of the log line
TIMESTAMP_REGEX = re.compile(r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z)")

# Compiled once at module level for performance
KV_PATTERN = re.compile(r'(\b\w+)=(?:"([^"]*)"|([^\s"]+))')

def parse_log_line(line: str) -> Optional[Dict[str, Any]]:
    """
    Parses a single structured telemetry log line into a dictionary.
    Supports key-value parsing, extracting quoted strings and raw numbers.
    """
    line = line.strip()
    if not line:
        return None
        
    # Extract timestamp
    ts_match = TIMESTAMP_REGEX.match(line)
    if not ts_match:
        return None
        
    timestamp = ts_match.group(1)
    # The rest of the log line contains key-value pairs
    kv_part = line[len(timestamp):].strip()
    
    matches = KV_PATTERN.findall(kv_part)
    
    data = {
        "timestamp": timestamp,
        "raw_log": line
    }
    
    for key, quoted_val, unquoted_val in matches:
        value = quoted_val if quoted_val else unquoted_val
        
        # Try integer first (handles negative numbers correctly)
        try:
            data[key] = int(value)
            continue
        except (ValueError, TypeError):
            pass
        
        # Handle floats or percentages
        try:
            if value.endswith('%'):
                data[key] = float(value[:-1]) / 100.0
            elif '.' in value:
                data[key] = float(value)
            else:
                data[key] = value
        except ValueError:
            data[key] = value
                
    return data
