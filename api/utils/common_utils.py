from datetime import datetime, timezone
import traceback
from decimal import Decimal

from dateutil.parser import parser


def safe_to_epoch(ts):
    """
    Convert a timestamp (ISO string, datetime, or epoch number) to epoch milliseconds (int).
    """
    try:
        if ts is None:
            return int(datetime.utcnow().timestamp() * 1000)

        if isinstance(ts, (int, float)):
            # If already looks like epoch millis (13 digits), just return it
            if ts > 1e12:
                return int(ts)
            # If epoch seconds, convert to millis
            return int(ts * 1000)

        if isinstance(ts, datetime):
            return int(ts.timestamp() * 1000)

        # Parse ISO-like string
        ts = ts.replace("T:", "T")  # fix formatting if needed
        return int(datetime.fromisoformat(ts).timestamp() * 1000)

    except Exception:
        print(traceback.format_exc())
        return int(datetime.utcnow().timestamp() * 1000)

def to_dt_utc(val):
    """Return a timezone-aware UTC datetime from Decimal/int/float ms or ISO string."""
    if val is None:
        return None

    # Handle DynamoDB Decimal
    if isinstance(val, Decimal):
        val = int(val)

    # Numbers: detect ms vs s
    if isinstance(val, (int, float)):
        secs = val / 1000.0 if val > 1e12 else float(val)
        return datetime.fromtimestamp(secs, tz=timezone.utc)

    # Strings: ISO8601
    if isinstance(val, str) and val.strip():
        try:
            dt = parser.isoparse(val)
            # Make naive ISO strings UTC-aware
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except Exception:
            return None

    return None