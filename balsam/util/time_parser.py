from datetime import datetime
from typing import Optional

import dateutil.parser
import dateutil.tz

et = dateutil.tz.gettz("America/New York")
ct = dateutil.tz.gettz("America/Chicago")
mt = dateutil.tz.gettz("America/Denver")
pt = dateutil.tz.gettz("America/Los Angeles")

TZINFOS = {
    "ET": et,
    "EST": et,
    "EDT": et,
    "CT": ct,
    "CST": ct,
    "CDT": ct,
    "MT": mt,
    "MST": mt,
    "MDT": mt,
    "PT": pt,
    "PST": pt,
    "PDT": pt,
}


def parse_to_utc(time_str: str, local_zone: Optional[str] = None) -> datetime:
    """
    Converts a non-UTC, timezone-aware string to a UTC datetime object
    """
    local_tz = {"L": TZINFOS[local_zone]} if local_zone else {}
    dt = dateutil.parser.parse(time_str, tzinfos={**TZINFOS, **local_tz})
    dt_utc = dt.astimezone(dateutil.tz.UTC)
    return dt_utc
