"""master_key 鉴权（常数时间比较，防时序攻击）。"""
from __future__ import annotations

import hmac
import re
from typing import Optional

BEARER_RE = re.compile(r"^Bearer\s+(.+)$", re.IGNORECASE)


def extract_bearer(authorization: Optional[str]) -> Optional[str]:
    if not authorization:
        return None
    m = BEARER_RE.match(authorization.strip())
    return m.group(1).strip() if m else None


def verify_master_key(authorization: Optional[str], master_key: str) -> bool:
    token = extract_bearer(authorization)
    if not token:
        return False
    return hmac.compare_digest(token.encode("utf-8"), master_key.encode("utf-8"))