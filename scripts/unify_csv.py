import math
import re
from datetime import datetime
from typing import Optional, Tuple

import pandas as pd


def extract_airline_code(flight_number: Optional[str]) -> str:
    """
    簡單描述
    從航班編號字串擷取航空公司兩到三碼（前綴英文字母）。

    參數：
    - flight_number：航班編號，如 "HX261"、"CI073"。

    返回：
    - str：航空公司代碼（大寫），若無法解析則回傳空字串。

    範例：
    - 輸入："HX261" → 輸出："HX"
    - 輸入：None → 輸出：""
    """
    if not isinstance(flight_number, str) or not flight_number:
        return ""
    m = re.match(r"([A-Za-z]+)", flight_number)
    return m.group(1).upper() if m else ""


def to_time_hhmm(value: Optional[str]) -> str:
    """
    簡單描述
    將時間字串正規化為 HH:MM（24 小時制）。支援完整日期時間（YYYY-MM-DD HH:MM:SS）、YYYY/MM/DD HH:MM、HH:MM，以及內文帶有 HH:MM 的情況。

    參數：
    - value：時間字串，如 "2025-11-05 19:20:00" 或 "19:20"。

    返回：
    - str：正規化後的 "HH:MM"；若無法解析則回傳空字串。

    範例：
    - 輸入："2025-11-05 19:20:00" → 輸出："19:20"
    - 輸入："0 days 19:20:00" → 輸出："19:20"
    - 輸入："19:05" → 輸出："19:05"
    """
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return ""
    if not isinstance(value, str):
        value = str(value)
    value = value.strip()
    if not value:
        return ""
    # Try full datetime
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M", "%Y-%m-%d %H:%M"):
        try:
            dt = datetime.strptime(value, fmt)
            return dt.strftime("%H:%M")
        except Exception:
            pass
    # Already HH:MM
    m = re.match(r"^(\d{1,2}):(\d{2})$", value)
    if m:
        hh = int(m.group(1))
        mm = int(m.group(2))
        return f"{hh:02d}:{mm:02d}"
    # Other formats -> best effort: keep tail HH:MM if found
    m = re.search(r"(\d{1,2}:\d{2})", value)
    if m:
        hh, mm = m.group(1).split(":")
        return f"{int(hh):02d}:{int(mm):02d}"
    return ""


def to_date_yyyy_slash_mm_slash_dd(value: Optional[str]) -> str:
    """
    簡單描述
    將日期或日期時間字串轉換為 YYYY/MM/DD 格式。

    參數：
    - value：日期或日期時間字串，如 "2025-11-05 19:20:00"、"2025/11/05"。

    返回：
    - str："YYYY/MM/DD" 字串；若無法解析則回傳空字串。

    範例：
    - 輸入："2025-11-05 19:20:00" → 輸出："2025/11/05"
    - 輸入："2025/11/05" → 輸出："2025/11/05"
    """
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return ""
    if not isinstance(value, str):
        value = str(value)
    value = value.strip()
    if not value:
        return ""
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%Y/%m/%d %H:%M", "%Y/%m/%d"):
        try:
            dt = datetime.strptime(value, fmt)
            return dt.strftime("%Y/%m/%d")
        except Exception:
            pass
    # Try to parse ISO-like with pandas as fallback
    try:
        dt = pd.to_datetime(value, errors="coerce")
        if pd.notna(dt):
            return dt.strftime("%Y/%m/%d")
    except Exception:
        pass
    return ""


def duration_to_minutes(value: Optional[str]) -> Optional[int]:
    """
    簡單描述
    將飛行時間字串轉為總分鐘數。支援格式如 "0 days 02:05:00"、"02:05:00"，或純數字（視為分鐘）。

    參數：
    - value：持續時間字串或數值。

    返回：
    - Optional[int]：總分鐘數；若無法解析則回傳 None。

    範例：
    - 輸入："0 days 02:05:00" → 輸出：125
    - 輸入："01:30:30" → 輸出：91（四捨五入秒數）
    - 輸入："95" → 輸出：95
    """
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return int(value)
    if not isinstance(value, str):
        value = str(value)
    value = value.strip()
    if not value:
        return None
    # Patterns like "0 days 02:05:00" or "02:05:00"
    m = re.search(r"(?:(\d+)\s*days\s*)?(\d{1,2}):(\d{2})(?::(\d{2}))?", value)
    if m:
        days = int(m.group(1)) if m.group(1) else 0
        hours = int(m.group(2))
        minutes = int(m.group(3))
        seconds = int(m.group(4)) if m.group(4) else 0
        total = days * 24 * 60 + hours * 60 + minutes + (1 if seconds >= 30 else 0)
        return total
    # Fallback: numbers-only assume minutes
    if re.match(r"^\d+$", value):
        return int(value)
    return None


def split_luggage(value: Optional[str]) -> Tuple[Optional[float], str]:
    """
    簡單描述
    解析行李欄位為（數值, 單位）。將單位正規化為「件」或「公斤」。

    參數：
    - value：行李描述，如 "1件"、"25 公斤"、"2 件"。

    返回：
    - Tuple[Optional[float], str]：數值（float 或 None）與單位（"件"/"公斤"/空字串）。

    範例：
    - 輸入："1件" → 輸出：(1.0, "件")
    - 輸入："25 公斤" → 輸出：(25.0, "公斤")
    - 輸入："無" → 輸出：(None, "")
    """
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None, ""
    if not isinstance(value, str):
        value = str(value)
    value = value.strip()
    if not value:
        return None, ""
    # Examples: "1件", "25公斤", "30 公斤", "2 件"
    num_match = re.search(r"(\d+(?:\.\d+)?)", value)
    unit = re.sub(r"[\d\s\.]+", "", value)
    number = float(num_match.group(1)) if num_match else None
    # Normalize units to exactly hk4g4 uses: 件 / 公斤
    if "件" in unit:
        unit = "件"
    elif any(u in unit for u in ["公斤", "kg", "KG", "Kg"]):
        unit = "公斤"
    return number, unit
