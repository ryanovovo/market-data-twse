from __future__ import annotations

import argparse
import csv
import json
import random
import re
import subprocess
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlencode

from loguru import logger
from tqdm.auto import tqdm

TWSE_BASE = "https://www.twse.com.tw"
TWSE_OPENAPI_BASE = "https://openapi.twse.com.tw/v1"
TWSE_RWD_ZH_BASE = "https://www.twse.com.tw/rwd/zh"
MARKET_DAY_FILE_RE = re.compile(r"market_day_all_(\d{4}-\d{2}-\d{2})\.csv$")


def parse_cli_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def iter_days(start: date, end: date) -> list[date]:
    days: list[date] = []
    current = start
    while current <= end:
        days.append(current)
        current += timedelta(days=1)
    return days


def iter_month_starts(start: date, end: date) -> list[date]:
    months: list[date] = []
    current = date(start.year, start.month, 1)
    while current <= end:
        months.append(current)
        if current.month == 12:
            current = date(current.year + 1, 1, 1)
        else:
            current = date(current.year, current.month + 1, 1)
    return months


def roc_slash_to_iso(roc_date: str) -> str:
    year_str, month_str, day_str = roc_date.split("/")
    year = int(year_str) + 1911
    return f"{year:04d}-{int(month_str):02d}-{int(day_str):02d}"


def cleanup_cell(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip().replace(",", "")


def parse_int_cell(value: Any) -> int:
    text = cleanup_cell(value)
    if not text:
        return 0
    return int(text)


def parse_float_cell(value: Any) -> float | None:
    text = cleanup_cell(value)
    if not text or text in {"--", "-", "X"}:
        return None
    return float(text)


def parse_roc_text_date(value: str) -> date:
    match = re.search(r"(\d+)年(\d+)月(\d+)日", value)
    if not match:
        raise ValueError(f"無法解析日期: {value}")
    year = int(match.group(1)) + 1911
    month = int(match.group(2))
    day = int(match.group(3))
    return date(year, month, day)


def parse_roc_dot_date(value: str) -> date:
    text = cleanup_cell(value)
    match = re.search(r"(\d+)\.(\d+)\.(\d+)", text)
    if not match:
        raise ValueError(f"無法解析日期: {value}")
    year = int(match.group(1)) + 1911
    month = int(match.group(2))
    day = int(match.group(3))
    return date(year, month, day)


def strip_html(value: Any) -> str:
    text = cleanup_cell(value)
    return re.sub(r"<[^>]+>", "", text).strip()


def first_row(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return rows[0] if rows else {}


def resolve_daytrade_status(daytrade_row: dict[str, Any]) -> tuple[str, str]:
    if not daytrade_row:
        return ("N", "不可當沖")
    if cleanup_cell(daytrade_row.get("suspension_flag", "")) == "Y":
        return ("Y", "僅先買後賣")
    return ("Y", "雙向皆可")


def apply_back_adjustment(
    price_rows: list[dict[str, Any]],
    event_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    factors_by_date: dict[date, float] = {}
    for event in event_rows:
        event_date = event["event_date"]
        factor = float(event.get("factor", 1.0))
        factors_by_date[event_date] = factors_by_date.get(event_date, 1.0) * factor

    event_dates_desc = sorted(factors_by_date.keys(), reverse=True)
    rows_desc = sorted(price_rows, key=lambda row: row["date"], reverse=True)

    idx = 0
    cumulative_factor = 1.0
    adjusted_desc: list[dict[str, Any]] = []

    for row in rows_desc:
        row_date = parse_cli_date(row["date"])
        while idx < len(event_dates_desc) and event_dates_desc[idx] > row_date:
            cumulative_factor *= factors_by_date[event_dates_desc[idx]]
            idx += 1

        out = dict(row)
        out["adj_factor_back"] = f"{cumulative_factor:.12f}"
        for key in ("open", "high", "low", "close"):
            raw = parse_float_cell(row.get(key, ""))
            out[f"adj_{key}"] = f"{raw * cumulative_factor:.4f}" if raw is not None else ""
        adjusted_desc.append(out)

    return sorted(adjusted_desc, key=lambda row: row["date"])


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        logger.warning("無資料可寫入：{}", path)
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8-sig") as fp:
        writer = csv.DictWriter(fp, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    logger.success("已輸出 {} 筆到 {}", len(rows), path)


class TwseClient:
    def __init__(self, delay: float = 0.8, jitter: float = 0.25) -> None:
        self.delay = delay
        self.jitter = jitter

    def close(self) -> None:
        return

    def _get_json(self, url: str, params: dict[str, Any] | None = None) -> Any:
        query = urlencode(params or {})
        request_url = f"{url}?{query}" if query else url
        last_error: Exception | None = None
        for attempt in range(6):
            try:
                result = subprocess.run(
                    [
                        "curl",
                        "-sS",
                        "--compressed",
                        "-w",
                        "\n__STATUS__:%{http_code}",
                        request_url,
                    ],
                    check=False,
                    capture_output=True,
                    text=True,
                )
                raw = result.stdout
                if "__STATUS__:" not in raw:
                    raise RuntimeError("curl 未回傳 HTTP 狀態碼")
                body, status_text = raw.rsplit("\n__STATUS__:", 1)
                status_code = int(status_text.strip())

                if result.returncode != 0:
                    raise RuntimeError(result.stderr.strip() or f"curl 失敗，code={result.returncode}")

                if status_code in (429, 500, 502, 503, 504):
                    raise RuntimeError(f"HTTP {status_code}")
                if status_code >= 400:
                    raise RuntimeError(f"HTTP {status_code} body={body[:200]}")

                payload = json.loads(body)
                time.sleep(self.delay + random.uniform(0.0, self.jitter))
                return payload
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                if attempt < 5:
                    backoff = min(10.0, (self.delay * (2**attempt)))
                    time.sleep(backoff + random.uniform(0.0, self.jitter))
        raise RuntimeError(f"呼叫 API 失敗: {request_url}") from last_error

    def fetch_price_month(self, stock_no: str, month_start: date) -> list[dict[str, Any]]:
        payload = self._get_json(
            f"{TWSE_BASE}/exchangeReport/STOCK_DAY",
            params={
                "response": "json",
                "date": month_start.strftime("%Y%m01"),
                "stockNo": stock_no,
            },
        )
        if payload.get("stat") != "OK":
            return []

        rows: list[dict[str, Any]] = []
        for row in payload.get("data", []):
            rows.append(
                {
                    "date": roc_slash_to_iso(row[0]),
                    "stock_no": stock_no,
                    "volume": cleanup_cell(row[1]),
                    "value": cleanup_cell(row[2]),
                    "open": cleanup_cell(row[3]),
                    "high": cleanup_cell(row[4]),
                    "low": cleanup_cell(row[5]),
                    "close": cleanup_cell(row[6]),
                    "change": cleanup_cell(row[7]),
                    "transactions": cleanup_cell(row[8]),
                    "note": cleanup_cell(row[9] if len(row) > 9 else ""),
                }
            )
        return rows

    def fetch_price_day_all(self, trading_day: date) -> dict[str, dict[str, Any]]:
        payload = self._get_json(
            f"{TWSE_RWD_ZH_BASE}/afterTrading/MI_INDEX",
            params={
                "response": "json",
                "date": trading_day.strftime("%Y%m%d"),
                "type": "ALLBUT0999",
            },
        )
        if payload.get("stat") != "OK":
            return {}

        target_table: dict[str, Any] | None = None
        for table in payload.get("tables", []):
            fields = table.get("fields", [])
            if (
                "證券代號" in fields
                and "證券名稱" in fields
                and "成交股數" in fields
                and "開盤價" in fields
                and "收盤價" in fields
            ):
                target_table = table
                break

        if not target_table:
            return {}

        rows: dict[str, dict[str, Any]] = {}
        for row in target_table.get("data", []):
            code = cleanup_cell(row[0])
            sign = strip_html(row[9])
            diff = cleanup_cell(row[10])
            if sign in {"+", "-"} and diff and not diff.startswith(("+", "-")):
                change = f"{sign}{diff}"
            else:
                change = diff
            rows[code] = {
                "date": trading_day.isoformat(),
                "stock_no": code,
                "stock_name": cleanup_cell(row[1]),
                "price_volume": cleanup_cell(row[2]),
                "price_transactions": cleanup_cell(row[3]),
                "price_value": cleanup_cell(row[4]),
                "price_open": cleanup_cell(row[5]),
                "price_high": cleanup_cell(row[6]),
                "price_low": cleanup_cell(row[7]),
                "price_close": cleanup_cell(row[8]),
                "price_change": change,
            }
        return rows

    def fetch_exright_results(
        self,
        stock_no: str,
        start: date,
        end: date,
    ) -> list[dict[str, Any]]:
        payload = self._get_json(
            f"{TWSE_RWD_ZH_BASE}/exRight/TWT49U",
            params={
                "response": "json",
                "startDate": start.strftime("%Y%m%d"),
                "endDate": end.strftime("%Y%m%d"),
            },
        )
        if payload.get("stat") != "OK":
            return []

        rows: list[dict[str, Any]] = []
        for row in payload.get("data", []):
            if row[1] != stock_no:
                continue
            event_date = parse_roc_text_date(cleanup_cell(row[0]))
            pre_close = parse_float_cell(row[3])
            ref_price = parse_float_cell(row[4])
            factor = 1.0
            if pre_close and ref_price and pre_close > 0:
                factor = ref_price / pre_close
            rows.append(
                {
                    "event_date": event_date,
                    "pre_close": pre_close,
                    "ref_price": ref_price,
                    "factor": factor,
                }
            )
        return rows

    def fetch_exright_results_all(
        self,
        start: date,
        end: date,
    ) -> dict[str, list[dict[str, Any]]]:
        payload = self._get_json(
            f"{TWSE_RWD_ZH_BASE}/exRight/TWT49U",
            params={
                "response": "json",
                "startDate": start.strftime("%Y%m%d"),
                "endDate": end.strftime("%Y%m%d"),
            },
        )
        if payload.get("stat") != "OK":
            return {}

        result: dict[str, list[dict[str, Any]]] = {}
        for row in payload.get("data", []):
            code = cleanup_cell(row[1] if len(row) > 1 else "")
            if not code:
                continue
            try:
                event_date = parse_roc_text_date(cleanup_cell(row[0]))
            except ValueError:
                continue
            pre_close = parse_float_cell(row[3] if len(row) > 3 else "")
            ref_price = parse_float_cell(row[4] if len(row) > 4 else "")
            factor = 1.0
            if pre_close and ref_price and pre_close > 0:
                factor = ref_price / pre_close
            result.setdefault(code, []).append({"event_date": event_date, "factor": factor})
        return result

    def fetch_variation_day(self, stock_no: str, trading_day: date) -> dict[str, Any] | None:
        payload = self._get_json(
            f"{TWSE_RWD_ZH_BASE}/variation/TWT84U",
            params={
                "response": "json",
                "date": trading_day.strftime("%Y%m%d"),
                "selectType": "ALLBUT0999",
            },
        )
        if payload.get("stat") != "OK":
            return None

        for row in payload.get("data", []):
            if row[0] != stock_no:
                continue
            last_day = parse_roc_dot_date(row[9]) if cleanup_cell(row[9]) else None
            return {
                "query_date": trading_day.isoformat(),
                "today_limit_up": cleanup_cell(row[2]),
                "today_open_ref": cleanup_cell(row[3]),
                "today_limit_down": cleanup_cell(row[4]),
                "previous_open_ref": cleanup_cell(row[5]),
                "previous_close": cleanup_cell(row[6]),
                "last_trading_day": last_day.isoformat() if last_day else "",
                "allow_odd_lot": cleanup_cell(row[10]),
            }

        return None

    def fetch_variation_day_all(self, trading_day: date) -> dict[str, dict[str, Any]]:
        payload = self._get_json(
            f"{TWSE_RWD_ZH_BASE}/variation/TWT84U",
            params={
                "response": "json",
                "date": trading_day.strftime("%Y%m%d"),
                "selectType": "ALLBUT0999",
            },
        )
        if payload.get("stat") != "OK":
            return {}

        rows: dict[str, dict[str, Any]] = {}
        for row in payload.get("data", []):
            code = cleanup_cell(row[0])
            last_day = parse_roc_dot_date(row[9]) if cleanup_cell(row[9]) else None
            rows[code] = {
                "query_date": trading_day.isoformat(),
                "today_limit_up": cleanup_cell(row[2]),
                "today_open_ref": cleanup_cell(row[3]),
                "today_limit_down": cleanup_cell(row[4]),
                "previous_open_ref": cleanup_cell(row[5]),
                "previous_close": cleanup_cell(row[6]),
                "last_trading_day": last_day.isoformat() if last_day else "",
                "allow_odd_lot": cleanup_cell(row[10]),
            }
        return rows

    def fetch_margin_day_all(self, trading_day: date) -> dict[str, dict[str, Any]]:
        payload = self._get_json(
            f"{TWSE_BASE}/exchangeReport/MI_MARGN",
            params={
                "response": "json",
                "date": trading_day.strftime("%Y%m%d"),
                "selectType": "ALL",
            },
        )
        if payload.get("stat") != "OK":
            return {}

        tables = payload.get("tables", [])
        if len(tables) < 2:
            return {}

        rows: dict[str, dict[str, Any]] = {}
        for row in tables[1].get("data", []):
            code = cleanup_cell(row[0])
            rows[code] = {
                "margn_margin_buy": cleanup_cell(row[2]),
                "margn_margin_sell": cleanup_cell(row[3]),
                "margn_margin_cash_repayment": cleanup_cell(row[4]),
                "margn_margin_prev_balance": cleanup_cell(row[5]),
                "margn_margin_balance": cleanup_cell(row[6]),
                "margn_short_buy": cleanup_cell(row[8]),
                "margn_short_sell": cleanup_cell(row[9]),
                "margn_short_cash_repayment": cleanup_cell(row[10]),
                "margn_short_prev_balance": cleanup_cell(row[11]),
                "margn_short_balance": cleanup_cell(row[12]),
                "margn_offset": cleanup_cell(row[14]),
                "margn_note": cleanup_cell(row[15]),
            }
        return rows

    def fetch_daytrade_day_all(self, trading_day: date) -> dict[str, dict[str, Any]]:
        payload = self._get_json(
            f"{TWSE_BASE}/exchangeReport/TWTB4U",
            params={
                "response": "json",
                "date": trading_day.strftime("%Y%m%d"),
            },
        )
        if payload.get("stat") != "OK":
            return {}

        tables = payload.get("tables", [])
        if len(tables) < 2:
            return {}

        rows: dict[str, dict[str, Any]] = {}
        for row in tables[1].get("data", []):
            code = cleanup_cell(row[0] if len(row) > 0 else "")
            if not code:
                continue
            suspension_flag = cleanup_cell(row[2] if len(row) > 2 else "")
            daytrade_type = "僅先買後賣" if suspension_flag == "Y" else "雙向皆可"
            rows[code] = {
                "can_daytrade": "Y",
                "daytrade_type": daytrade_type,
                "daytrade_suspension_flag": suspension_flag,
                "daytrade_volume": cleanup_cell(row[3] if len(row) > 3 else ""),
                "daytrade_buy_value": cleanup_cell(row[4] if len(row) > 4 else ""),
                "daytrade_sell_value": cleanup_cell(row[5] if len(row) > 5 else ""),
            }
        return rows

    def fetch_credit_quota_day_all(self, trading_day: date) -> dict[str, dict[str, Any]]:
        payload = self._get_json(
            f"{TWSE_BASE}/exchangeReport/TWT93U",
            params={
                "response": "json",
                "date": trading_day.strftime("%Y%m%d"),
            },
        )
        if payload.get("stat") != "OK":
            return {}

        rows: dict[str, dict[str, Any]] = {}
        for row in payload.get("data", []):
            code = cleanup_cell(row[0])
            rows[code] = {
                "quota_margin_short_prev_balance": cleanup_cell(row[2]),
                "quota_margin_short_sell": cleanup_cell(row[3]),
                "quota_margin_short_buy": cleanup_cell(row[4]),
                "quota_margin_short_cash_repayment": cleanup_cell(row[5]),
                "quota_margin_short_balance": cleanup_cell(row[6]),
                "quota_margin_short_next_limit": cleanup_cell(row[7]),
                "quota_sbl_prev_balance": cleanup_cell(row[8]),
                "quota_sbl_sell": cleanup_cell(row[9]),
                "quota_sbl_return": cleanup_cell(row[10]),
                "quota_sbl_adjust": cleanup_cell(row[11]),
                "quota_sbl_balance": cleanup_cell(row[12]),
                "quota_sbl_next_limit": cleanup_cell(row[13]),
                "quota_note": cleanup_cell(row[14]),
            }
        return rows

    def fetch_issued_shares_day_all(self, trading_day: date) -> dict[str, str]:
        payload = self._get_json(
            f"{TWSE_BASE}/fund/MI_QFIIS",
            params={
                "response": "json",
                "date": trading_day.strftime("%Y%m%d"),
                "selectType": "ALLBUT0999",
            },
        )
        if payload.get("stat") != "OK":
            return {}

        rows: dict[str, str] = {}
        for row in payload.get("data", []):
            rows[cleanup_cell(row[0])] = cleanup_cell(row[3])
        return rows

    def fetch_sbl_available_all(self) -> dict[str, str]:
        rows = self._get_json(f"{TWSE_OPENAPI_BASE}/SBL/TWT96U")
        out: dict[str, str] = {}
        for row in rows:
            code = cleanup_cell(row.get("TWSECode", ""))
            if not code:
                continue
            out[code] = cleanup_cell(row.get("TWSEAvailableVolume", ""))
        return out

    def fetch_margin_day(self, stock_no: str, trading_day: date) -> list[dict[str, Any]]:
        payload = self._get_json(
            f"{TWSE_BASE}/exchangeReport/MI_MARGN",
            params={
                "response": "json",
                "date": trading_day.strftime("%Y%m%d"),
                "selectType": "ALL",
            },
        )
        if payload.get("stat") != "OK":
            return []

        tables = payload.get("tables", [])
        if len(tables) < 2:
            return []

        result: list[dict[str, Any]] = []
        for row in tables[1].get("data", []):
            if row[0] != stock_no:
                continue
            result.append(
                {
                    "date": trading_day.isoformat(),
                    "stock_no": row[0],
                    "stock_name": cleanup_cell(row[1]),
                    "margin_buy": cleanup_cell(row[2]),
                    "margin_sell": cleanup_cell(row[3]),
                    "margin_cash_repayment": cleanup_cell(row[4]),
                    "margin_prev_balance": cleanup_cell(row[5]),
                    "margin_balance": cleanup_cell(row[6]),
                    "short_buy": cleanup_cell(row[8]),
                    "short_sell": cleanup_cell(row[9]),
                    "short_cash_repayment": cleanup_cell(row[10]),
                    "short_prev_balance": cleanup_cell(row[11]),
                    "short_balance": cleanup_cell(row[12]),
                    "offset": cleanup_cell(row[14]),
                    "note": cleanup_cell(row[15]),
                }
            )
        return result

    def fetch_daytrade_day(self, stock_no: str, trading_day: date) -> list[dict[str, Any]]:
        payload = self._get_json(
            f"{TWSE_BASE}/exchangeReport/TWTB4U",
            params={
                "response": "json",
                "date": trading_day.strftime("%Y%m%d"),
            },
        )
        if payload.get("stat") != "OK":
            return []

        tables = payload.get("tables", [])
        if len(tables) < 2:
            return []

        result: list[dict[str, Any]] = []
        for row in tables[1].get("data", []):
            code = cleanup_cell(row[0] if len(row) > 0 else "")
            if code != stock_no:
                continue
            suspension_flag = cleanup_cell(row[2] if len(row) > 2 else "")
            if suspension_flag == "Y":
                daytrade_type = "僅先買後賣"
            else:
                daytrade_type = "雙向皆可"
            result.append(
                {
                    "date": trading_day.isoformat(),
                    "stock_no": code,
                    "stock_name": cleanup_cell(row[1] if len(row) > 1 else ""),
                    "can_daytrade": "Y",
                    "daytrade_type": daytrade_type,
                    "suspension_flag": suspension_flag,
                    "daytrade_volume": cleanup_cell(row[3] if len(row) > 3 else ""),
                    "daytrade_buy_value": cleanup_cell(row[4] if len(row) > 4 else ""),
                    "daytrade_sell_value": cleanup_cell(row[5] if len(row) > 5 else ""),
                }
            )
        return result

    def fetch_credit_quota_day(self, stock_no: str, trading_day: date) -> list[dict[str, Any]]:
        payload = self._get_json(
            f"{TWSE_BASE}/exchangeReport/TWT93U",
            params={
                "response": "json",
                "date": trading_day.strftime("%Y%m%d"),
            },
        )
        if payload.get("stat") != "OK":
            return []

        result: list[dict[str, Any]] = []
        for row in payload.get("data", []):
            if row[0] != stock_no:
                continue
            result.append(
                {
                    "date": trading_day.isoformat(),
                    "stock_no": row[0],
                    "stock_name": cleanup_cell(row[1]),
                    "margin_short_prev_balance": cleanup_cell(row[2]),
                    "margin_short_sell": cleanup_cell(row[3]),
                    "margin_short_buy": cleanup_cell(row[4]),
                    "margin_short_cash_repayment": cleanup_cell(row[5]),
                    "margin_short_balance": cleanup_cell(row[6]),
                    "margin_short_next_limit": cleanup_cell(row[7]),
                    "sbl_prev_balance": cleanup_cell(row[8]),
                    "sbl_sell": cleanup_cell(row[9]),
                    "sbl_return": cleanup_cell(row[10]),
                    "sbl_adjust": cleanup_cell(row[11]),
                    "sbl_balance": cleanup_cell(row[12]),
                    "sbl_next_limit": cleanup_cell(row[13]),
                    "note": cleanup_cell(row[14]),
                }
            )
        return result

    def fetch_sbl_available(self, stock_no: str) -> list[dict[str, Any]]:
        rows = self._get_json(f"{TWSE_OPENAPI_BASE}/SBL/TWT96U")
        result: list[dict[str, Any]] = []
        for row in rows:
            if row.get("TWSECode") != stock_no:
                continue
            result.append(
                {
                    "stock_no": stock_no,
                    "sbl_available_volume": cleanup_cell(row.get("TWSEAvailableVolume", "")),
                }
            )
        return result

    def fetch_listed_companies(self) -> list[dict[str, Any]]:
        return self._get_json(f"{TWSE_OPENAPI_BASE}/opendata/t187ap03_L")

    def fetch_issued_shares_day(self, stock_no: str, trading_day: date) -> int | None:
        payload = self._get_json(
            f"{TWSE_BASE}/fund/MI_QFIIS",
            params={
                "response": "json",
                "date": trading_day.strftime("%Y%m%d"),
                "selectType": "ALLBUT0999",
            },
        )
        if payload.get("stat") != "OK":
            return None

        for row in payload.get("data", []):
            if row[0] == stock_no:
                return parse_int_cell(row[3])
        return None

    def fetch_issued_shares(self, stock_no: str) -> int:
        rows = self._get_json(f"{TWSE_OPENAPI_BASE}/opendata/t187ap03_L")
        for row in rows:
            if row.get("公司代號") == stock_no:
                shares_text = row.get("已發行普通股數或TDR原股發行股數", "0")
                return parse_int_cell(shares_text)
        raise ValueError(f"查無股票代號 {stock_no} 的已發行股數資料")


def ensure_range(start: date, end: date) -> None:
    if end < start:
        raise ValueError("--end 必須大於或等於 --start")


def configure_logger(level: str) -> None:
    logger.remove()
    logger.add(
        lambda msg: tqdm.write(msg, end=""),
        level=level.upper(),
        colorize=False,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level:<7} | {message}",
    )


def discover_market_day_files(out_dir: Path) -> list[tuple[date, Path]]:
    files: list[tuple[date, Path]] = []
    for path in out_dir.glob("market_day_all_*.csv"):
        match = MARKET_DAY_FILE_RE.fullmatch(path.name)
        if not match:
            continue
        try:
            day = parse_cli_date(match.group(1))
        except ValueError:
            continue
        files.append((day, path))
    files.sort(key=lambda item: item[0])
    return files


def find_latest_trading_day(client: TwseClient, anchor: date | None = None) -> date:
    check_day = anchor or date.today()
    for _ in range(31):
        try:
            rows = client.fetch_price_day_all(check_day)
            if rows:
                return check_day
        except Exception:  # noqa: BLE001
            pass
        check_day -= timedelta(days=1)
    raise RuntimeError("找不到最近交易日，請手動指定 --end")


def resolve_end_date(client: TwseClient, end: date | None) -> date:
    if end:
        return end
    latest = find_latest_trading_day(client)
    logger.info("未指定 --end，使用最近交易日 {}", latest.isoformat())
    return latest


def progress(
    iterable: Any,
    *,
    desc: str,
    total: int | None = None,
) -> Any:
    return tqdm(
        iterable,
        desc=desc,
        total=total,
        dynamic_ncols=True,
        ascii=True,
    )


def run_price(client: TwseClient, args: argparse.Namespace) -> None:
    end = resolve_end_date(client, args.end)
    ensure_range(args.start, end)
    all_rows: list[dict[str, Any]] = []
    months = iter_month_starts(args.start, end)
    for month in progress(
        months,
        desc=f"price {args.stock} 月資料",
        total=len(months),
    ):
        month_rows = client.fetch_price_month(args.stock, month)
        for row in month_rows:
            d = parse_cli_date(row["date"])
            if args.start <= d <= end:
                all_rows.append(row)
    all_rows.sort(key=lambda row: row["date"])
    write_csv(args.out, all_rows)


def run_adjusted_price(client: TwseClient, args: argparse.Namespace) -> None:
    end = resolve_end_date(client, args.end)
    ensure_range(args.start, end)
    price_rows: list[dict[str, Any]] = []
    months = iter_month_starts(args.start, end)
    for month in progress(
        months,
        desc=f"adjusted-price {args.stock} 月資料",
        total=len(months),
    ):
        for row in client.fetch_price_month(args.stock, month):
            d = parse_cli_date(row["date"])
            if args.start <= d <= end:
                price_rows.append(row)
    price_rows.sort(key=lambda row: row["date"])

    events = client.fetch_exright_results(args.stock, args.start, end)
    adjusted_rows = apply_back_adjustment(price_rows, events)
    write_csv(args.out, adjusted_rows)


def run_daily_loop(
    client: TwseClient,
    stock_no: str,
    start: date,
    end: date,
    fetcher: Callable[[str, date], list[dict[str, Any]]],
    progress_desc: str | None = None,
) -> list[dict[str, Any]]:
    ensure_range(start, end)
    rows: list[dict[str, Any]] = []
    trading_days = [day for day in iter_days(start, end) if day.weekday() < 5]
    for day in progress(
        trading_days,
        desc=progress_desc or f"{stock_no} 日資料",
        total=len(trading_days),
    ):
        rows.extend(fetcher(stock_no, day))
    rows.sort(key=lambda row: row["date"])
    return rows


def run_margin(client: TwseClient, args: argparse.Namespace) -> None:
    end = resolve_end_date(client, args.end)
    rows = run_daily_loop(
        client=client,
        stock_no=args.stock,
        start=args.start,
        end=end,
        fetcher=client.fetch_margin_day,
        progress_desc=f"margin {args.stock}",
    )
    write_csv(args.out, rows)


def run_daytrade(client: TwseClient, args: argparse.Namespace) -> None:
    end = resolve_end_date(client, args.end)
    rows = run_daily_loop(
        client=client,
        stock_no=args.stock,
        start=args.start,
        end=end,
        fetcher=client.fetch_daytrade_day,
        progress_desc=f"daytrade {args.stock}",
    )
    write_csv(args.out, rows)


def run_credit_quota(client: TwseClient, args: argparse.Namespace) -> None:
    end = resolve_end_date(client, args.end)
    rows = run_daily_loop(
        client=client,
        stock_no=args.stock,
        start=args.start,
        end=end,
        fetcher=client.fetch_credit_quota_day,
        progress_desc=f"credit-quota {args.stock}",
    )
    write_csv(args.out, rows)


def run_sbl(client: TwseClient, args: argparse.Namespace) -> None:
    rows = client.fetch_sbl_available(args.stock)
    write_csv(args.out, rows)


def run_listed_companies(client: TwseClient, args: argparse.Namespace) -> None:
    rows = client.fetch_listed_companies()
    result: list[dict[str, Any]] = []
    for row in rows:
        result.append(
            {
                "report_date": cleanup_cell(row.get("出表日期", "")),
                "stock_no": cleanup_cell(row.get("公司代號", "")),
                "company_name": cleanup_cell(row.get("公司名稱", "")),
                "short_name": cleanup_cell(row.get("公司簡稱", "")),
                "industry_code": cleanup_cell(row.get("產業別", "")),
                "listing_date": cleanup_cell(row.get("上市日期", "")),
                "issued_shares": cleanup_cell(row.get("已發行普通股數或TDR原股發行股數", "")),
                "paid_in_capital": cleanup_cell(row.get("實收資本額", "")),
                "chairman": cleanup_cell(row.get("董事長", "")),
                "general_manager": cleanup_cell(row.get("總經理", "")),
                "spokesperson": cleanup_cell(row.get("發言人", "")),
                "phone": cleanup_cell(row.get("總機電話", "")),
                "address": cleanup_cell(row.get("住址", "")),
                "website": cleanup_cell(row.get("網址", "")),
            }
        )

    result.sort(key=lambda row: row["stock_no"])
    write_csv(args.out, result)


def build_market_day_all_rows(
    client: TwseClient,
    trading_day: date,
    listed_rows: list[dict[str, Any]] | None = None,
    sbl_map: dict[str, str] | None = None,
) -> list[dict[str, Any]]:
    if listed_rows is None:
        listed_rows = client.fetch_listed_companies()
    listed_set = {cleanup_cell(row.get("公司代號", "")) for row in listed_rows}
    listed_set.discard("")
    fallback_shares_by_code = {
        cleanup_cell(row.get("公司代號", "")): cleanup_cell(row.get("已發行普通股數或TDR原股發行股數", ""))
        for row in listed_rows
        if cleanup_cell(row.get("公司代號", ""))
    }
    industry_by_code = {
        cleanup_cell(row.get("公司代號", "")): cleanup_cell(row.get("產業別", ""))
        for row in listed_rows
        if cleanup_cell(row.get("公司代號", ""))
    }

    price_map = client.fetch_price_day_all(trading_day)
    margin_map = client.fetch_margin_day_all(trading_day)
    daytrade_map = client.fetch_daytrade_day_all(trading_day)
    quota_map = client.fetch_credit_quota_day_all(trading_day)
    issued_map = client.fetch_issued_shares_day_all(trading_day)
    if sbl_map is None:
        sbl_map = client.fetch_sbl_available_all()

    next_day_map: dict[str, dict[str, str]] = {}
    for day in iter_days(trading_day + timedelta(days=1), trading_day + timedelta(days=10)):
        if day.weekday() >= 5:
            continue
        variation_all = client.fetch_variation_day_all(day)
        target_codes: list[str] = []
        for code, row in variation_all.items():
            if cleanup_cell(row.get("last_trading_day", "")) != trading_day.isoformat():
                continue
            target_codes.append(code)
            next_day_map[code] = {
                "next_trade_date": cleanup_cell(row.get("query_date", "")),
                "next_open_ref_price": cleanup_cell(row.get("today_open_ref", "")),
                "next_limit_up_price": cleanup_cell(row.get("today_limit_up", "")),
                "next_limit_down_price": cleanup_cell(row.get("today_limit_down", "")),
            }

        if not target_codes:
            continue
        daytrade_next_map = client.fetch_daytrade_day_all(day)
        for code in target_codes:
            daytrade_next = daytrade_next_map.get(code, {})
            if daytrade_next:
                next_can_daytrade = cleanup_cell(daytrade_next.get("can_daytrade", "Y"))
                next_daytrade_type = cleanup_cell(daytrade_next.get("daytrade_type", "雙向皆可"))
                next_daytrade_flag = cleanup_cell(daytrade_next.get("daytrade_suspension_flag", ""))
            else:
                next_can_daytrade = "N"
                next_daytrade_type = "不可當沖"
                next_daytrade_flag = ""

            next_day_map[code] = {
                **next_day_map.get(code, {}),
                "next_can_daytrade": next_can_daytrade,
                "next_daytrade_type": next_daytrade_type,
                "next_daytrade_suspension_flag": next_daytrade_flag,
            }

    rows: list[dict[str, Any]] = []
    for code, price in sorted(price_map.items()):
        if code not in listed_set:
            continue

        margin = margin_map.get(code, {})
        daytrade = daytrade_map.get(code, {})
        quota = quota_map.get(code, {})
        next_day = next_day_map.get(code, {})

        if not daytrade:
            can_daytrade = "N"
            daytrade_type = "不可當沖"
        else:
            can_daytrade = cleanup_cell(daytrade.get("can_daytrade", "Y"))
            daytrade_type = cleanup_cell(daytrade.get("daytrade_type", "雙向皆可"))

        issued = issued_map.get(code, fallback_shares_by_code.get(code, ""))
        issued_source = "MI_QFIIS" if code in issued_map else "t187ap03_L"
        volume = parse_int_cell(price.get("price_volume", "0"))
        issued_num = parse_int_cell(issued)
        turnover = (volume / issued_num * 100) if issued_num else 0.0

        rows.append(
            {
                "date": trading_day.isoformat(),
                "stock_no": code,
                "stock_name": price.get("stock_name", ""),
                "industry_code": industry_by_code.get(code, ""),
                "price_volume": price.get("price_volume", ""),
                "price_value": price.get("price_value", ""),
                "price_open": price.get("price_open", ""),
                "price_high": price.get("price_high", ""),
                "price_low": price.get("price_low", ""),
                "price_close": price.get("price_close", ""),
                "price_change": price.get("price_change", ""),
                "price_transactions": price.get("price_transactions", ""),
                "adj_factor_back": "1.000000000000",
                "adj_open": price.get("price_open", ""),
                "adj_high": price.get("price_high", ""),
                "adj_low": price.get("price_low", ""),
                "adj_close": price.get("price_close", ""),
                "next_trade_date": next_day.get("next_trade_date", ""),
                "next_open_ref_price": next_day.get("next_open_ref_price", ""),
                "next_limit_up_price": next_day.get("next_limit_up_price", ""),
                "next_limit_down_price": next_day.get("next_limit_down_price", ""),
                "next_can_daytrade": next_day.get("next_can_daytrade", ""),
                "next_daytrade_type": next_day.get("next_daytrade_type", ""),
                "next_daytrade_suspension_flag": next_day.get("next_daytrade_suspension_flag", ""),
                "margn_margin_buy": margin.get("margn_margin_buy", ""),
                "margn_margin_sell": margin.get("margn_margin_sell", ""),
                "margn_margin_cash_repayment": margin.get("margn_margin_cash_repayment", ""),
                "margn_margin_prev_balance": margin.get("margn_margin_prev_balance", ""),
                "margn_margin_balance": margin.get("margn_margin_balance", ""),
                "margn_short_buy": margin.get("margn_short_buy", ""),
                "margn_short_sell": margin.get("margn_short_sell", ""),
                "margn_short_cash_repayment": margin.get("margn_short_cash_repayment", ""),
                "margn_short_prev_balance": margin.get("margn_short_prev_balance", ""),
                "margn_short_balance": margin.get("margn_short_balance", ""),
                "margn_offset": margin.get("margn_offset", ""),
                "margn_note": margin.get("margn_note", ""),
                "can_daytrade": can_daytrade,
                "daytrade_type": daytrade_type,
                "daytrade_suspension_flag": daytrade.get("daytrade_suspension_flag", ""),
                "daytrade_volume": daytrade.get("daytrade_volume", ""),
                "daytrade_buy_value": daytrade.get("daytrade_buy_value", ""),
                "daytrade_sell_value": daytrade.get("daytrade_sell_value", ""),
                "quota_margin_short_prev_balance": quota.get("quota_margin_short_prev_balance", ""),
                "quota_margin_short_sell": quota.get("quota_margin_short_sell", ""),
                "quota_margin_short_buy": quota.get("quota_margin_short_buy", ""),
                "quota_margin_short_cash_repayment": quota.get("quota_margin_short_cash_repayment", ""),
                "quota_margin_short_balance": quota.get("quota_margin_short_balance", ""),
                "quota_margin_short_next_limit": quota.get("quota_margin_short_next_limit", ""),
                "quota_sbl_prev_balance": quota.get("quota_sbl_prev_balance", ""),
                "quota_sbl_sell": quota.get("quota_sbl_sell", ""),
                "quota_sbl_return": quota.get("quota_sbl_return", ""),
                "quota_sbl_adjust": quota.get("quota_sbl_adjust", ""),
                "quota_sbl_balance": quota.get("quota_sbl_balance", ""),
                "quota_sbl_next_limit": quota.get("quota_sbl_next_limit", ""),
                "quota_note": quota.get("quota_note", ""),
                "issued_shares": issued,
                "issued_shares_source": issued_source,
                "turnover_ratio_pct": f"{turnover:.6f}",
                "sbl_available_volume_latest": sbl_map.get(code, ""),
            }
        )

    return rows


def run_market_day_all(client: TwseClient, args: argparse.Namespace) -> None:
    rows = build_market_day_all_rows(client, args.date)
    write_csv(args.out, rows)


def run_market_adj_backfill(client: TwseClient, out_dir: Path) -> None:
    files = discover_market_day_files(out_dir)
    if not files:
        logger.info("沒有找到可回補的 market_day_all 檔案，略過還原回補")
        return

    all_dates = [d for d, _ in files]
    start_day = all_dates[0]
    end_day = all_dates[-1]
    logger.info(
        "開始檢查全市場還原回補：檔案數={}，日期範圍 {} ~ {}",
        len(files),
        start_day.isoformat(),
        end_day.isoformat(),
    )

    events_by_stock = client.fetch_exright_results_all(start_day, end_day)
    if not events_by_stock:
        logger.info("查無除權息事件，僅執行次日當沖欄位回補")

    factors_by_stock_date: dict[str, dict[date, float]] = {}
    for code, events in events_by_stock.items():
        stock_map = factors_by_stock_date.setdefault(code, {})
        for event in events:
            event_day = event["event_date"]
            factor = float(event.get("factor", 1.0))
            stock_map[event_day] = stock_map.get(event_day, 1.0) * factor

    dates_desc = sorted(all_dates, reverse=True)
    factor_lookup: dict[str, dict[str, float]] = {}
    for code, factors_by_date in factors_by_stock_date.items():
        event_dates_desc = sorted(factors_by_date.keys(), reverse=True)
        idx = 0
        cumulative = 1.0
        by_day: dict[str, float] = {}
        for day in dates_desc:
            while idx < len(event_dates_desc) and event_dates_desc[idx] > day:
                cumulative *= factors_by_date[event_dates_desc[idx]]
                idx += 1
            if cumulative != 1.0:
                by_day[day.isoformat()] = cumulative
        if by_day:
            factor_lookup[code] = by_day

    logger.info(
        "除權息事件股票數={}，需套用還原係數股票數={}",
        len(factors_by_stock_date),
        len(factor_lookup),
    )

    updated_files = 0
    updated_rows = 0
    next_daytrade_cache: dict[str, dict[str, dict[str, Any]]] = {}
    for day, path in progress(files, desc="回補還原股價", total=len(files)):
        with path.open("r", newline="", encoding="utf-8-sig") as fp:
            reader = csv.DictReader(fp)
            fieldnames = list(reader.fieldnames or [])
            rows = list(reader)

        if not rows:
            continue

        for col in ("adj_factor_back", "adj_open", "adj_high", "adj_low", "adj_close"):
            if col not in fieldnames:
                fieldnames.append(col)
        for col in (
            "next_can_daytrade",
            "next_daytrade_type",
            "next_daytrade_suspension_flag",
        ):
            if col not in fieldnames:
                fieldnames.append(col)

        next_trade_dates = {
            cleanup_cell(row.get("next_trade_date", ""))
            for row in rows
            if cleanup_cell(row.get("next_trade_date", ""))
        }
        for next_trade_day in next_trade_dates:
            if next_trade_day in next_daytrade_cache:
                continue
            try:
                next_daytrade_cache[next_trade_day] = client.fetch_daytrade_day_all(
                    parse_cli_date(next_trade_day)
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning("取得次日當沖資料失敗 {}：{}", next_trade_day, exc)
                next_daytrade_cache[next_trade_day] = {}

        day_key = day.isoformat()
        changed = 0
        header_changed = any(
            col not in (reader.fieldnames or [])
            for col in ("next_can_daytrade", "next_daytrade_type", "next_daytrade_suspension_flag")
        )
        for row in rows:
            code = cleanup_cell(row.get("stock_no", ""))
            factor = factor_lookup.get(code, {}).get(day_key, 1.0)
            new_factor = f"{factor:.12f}"
            price_open = parse_float_cell(row.get("price_open", ""))
            price_high = parse_float_cell(row.get("price_high", ""))
            price_low = parse_float_cell(row.get("price_low", ""))
            price_close = parse_float_cell(row.get("price_close", ""))
            new_open = f"{price_open * factor:.4f}" if price_open is not None else ""
            new_high = f"{price_high * factor:.4f}" if price_high is not None else ""
            new_low = f"{price_low * factor:.4f}" if price_low is not None else ""
            new_close = f"{price_close * factor:.4f}" if price_close is not None else ""

            next_trade_day = cleanup_cell(row.get("next_trade_date", ""))
            if next_trade_day:
                next_daytrade = next_daytrade_cache.get(next_trade_day, {}).get(code, {})
                if next_daytrade:
                    new_next_can_daytrade = cleanup_cell(next_daytrade.get("can_daytrade", "Y"))
                    new_next_daytrade_type = cleanup_cell(
                        next_daytrade.get("daytrade_type", "雙向皆可")
                    )
                    new_next_daytrade_flag = cleanup_cell(
                        next_daytrade.get("daytrade_suspension_flag", "")
                    )
                else:
                    new_next_can_daytrade = "N"
                    new_next_daytrade_type = "不可當沖"
                    new_next_daytrade_flag = ""
            else:
                new_next_can_daytrade = ""
                new_next_daytrade_type = ""
                new_next_daytrade_flag = ""

            if (
                row.get("adj_factor_back", "") != new_factor
                or row.get("adj_open", "") != new_open
                or row.get("adj_high", "") != new_high
                or row.get("adj_low", "") != new_low
                or row.get("adj_close", "") != new_close
                or row.get("next_can_daytrade", "") != new_next_can_daytrade
                or row.get("next_daytrade_type", "") != new_next_daytrade_type
                or row.get("next_daytrade_suspension_flag", "") != new_next_daytrade_flag
            ):
                row["adj_factor_back"] = new_factor
                row["adj_open"] = new_open
                row["adj_high"] = new_high
                row["adj_low"] = new_low
                row["adj_close"] = new_close
                row["next_can_daytrade"] = new_next_can_daytrade
                row["next_daytrade_type"] = new_next_daytrade_type
                row["next_daytrade_suspension_flag"] = new_next_daytrade_flag
                changed += 1
            else:
                row["next_can_daytrade"] = new_next_can_daytrade
                row["next_daytrade_type"] = new_next_daytrade_type
                row["next_daytrade_suspension_flag"] = new_next_daytrade_flag

        if changed <= 0 and not header_changed:
            continue

        with path.open("w", newline="", encoding="utf-8-sig") as fp:
            writer = csv.DictWriter(fp, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        updated_files += 1
        updated_rows += changed

    logger.success(
        "還原回補完成：更新檔案={}，更新列數={}",
        updated_files,
        updated_rows,
    )


def run_market_range_all(client: TwseClient, args: argparse.Namespace) -> None:
    end = resolve_end_date(client, args.end)
    ensure_range(args.start, end)
    args.out_dir.mkdir(parents=True, exist_ok=True)

    total_days = 0
    written_days = 0
    skipped_days = 0
    empty_days = 0
    failed_days = 0
    pending_days: list[tuple[date, Path]] = []

    trading_days = [day for day in iter_days(args.start, end) if day.weekday() < 5]
    total_days = len(trading_days)
    for trading_day in progress(
        trading_days,
        desc="掃描既有檔案",
        total=total_days,
    ):
        out_file = args.out_dir / f"market_day_all_{trading_day.isoformat()}.csv"
        if out_file.exists() and not args.overwrite:
            skipped_days += 1
            continue
        pending_days.append((trading_day, out_file))

    if not pending_days:
        logger.info(
            "market-range-all 下載階段完成：交易日={}，新寫入={}，已存在略過={}，無資料={}，失敗={}",
            total_days,
            written_days,
            skipped_days,
            empty_days,
            failed_days,
        )
        if not args.skip_adj_backfill:
            run_market_adj_backfill(client, args.out_dir)
        return

    logger.info("開始抓取全市場：待下載交易日 {}", len(pending_days))
    listed_rows = client.fetch_listed_companies()
    sbl_map = client.fetch_sbl_available_all()

    for trading_day, out_file in progress(
        pending_days,
        desc="下載全市場資料",
        total=len(pending_days),
    ):
        try:
            rows = build_market_day_all_rows(
                client=client,
                trading_day=trading_day,
                listed_rows=listed_rows,
                sbl_map=sbl_map,
            )
            if not rows:
                empty_days += 1
                logger.warning("{} 無資料，略過輸出", trading_day.isoformat())
                continue
            write_csv(out_file, rows)
            written_days += 1
        except Exception as exc:  # noqa: BLE001
            failed_days += 1
            logger.error("{} 下載失敗：{}", trading_day.isoformat(), exc)
            if not args.continue_on_error:
                raise

    logger.success(
        "market-range-all 下載階段完成：交易日={}，新寫入={}，已存在略過={}，無資料={}，失敗={}",
        total_days,
        written_days,
        skipped_days,
        empty_days,
        failed_days,
    )
    if args.skip_adj_backfill:
        logger.warning("已指定 --skip-adj-backfill，略過還原回補")
    else:
        run_market_adj_backfill(client, args.out_dir)


def run_turnover(client: TwseClient, args: argparse.Namespace) -> None:
    end = resolve_end_date(client, args.end)
    ensure_range(args.start, end)
    fallback_shares = (
        args.shares if args.shares and args.shares > 0 else client.fetch_issued_shares(args.stock)
    )
    shares_cache: dict[str, tuple[int, str]] = {}
    rows: list[dict[str, Any]] = []

    months = iter_month_starts(args.start, end)
    for month in progress(
        months,
        desc=f"turnover {args.stock} 月資料",
        total=len(months),
    ):
        month_rows = client.fetch_price_month(args.stock, month)
        for row in month_rows:
            d = parse_cli_date(row["date"])
            if not (args.start <= d <= end):
                continue
            day_key = d.isoformat()
            if day_key not in shares_cache:
                daily_shares = client.fetch_issued_shares_day(args.stock, d)
                if daily_shares and daily_shares > 0:
                    shares_cache[day_key] = (daily_shares, "MI_QFIIS")
                else:
                    shares_cache[day_key] = (fallback_shares, "t187ap03_L/--shares")

            issued_shares, shares_source = shares_cache[day_key]
            volume = parse_int_cell(row["volume"])
            turnover = (volume / issued_shares * 100) if issued_shares else 0.0
            rows.append(
                {
                    "date": row["date"],
                    "stock_no": row["stock_no"],
                    "volume": row["volume"],
                    "issued_shares": str(issued_shares),
                    "issued_shares_source": shares_source,
                    "turnover_ratio_pct": f"{turnover:.6f}",
                }
            )

    rows.sort(key=lambda row: row["date"])
    write_csv(args.out, rows)


def run_all_in_one(client: TwseClient, args: argparse.Namespace) -> None:
    end = resolve_end_date(client, args.end)
    ensure_range(args.start, end)
    fallback_shares = (
        args.shares if args.shares and args.shares > 0 else client.fetch_issued_shares(args.stock)
    )
    listed_rows = client.fetch_listed_companies()
    industry_code = ""
    for row in listed_rows:
        if cleanup_cell(row.get("公司代號", "")) == args.stock:
            industry_code = cleanup_cell(row.get("產業別", ""))
            break

    sbl_latest_volume = ""
    sbl_rows = client.fetch_sbl_available(args.stock)
    if sbl_rows:
        sbl_latest_volume = sbl_rows[0].get("sbl_available_volume", "")

    price_rows: list[dict[str, Any]] = []
    months = iter_month_starts(args.start, end)
    for month in progress(
        months,
        desc=f"all-in-one {args.stock} 月股價",
        total=len(months),
    ):
        for row in client.fetch_price_month(args.stock, month):
            d = parse_cli_date(row["date"])
            if args.start <= d <= end:
                price_rows.append(row)
    price_rows.sort(key=lambda row: row["date"])
    exright_events = client.fetch_exright_results(args.stock, args.start, end)
    adjusted_price_rows = apply_back_adjustment(price_rows, exright_events)
    adjusted_by_date = {row["date"]: row for row in adjusted_price_rows}

    next_day_price_map: dict[str, dict[str, str]] = {}
    variation_end = end + timedelta(days=10)
    variation_days = [day for day in iter_days(args.start, variation_end) if day.weekday() < 5]
    for day in progress(
        variation_days,
        desc=f"all-in-one {args.stock} 次日參考價",
        total=len(variation_days),
    ):
        variation = client.fetch_variation_day(args.stock, day)
        if not variation:
            continue
        last_day = cleanup_cell(variation.get("last_trading_day", ""))
        if not last_day:
            continue
        last_date = parse_cli_date(last_day)
        if not (args.start <= last_date <= end):
            continue
        next_daytrade_row = first_row(client.fetch_daytrade_day(args.stock, day))
        next_can_daytrade, next_daytrade_type = resolve_daytrade_status(next_daytrade_row)
        next_day_price_map[last_day] = {
            "next_trade_date": cleanup_cell(variation.get("query_date", "")),
            "next_open_ref_price": cleanup_cell(variation.get("today_open_ref", "")),
            "next_limit_up_price": cleanup_cell(variation.get("today_limit_up", "")),
            "next_limit_down_price": cleanup_cell(variation.get("today_limit_down", "")),
            "next_can_daytrade": next_can_daytrade,
            "next_daytrade_type": next_daytrade_type,
            "next_daytrade_suspension_flag": cleanup_cell(
                next_daytrade_row.get("suspension_flag", "")
            ),
        }

    merged_rows: list[dict[str, Any]] = []
    for price in progress(
        price_rows,
        desc=f"all-in-one {args.stock} 每日整併",
        total=len(price_rows),
    ):
        trading_day = parse_cli_date(price["date"])
        adjusted = adjusted_by_date.get(price["date"], {})
        next_day_price = next_day_price_map.get(price["date"], {})
        margin = first_row(client.fetch_margin_day(args.stock, trading_day))
        daytrade = first_row(client.fetch_daytrade_day(args.stock, trading_day))
        quota = first_row(client.fetch_credit_quota_day(args.stock, trading_day))
        can_daytrade, daytrade_type = resolve_daytrade_status(daytrade)

        daily_shares = client.fetch_issued_shares_day(args.stock, trading_day)
        if daily_shares and daily_shares > 0:
            issued_shares = daily_shares
            shares_source = "MI_QFIIS"
        else:
            issued_shares = fallback_shares
            shares_source = "t187ap03_L/--shares"

        turnover = (
            parse_int_cell(price.get("volume", "0")) / issued_shares * 100
            if issued_shares
            else 0.0
        )

        merged_rows.append(
            {
                "date": price["date"],
                "stock_no": args.stock,
                "stock_name": (
                    margin.get("stock_name")
                    or daytrade.get("stock_name")
                    or quota.get("stock_name")
                    or ""
                ),
                "industry_code": industry_code,
                "price_volume": price.get("volume", ""),
                "price_value": price.get("value", ""),
                "price_open": price.get("open", ""),
                "price_high": price.get("high", ""),
                "price_low": price.get("low", ""),
                "price_close": price.get("close", ""),
                "price_change": price.get("change", ""),
                "price_transactions": price.get("transactions", ""),
                "price_note": price.get("note", ""),
                "adj_factor_back": adjusted.get("adj_factor_back", "1.000000000000"),
                "adj_open": adjusted.get("adj_open", ""),
                "adj_high": adjusted.get("adj_high", ""),
                "adj_low": adjusted.get("adj_low", ""),
                "adj_close": adjusted.get("adj_close", ""),
                "next_trade_date": next_day_price.get("next_trade_date", ""),
                "next_open_ref_price": next_day_price.get("next_open_ref_price", ""),
                "next_limit_up_price": next_day_price.get("next_limit_up_price", ""),
                "next_limit_down_price": next_day_price.get("next_limit_down_price", ""),
                "next_can_daytrade": next_day_price.get("next_can_daytrade", ""),
                "next_daytrade_type": next_day_price.get("next_daytrade_type", ""),
                "next_daytrade_suspension_flag": next_day_price.get(
                    "next_daytrade_suspension_flag", ""
                ),
                "margn_margin_buy": margin.get("margin_buy", ""),
                "margn_margin_sell": margin.get("margin_sell", ""),
                "margn_margin_cash_repayment": margin.get("margin_cash_repayment", ""),
                "margn_margin_prev_balance": margin.get("margin_prev_balance", ""),
                "margn_margin_balance": margin.get("margin_balance", ""),
                "margn_short_buy": margin.get("short_buy", ""),
                "margn_short_sell": margin.get("short_sell", ""),
                "margn_short_cash_repayment": margin.get("short_cash_repayment", ""),
                "margn_short_prev_balance": margin.get("short_prev_balance", ""),
                "margn_short_balance": margin.get("short_balance", ""),
                "margn_offset": margin.get("offset", ""),
                "margn_note": margin.get("note", ""),
                "can_daytrade": can_daytrade,
                "daytrade_type": daytrade_type,
                "daytrade_suspension_flag": daytrade.get("suspension_flag", ""),
                "daytrade_volume": daytrade.get("daytrade_volume", ""),
                "daytrade_buy_value": daytrade.get("daytrade_buy_value", ""),
                "daytrade_sell_value": daytrade.get("daytrade_sell_value", ""),
                "quota_margin_short_prev_balance": quota.get("margin_short_prev_balance", ""),
                "quota_margin_short_sell": quota.get("margin_short_sell", ""),
                "quota_margin_short_buy": quota.get("margin_short_buy", ""),
                "quota_margin_short_cash_repayment": quota.get("margin_short_cash_repayment", ""),
                "quota_margin_short_balance": quota.get("margin_short_balance", ""),
                "quota_margin_short_next_limit": quota.get("margin_short_next_limit", ""),
                "quota_sbl_prev_balance": quota.get("sbl_prev_balance", ""),
                "quota_sbl_sell": quota.get("sbl_sell", ""),
                "quota_sbl_return": quota.get("sbl_return", ""),
                "quota_sbl_adjust": quota.get("sbl_adjust", ""),
                "quota_sbl_balance": quota.get("sbl_balance", ""),
                "quota_sbl_next_limit": quota.get("sbl_next_limit", ""),
                "quota_note": quota.get("note", ""),
                "issued_shares": str(issued_shares),
                "issued_shares_source": shares_source,
                "turnover_ratio_pct": f"{turnover:.6f}",
                "sbl_available_volume_latest": sbl_latest_volume,
            }
        )

    write_csv(args.out, merged_rows)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="TWSE API 抓取工具：股價(含還原)、信用交易、周轉率、上市公司清單"
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=1.0,
        help="每次請求後休眠秒數，避免打太快被限流 (預設: 1.0)",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["TRACE", "DEBUG", "INFO", "SUCCESS", "WARNING", "ERROR"],
        help="loguru 日誌等級 (預設: INFO)",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    price_parser = subparsers.add_parser("price", help="抓個股歷史日線 (STOCK_DAY)")
    price_parser.add_argument("--stock", required=True, help="股票代號，例如 2330")
    price_parser.add_argument("--start", type=parse_cli_date, required=True, help="起始日 YYYY-MM-DD")
    price_parser.add_argument(
        "--end",
        type=parse_cli_date,
        help="結束日 YYYY-MM-DD（可省略，預設最近交易日）",
    )
    price_parser.add_argument("--out", type=Path, required=True, help="輸出 CSV 路徑")

    adj_price_parser = subparsers.add_parser(
        "adjusted-price",
        help="抓個股還原股價 (STOCK_DAY + TWT49U，前復權)",
    )
    adj_price_parser.add_argument("--stock", required=True, help="股票代號，例如 2330")
    adj_price_parser.add_argument("--start", type=parse_cli_date, required=True, help="起始日 YYYY-MM-DD")
    adj_price_parser.add_argument(
        "--end",
        type=parse_cli_date,
        help="結束日 YYYY-MM-DD（可省略，預設最近交易日）",
    )
    adj_price_parser.add_argument("--out", type=Path, required=True, help="輸出 CSV 路徑")

    margin_parser = subparsers.add_parser("margin", help="抓融資融券 (MI_MARGN)")
    margin_parser.add_argument("--stock", required=True, help="股票代號，例如 2330")
    margin_parser.add_argument("--start", type=parse_cli_date, required=True, help="起始日 YYYY-MM-DD")
    margin_parser.add_argument(
        "--end",
        type=parse_cli_date,
        help="結束日 YYYY-MM-DD（可省略，預設最近交易日）",
    )
    margin_parser.add_argument("--out", type=Path, required=True, help="輸出 CSV 路徑")

    daytrade_parser = subparsers.add_parser("daytrade", help="抓當沖成交資訊 (TWTB4U)")
    daytrade_parser.add_argument("--stock", required=True, help="股票代號，例如 2330")
    daytrade_parser.add_argument("--start", type=parse_cli_date, required=True, help="起始日 YYYY-MM-DD")
    daytrade_parser.add_argument(
        "--end",
        type=parse_cli_date,
        help="結束日 YYYY-MM-DD（可省略，預設最近交易日）",
    )
    daytrade_parser.add_argument("--out", type=Path, required=True, help="輸出 CSV 路徑")

    credit_parser = subparsers.add_parser(
        "credit-quota",
        help="抓信用額度總量管制餘額表 (TWT93U，含融券/借券賣出餘額)",
    )
    credit_parser.add_argument("--stock", required=True, help="股票代號，例如 2330")
    credit_parser.add_argument("--start", type=parse_cli_date, required=True, help="起始日 YYYY-MM-DD")
    credit_parser.add_argument(
        "--end",
        type=parse_cli_date,
        help="結束日 YYYY-MM-DD（可省略，預設最近交易日）",
    )
    credit_parser.add_argument("--out", type=Path, required=True, help="輸出 CSV 路徑")

    sbl_parser = subparsers.add_parser("sbl", help="抓上市可借券賣出股數快照 (TWT96U)")
    sbl_parser.add_argument("--stock", required=True, help="股票代號，例如 2330")
    sbl_parser.add_argument("--out", type=Path, required=True, help="輸出 CSV 路徑")

    listed_parser = subparsers.add_parser("listed-companies", help="抓所有上市公司清單 (t187ap03_L)")
    listed_parser.add_argument("--out", type=Path, required=True, help="輸出 CSV 路徑")

    market_day_parser = subparsers.add_parser(
        "market-day-all",
        help="抓單日全市場上市公司整合資料",
    )
    market_day_parser.add_argument("--date", type=parse_cli_date, required=True, help="日期 YYYY-MM-DD")
    market_day_parser.add_argument("--out", type=Path, required=True, help="輸出 CSV 路徑")

    market_range_parser = subparsers.add_parser(
        "market-range-all",
        help="抓日期區間全市場上市公司整合資料（已存在檔案會略過）",
    )
    market_range_parser.add_argument(
        "--start", type=parse_cli_date, required=True, help="起始日 YYYY-MM-DD"
    )
    market_range_parser.add_argument(
        "--end",
        type=parse_cli_date,
        help="結束日 YYYY-MM-DD（可省略，預設最近交易日）",
    )
    market_range_parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path("data"),
        help="輸出資料夾（預設: data）",
    )
    market_range_parser.add_argument(
        "--overwrite",
        action="store_true",
        help="若檔案已存在仍重新下載覆蓋",
    )
    market_range_parser.add_argument(
        "--continue-on-error",
        action="store_true",
        help="某日下載失敗時，繼續下載下一天",
    )
    market_range_parser.add_argument(
        "--skip-adj-backfill",
        action="store_true",
        help="略過 market_day_all 的還原股價(adj_*)回補",
    )

    turnover_parser = subparsers.add_parser(
        "turnover",
        help="計算個股每日周轉率 (STOCK_DAY + MI_QFIIS + fallback t187ap03_L)",
    )
    turnover_parser.add_argument("--stock", required=True, help="股票代號，例如 2330")
    turnover_parser.add_argument("--start", type=parse_cli_date, required=True, help="起始日 YYYY-MM-DD")
    turnover_parser.add_argument(
        "--end",
        type=parse_cli_date,
        help="結束日 YYYY-MM-DD（可省略，預設最近交易日）",
    )
    turnover_parser.add_argument("--out", type=Path, required=True, help="輸出 CSV 路徑")
    turnover_parser.add_argument(
        "--shares",
        type=int,
        help="可選：自行指定流通股數。未提供時使用 t187ap03_L 的已發行普通股數",
    )

    all_parser = subparsers.add_parser(
        "all-in-one",
        help="整合股價(含還原/次一交易日價位)/信用交易/周轉率到單一 CSV",
    )
    all_parser.add_argument("--stock", required=True, help="股票代號，例如 2330")
    all_parser.add_argument("--start", type=parse_cli_date, required=True, help="起始日 YYYY-MM-DD")
    all_parser.add_argument(
        "--end",
        type=parse_cli_date,
        help="結束日 YYYY-MM-DD（可省略，預設最近交易日）",
    )
    all_parser.add_argument("--out", type=Path, required=True, help="輸出 CSV 路徑")
    all_parser.add_argument(
        "--shares",
        type=int,
        help="可選：自行指定流通股數。未提供時使用 t187ap03_L 的已發行普通股數",
    )

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    configure_logger(args.log_level)
    client = TwseClient(delay=args.delay)

    try:
        if args.command == "price":
            run_price(client, args)
        elif args.command == "adjusted-price":
            run_adjusted_price(client, args)
        elif args.command == "margin":
            run_margin(client, args)
        elif args.command == "daytrade":
            run_daytrade(client, args)
        elif args.command == "credit-quota":
            run_credit_quota(client, args)
        elif args.command == "sbl":
            run_sbl(client, args)
        elif args.command == "listed-companies":
            run_listed_companies(client, args)
        elif args.command == "market-day-all":
            run_market_day_all(client, args)
        elif args.command == "market-range-all":
            run_market_range_all(client, args)
        elif args.command == "turnover":
            run_turnover(client, args)
        elif args.command == "all-in-one":
            run_all_in_one(client, args)
    finally:
        client.close()


if __name__ == "__main__":
    main()
