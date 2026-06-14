# ─────────────────────────────────────────────────────────────
# OI INTRADAY STRATEGY — PAPER TRADING (FINAL FIXED)
# ─────────────────────────────────────────────────────────────

import os
import sys
import csv
import json
import math
import time
import logging
import threading
import datetime
import traceback
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import requests
from scipy.stats import norm

# Live Tick
from market.tick_subscriber import TickSubscriber

# ══════════════════════════════════════════════════════════════
# PATHS & CONFIG
# ══════════════════════════════════════════════════════════════

BASE_DIR    = os.path.dirname(os.path.abspath(__file__))
SCRIPT_DIR  = os.path.join(BASE_DIR, "strategies", "intra_oi")
CONFIG_FILE = os.path.join(SCRIPT_DIR, "intra_oi_config.json")
LOG_FILE    = os.path.join(SCRIPT_DIR, "log_intra_oi.log")
CSV_DIR     = os.path.join(BASE_DIR, "pcr")

os.makedirs(SCRIPT_DIR, exist_ok=True)
sys.stdout.reconfigure(encoding="utf-8")


def load_config() -> dict:
    path = Path(CONFIG_FILE)
    if not path.is_file():
        raise FileNotFoundError(f"Config not found: {CONFIG_FILE}")
    with open(path, encoding="utf-8") as f:
        return json.load(f)


CONFIG = load_config()

SYMBOL        = CONFIG["symbol"]
PAPER_TRADING = CONFIG.get("paper_trading", True)
PAPER_LOT     = CONFIG.get("paper_lot", 4)

_tg                = CONFIG.get("telegram", {})
TELEGRAM_ENABLED   = _tg.get("enabled", False)
TELEGRAM_BOT_TOKEN = _tg.get("bot_token", "")
TELEGRAM_CHAT_ID   = _tg.get("chat_id", "")

OPTION_CHAIN_URL = CONFIG["urls"]["option_chain_url"]
NSE_HOME         = "https://www.nseindia.com/"

_mkt         = CONFIG["market"]
MARKET_START = _mkt.get("start_time", "09:15")
MARKET_END   = _mkt.get("end_time",   "15:30")
WEEKLY_OFF   = set(_mkt.get("weekly_off_days", ["SAT", "SUN"]))

DELTA_THRESHOLD = 0.30
RISK_FREE       = 0.065

TRADES_CSV_B_PATH = os.path.join(SCRIPT_DIR, f"trades_buy_{SYMBOL}.csv")
TRADES_CSV_S_PATH = os.path.join(SCRIPT_DIR, f"trades_sell_{SYMBOL}.csv")
MATCHED_CSV_PATH  = CONFIG.get("matched_csv_path", "")

# ══════════════════════════════════════════════════════════════
# LOGGING + GLOBALS
# ══════════════════════════════════════════════════════════════

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s",
                    handlers=[logging.FileHandler(LOG_FILE, encoding="utf-8"), logging.StreamHandler()])
logger = logging.getLogger(__name__)

shutdown_event = threading.Event()
live_ltp: Dict[str, float] = {}
ltp_lock = threading.Lock()

# ══════════════════════════════════════════════════════════════
# DYNAMIC SCRIP MASTER
# ══════════════════════════════════════════════════════════════

scrip_master: List[dict] = []
lot_size_map: Dict[Tuple[str, int, str], int] = {}
current_weekly: Optional[str] = None
current_monthly: Optional[str] = None
next_monthly: Optional[str] = None
current_lot_size: int = 65

def _parse_scrip_expiry(exp_str: str) -> Optional[datetime.date]:
    if not exp_str: return None
    try:
        if len(exp_str) >= 9 and exp_str[2:5].isalpha():
            return datetime.datetime.strptime(exp_str[:9], "%d%b%Y").date()
        return datetime.datetime.strptime(exp_str.split("T")[0], "%Y-%m-%d").date()
    except Exception:
        return None


def build_scrip_master():
    global scrip_master, lot_size_map, current_weekly, current_monthly, next_monthly, current_lot_size
    scrip_master.clear()
    lot_size_map.clear()

    if not MATCHED_CSV_PATH or not os.path.isfile(MATCHED_CSV_PATH):
        logger.warning("Matched CSV not found")
        return

    with open(MATCHED_CSV_PATH, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                instr = {
                    "token": row.get("token"),
                    "name": row.get("a_name"),
                    "instrumenttype": row.get("a_instrumenttype"),
                    "expiry": row.get("a_expiry"),
                    "strike": row.get("a_strike"),
                    "lotsize": int(row.get("a_lotsize") or 0),
                    "symbol": row.get("a_symbol"),
                }
                if instr["token"] and instr["name"]:
                    scrip_master.append(instr)
            except Exception:
                continue

    logger.info(f"Scrip master loaded: {len(scrip_master)} records")

    # Collect all NIFTY OPTIDX expiries
    expiry_dict = {}
    for instr in scrip_master:
        if instr.get("name") != SYMBOL or instr.get("instrumenttype") != "OPTIDX":
            continue
        exp_str = instr.get("a_expiry", "")
        exp_date = _parse_scrip_expiry(exp_str)
        if not exp_date:
            continue
        lot = int(instr.get("lotsize") or 0)
        if exp_str not in expiry_dict or exp_date > expiry_dict[exp_str][0]:
            expiry_dict[exp_str] = (exp_date, lot)

    all_exps = sorted(expiry_dict.items(), key=lambda x: x[1][0])

    if len(all_exps) >= 3:
        current_weekly = all_exps[0][0]
        # Find the monthly (usually the 3rd or one with higher date)
        current_monthly = all_exps[2][0] if len(all_exps) > 2 else all_exps[1][0]
        next_monthly = all_exps[3][0] if len(all_exps) > 3 else all_exps[2][0]
        current_lot_size = all_exps[0][1][1] or 65

        logger.info(f"Expiries → Weekly: {current_weekly} | Monthly: {current_monthly} | Next Monthly: {next_monthly}")
    elif all_exps:
        current_weekly = all_exps[0][0]
        current_monthly = all_exps[-1][0]
        next_monthly = all_exps[-1][0]
        logger.info(f"Fallback Expiries → Weekly: {current_weekly} | Monthly: {current_monthly}")


def lookup_lot_size(expiry: str, strike: int, side: str) -> int:
    # Simple fallback for now
    return current_lot_size


def get_qty(expiry: str, strike: int, side: str) -> int:
    return PAPER_LOT * lookup_lot_size(expiry, strike, side)


# ══════════════════════════════════════════════════════════════
# EXPIRY LOGIC — EXACT TABLE MATCH
# ══════════════════════════════════════════════════════════════

def is_in_last_8_days() -> bool:
    if not current_weekly: return False
    try:
        exp_date = _parse_scrip_expiry(current_weekly)
        return datetime.date.today() >= (exp_date - datetime.timedelta(days=8))
    except:
        return False


def get_current_weekly_expiry(today: datetime.date) -> Optional[str]:
    return current_weekly


def get_sell_trade_expiry(today: datetime.date) -> str:
    """Exactly matches your table with 8-day switch rule"""
    if not current_monthly:
        return current_weekly or ""

    if is_in_last_8_days():
        return next_monthly or current_monthly
    return current_monthly


# ══════════════════════════════════════════════════════════════
# TELEGRAM + TICK BRIDGE
# ══════════════════════════════════════════════════════════════

def send_telegram(text: str):
    if not TELEGRAM_ENABLED:
        logger.info(f"[TG] {text}")
        return
    try:
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                      data={"chat_id": TELEGRAM_CHAT_ID, "text": text}, timeout=5)
    except Exception as e:
        logger.error(f"Telegram error: {e}")


class TraderBridge:
    def __init__(self):
        self._last_tick_time = None
        self._watchdog_started = False

    def update_ltp(self, token, ltp):
        global live_ltp
        token = str(token)
        self._last_tick_time = time.time()
        with ltp_lock:
            live_ltp[token] = float(ltp)
        logger.debug(f"[TICK] {token} | {float(ltp):.2f}")

    def _tick_watchdog(self):
        # ... your watchdog code ...
        pass   # keep as is


tick_bridge = TraderBridge()
tick_subscriber = TickSubscriber(trader=tick_bridge)   # Correct way
tick_subscriber.start()

# ══════════════════════════════════════════════════════════════
# STRIKE CALCULATION
# ══════════════════════════════════════════════════════════════

def get_otm_strikes(nifty_spot: float) -> Tuple[int, int]:
    """
    Return (ce_strike, pe_strike) for a given Nifty spot price.

    Rule:
      last two digits ≤ 20  → both strikes = lower 100-multiple
      last two digits ≥ 80  → both strikes = upper 100-multiple
      otherwise             → CE = upper, PE = lower

    Examples:
      24100 → (24100, 24100)
      24120 → (24100, 24100)
      24121 → (24200, 24100)
      24180 → (24200, 24200)
    """
    lower    = (int(nifty_spot) // 100) * 100
    upper    = lower + 100
    last_two = int(nifty_spot) % 100

    if last_two <= 20:
        return lower, lower
    elif last_two >= 80:
        return upper, upper
    else:
        return upper, lower


# ══════════════════════════════════════════════════════════════
# SIGNAL CALCULATION
# ══════════════════════════════════════════════════════════════

def evaluate_oi_ltp(
    base_oi:  Optional[float],
    base_ltp: Optional[float],
    cur_oi:   Optional[float],
    cur_ltp:  Optional[float],
) -> str:
    """
    Compare current OI and LTP vs the 9:30 AM base.
    Returns one of: "LONG", "SHORT", "SHORT COVER", "LONG UNWIND", or "".

    OI ↑  LTP ↑  → LONG          (new longs being added)
    OI ↑  LTP ↓  → SHORT         (new shorts being added)
    OI ↓  LTP ↑  → SHORT COVER   (shorts being closed)
    OI ↓  LTP ↓  → LONG UNWIND   (longs being closed)
    """
    if any(v is None for v in [base_oi, base_ltp, cur_oi, cur_ltp]):
        return ""

    oi_change  = cur_oi  - base_oi
    ltp_change = cur_ltp - base_ltp

    if oi_change > 0 and ltp_change > 0:
        return "LONG"
    if oi_change > 0 and ltp_change < 0:
        return "SHORT"
    if oi_change < 0 and ltp_change > 0:
        return "SHORT COVER"
    if oi_change < 0 and ltp_change < 0:
        return "LONG UNWIND"
    return ""


# ══════════════════════════════════════════════════════════════
# SIGNAL CONDITION HELPERS
# ══════════════════════════════════════════════════════════════

BULLISH = {"LONG", "SHORT COVER"}   # Bullish signals
BEARISH = {"SHORT", "LONG UNWIND"}  # Bearish signals


def is_buy_entry(pre_val: str, cur_val: str) -> bool:
    """
    Buy Entry: 2 consecutive bullish signals.
    Valid: LONG+LONG, SC+SC, LONG+SC, SC+LONG
    """
    return pre_val in BULLISH and cur_val in BULLISH


def is_buy_exit(pre_val: str, cur_val: str) -> bool:
    """
    Buy Exit: 2 consecutive bearish signals.
    Valid: SHORT+SHORT, LU+LU, SHORT+LU, LU+SHORT
    """
    return pre_val in BEARISH and cur_val in BEARISH


def is_sell_weekly_trigger(pre_val: str, cur_val: str) -> bool:
    """
    Sell Entry Weekly Trigger — STRICT.
    Only: SHORT + SHORT
    """
    return pre_val == "SHORT" and cur_val == "SHORT"


def is_sell_monthly_confirm(pre_val: str, cur_val: str) -> bool:
    """
    Sell Entry Monthly Confirmation — relaxed.
    Valid: SHORT+SHORT, SHORT+LU, LU+LU, LU+SHORT
    """
    return pre_val in BEARISH and cur_val in BEARISH


def is_sell_exit(pre_val: str, cur_val: str) -> bool:
    """
    Sell Exit: 2 consecutive bullish signals on the monthly contract.
    Valid: LONG+LONG, SC+SC, LONG+SC, SC+LONG
    """
    return pre_val in BULLISH and cur_val in BULLISH


# ══════════════════════════════════════════════════════════════
# NSE SESSION (for option chain Greeks)
# ══════════════════════════════════════════════════════════════

nse_session = None
nse_headers: Dict = {}


def init_nse_session():
    """Warm up the NSE session by visiting the home page first."""
    global nse_session, nse_headers
    nse_session = requests.Session()
    nse_headers = {
        "user-agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
            " AppleWebKit/537.36 (KHTML, like Gecko)"
            " Chrome/124.0.0.0 Safari/537.36"
        ),
        "referer":         "https://www.nseindia.com/option-chain",
        "accept":          "application/json, text/plain, */*",
        "accept-language": "en-US,en;q=0.9",
    }
    logger.info("Warming up NSE session…")
    try:
        nse_session.get(NSE_HOME, headers=nse_headers, timeout=15)
        time.sleep(1.5)
        logger.info("NSE session ready.")
    except Exception as e:
        logger.warning(f"NSE session warmup failed: {e}")


def fetch_option_chain_json(expiry: str) -> dict:
    """Fetch option chain data from NSE for a given expiry."""
    url = OPTION_CHAIN_URL.format(symbol=SYMBOL, expiry=expiry)
    r   = nse_session.get(url, headers=nse_headers, timeout=15)
    r.raise_for_status()
    return r.json()


# ══════════════════════════════════════════════════════════════
# BLACK-SCHOLES GREEKS
# ══════════════════════════════════════════════════════════════

def get_delta(S: float, K: float, T: float, r: float, sigma: float, option_type: str = "CE") -> Optional[float]:
    """
    Calculate Black-Scholes delta for a single option.

    S     = Spot price
    K     = Strike price
    T     = Time to expiry in years
    r     = Risk-free rate
    sigma = Implied volatility (as decimal, e.g. 0.20 for 20%)
    option_type = "CE" or "PE"

    Returns delta or None if inputs are invalid.
    """
    if T <= 0 or sigma <= 0 or S <= 0 or K <= 0:
        return None
    d1    = (math.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * math.sqrt(T))
    if option_type == "CE":
        return round(norm.cdf(d1), 4)
    else:
        return round(norm.cdf(d1) - 1, 4)


def fetch_greeks_for_expiry(expiry: str) -> Optional[pd.DataFrame]:
    """
    Fetch NSE option chain for the given expiry date.
    Calculate Black-Scholes delta for every strike.

    Returns a DataFrame with columns:
        strike, ce_iv, ce_ltp, ce_delta, pe_iv, pe_ltp, pe_delta
    Returns None on failure.
    """
    try:
        json_data = fetch_option_chain_json(expiry)
    except Exception as e:
        logger.error(f"Option chain fetch failed for {expiry}: {e}")
        return None

    records_raw   = json_data["records"]["data"]
    spot          = float(json_data["records"]["underlyingValue"])
    timestamp_str = json_data["records"]["timestamp"]            # "15-May-2026 15:30:00"

    today_dt  = datetime.datetime.strptime(timestamp_str, "%d-%b-%Y %H:%M:%S")
    expiry_dt = datetime.datetime.strptime(expiry, "%d-%b-%Y")
    days_left = max((expiry_dt - today_dt).days, 0)
    T         = days_left / 365

    rows = []
    for item in records_raw:
        strike = float(item["strikePrice"])
        ce     = item.get("CE") or {}
        pe     = item.get("PE") or {}

        ce_iv  = ce.get("impliedVolatility") or None
        ce_ltp = float(ce.get("lastPrice", 0))
        pe_iv  = pe.get("impliedVolatility") or None
        pe_ltp = float(pe.get("lastPrice", 0))

        ce_delta = pe_delta = None
        if ce_iv and ce_iv > 0:
            ce_delta = get_delta(spot, strike, T, RISK_FREE, ce_iv / 100, "CE")
        if pe_iv and pe_iv > 0:
            pe_delta = get_delta(spot, strike, T, RISK_FREE, pe_iv / 100, "PE")

        rows.append({
            "strike":   int(strike),
            "ce_iv":    ce_iv,
            "ce_ltp":   ce_ltp,
            "ce_delta": ce_delta,
            "pe_iv":    pe_iv,
            "pe_ltp":   pe_ltp,
            "pe_delta": pe_delta,
        })

    df = pd.DataFrame(rows)
    logger.info(f"Greeks fetched | expiry={expiry} | spot={spot} | T={T:.4f}y | {len(df)} strikes")
    return df


def find_delta_strike(greeks_df: pd.DataFrame, side: str) -> Optional[int]:
    """
    From the monthly expiry option chain, find the strike whose delta is:
      - ≤ 0.30 for CE (closest to 0.30 from below)
      - ≥ -0.30 for PE (closest to -0.30 from above, i.e. least negative)

    Only considers strikes that are multiples of 100.
    Returns the strike as an integer, or None if not found.
    """
    if side.upper() == "CE":
        subset = greeks_df[
            greeks_df["ce_delta"].notna() &
            (greeks_df["ce_delta"] <= DELTA_THRESHOLD) &
            (greeks_df["ce_delta"] > 0) &
            (greeks_df["strike"] % 100 == 0)
        ]
        if subset.empty:
            logger.warning(f"CE: No strike found with delta ≤ {DELTA_THRESHOLD}")
            return None
        best = subset.loc[subset["ce_delta"].idxmax()]  # closest to 0.30

    else:  # PE
        subset = greeks_df[
            greeks_df["pe_delta"].notna() &
            (greeks_df["pe_delta"] >= -DELTA_THRESHOLD) &
            (greeks_df["pe_delta"] < 0) &
            (greeks_df["strike"] % 100 == 0)
        ]
        if subset.empty:
            logger.warning(f"PE: No strike found with |delta| ≤ {DELTA_THRESHOLD}")
            return None
        best = subset.loc[subset["pe_delta"].idxmin()]  # closest to -0.30

    strike = int(best["strike"])
    delta  = best["ce_delta"] if side.upper() == "CE" else best["pe_delta"]
    logger.info(f"Delta strike | {side} | {strike} | delta={delta:.4f}")
    return strike


# ══════════════════════════════════════════════════════════════
# CSV READING — INTRADAY OI DATA
# ══════════════════════════════════════════════════════════════

def get_csv_filename(expiry_nse: str) -> str:
    """
    Convert NSE expiry "16-Jun-2026" to CSV filename "nifty_16JUN2026.csv".
    The CSV lives in CSV_DIR (the "pcr" folder).
    """
    dt   = datetime.datetime.strptime(expiry_nse, "%d-%b-%Y")
    name = f"{SYMBOL.lower()}_{dt.strftime('%d%b%Y').upper()}.csv"
    return os.path.join(CSV_DIR, name)


def get_intra_csv_filename(expiry_nse: str) -> str:
    """
    Path for the processed intraday CSV we write to.
    Example: strategies/intra_oi/intra_nifty_16JUN2026.csv
    """
    dt   = datetime.datetime.strptime(expiry_nse, "%d-%b-%Y")
    name = f"intra_{SYMBOL.lower()}_{dt.strftime('%d%b%Y').upper()}.csv"
    return os.path.join(SCRIPT_DIR, name)


def read_source_csv(expiry_nse: str) -> Optional[pd.DataFrame]:
    """
    Read the source OI/LTP CSV for a given expiry.
    Expected columns include: strikePrice, CE_OI, CE_LTP, PE_OI, PE_LTP
    (exact column names depend on the external data provider).

    Returns a DataFrame or None if the file cannot be read.
    """
    path = get_csv_filename(expiry_nse)
    if not os.path.isfile(path):
        logger.warning(f"Source CSV not found: {path}")
        return None
    try:
        df = pd.read_csv(path)
        # Normalise column names (strip spaces)
        df.columns = [c.strip() for c in df.columns]
        # Only keep rows where strikePrice is a multiple of 100
        if "strikePrice" in df.columns:
            df = df[df["strikePrice"] % 100 == 0].reset_index(drop=True)
        return df
    except Exception as e:
        logger.error(f"Error reading {path}: {e}")
        return None



# ══════════════════════════════════════════════════════════════
# INTRADAY DATAFRAME — COLUMN STRUCTURE
# ══════════════════════════════════════════════════════════════

INTRA_COLS = [
    "930_cOI",  "930_cLTP",                       # 9:30 AM CE base
    "Pre_cOI",  "Pre_cLTP",  "Pre_cvalue",         # Previous candle CE
    "cur_cOI",  "cur_cLTP",  "cur_cvalue",         # Current candle CE
    "strikePrice",                                  # Strike (multiple of 100)
    "930_pOI",  "930_pLTP",                       # 9:30 AM PE base
    "Pre_pOI",  "Pre_pLTP",  "Pre_pvalue",         # Previous candle PE
    "cur_pOI",  "cur_pLTP",  "cur_pvalue",         # Current candle PE
    "expiry",                                       # NSE expiry string
]


def build_empty_intra_df() -> pd.DataFrame:
    """Create an empty intraday DataFrame with the correct column structure."""
    return pd.DataFrame(columns=INTRA_COLS)


def save_intra_df(df: pd.DataFrame, expiry_nse: str):
    """Save the intraday DataFrame to the processed CSV file."""
    path = get_intra_csv_filename(expiry_nse)
    df.to_csv(path, index=False)


def load_intra_df(expiry_nse: str) -> pd.DataFrame:
    """Load the intraday DataFrame from disk (or return empty if not found)."""
    path = get_intra_csv_filename(expiry_nse)
    if not os.path.isfile(path):
        return build_empty_intra_df()
    try:
        return pd.read_csv(path)
    except Exception:
        return build_empty_intra_df()


# ══════════════════════════════════════════════════════════════
# NIFTY SPOT — from source CSV or NSE
# ══════════════════════════════════════════════════════════════

def get_nifty_spot_from_csv(expiry_nse: str) -> Optional[float]:
    """
    Try to read the Nifty spot price from the source CSV.
    Looks for a column named 'underlyingValue' or 'niftySpot'.
    Returns None if not available.
    """
    df = read_source_csv(expiry_nse)
    if df is None:
        return None
    for col in ["underlyingValue", "niftySpot", "underlying", "spot"]:
        if col in df.columns:
            val = df[col].dropna().iloc[0] if not df[col].dropna().empty else None
            if val is not None:
                try:
                    return float(val)
                except Exception:
                    pass
    return None


def get_live_nifty_spot(weekly_expiry: str) -> Optional[float]:
    """
    Get the current Nifty spot price.
    First tries the source CSV, then falls back to NSE option chain.
    """
    # Try CSV first
    spot = get_nifty_spot_from_csv(weekly_expiry)
    if spot:
        return spot

    # Fallback: NSE option chain
    try:
        json_data = fetch_option_chain_json(weekly_expiry)
        return float(json_data["records"]["underlyingValue"])
    except Exception as e:
        logger.error(f"Could not fetch Nifty spot: {e}")
        return None


# ══════════════════════════════════════════════════════════════
# CANDLE DATA MANAGEMENT
# ══════════════════════════════════════════════════════════════

def capture_930_base(intra_df: pd.DataFrame, source_df: pd.DataFrame, expiry_nse: str) -> pd.DataFrame:
    """
    Called at exactly 9:30 AM.
    For each strike in the source CSV:
      - Save CE OI → 930_cOI,  CE LTP → 930_cLTP
      - Save PE OI → 930_pOI,  PE LTP → 930_pLTP

    The 930_* values NEVER change during the day.
    """
    rows = []
    for _, row in source_df.iterrows():
        strike = int(row.get("strikePrice", 0))
        if strike % 100 != 0:
            continue
        rows.append({
            "strikePrice": strike,
            "expiry":      expiry_nse,
            "930_cOI":     row.get("CE_OI",  row.get("cOI",  None)),
            "930_cLTP":    row.get("CE_LTP", row.get("cLTP", None)),
            "930_pOI":     row.get("PE_OI",  row.get("pOI",  None)),
            "930_pLTP":    row.get("PE_LTP", row.get("pLTP", None)),
            # All other columns start empty
            "Pre_cOI": None, "Pre_cLTP": None, "Pre_cvalue": None,
            "cur_cOI": None, "cur_cLTP": None, "cur_cvalue": None,
            "Pre_pOI": None, "Pre_pLTP": None, "Pre_pvalue": None,
            "cur_pOI": None, "cur_pLTP": None, "cur_pvalue": None,
        })

    intra_df = pd.DataFrame(rows)[INTRA_COLS]
    logger.info(f"[9:30 BASE] Captured {len(intra_df)} strikes for {expiry_nse}")
    return intra_df


def update_current_candle(intra_df: pd.DataFrame, source_df: pd.DataFrame) -> pd.DataFrame:
    """
    Called at 9:35 AM and every 5 minutes after.
    Merges fresh OI/LTP from source_df into the cur_* columns of intra_df.
    Also recalculates cur_cvalue and cur_pvalue using evaluate_oi_ltp().
    """
    # Build a quick lookup: strike → (CE_OI, CE_LTP, PE_OI, PE_LTP)
    src_lookup: Dict[int, dict] = {}
    for _, row in source_df.iterrows():
        strike = int(row.get("strikePrice", 0))
        if strike % 100 != 0:
            continue
        src_lookup[strike] = {
            "cOI":  row.get("CE_OI",  row.get("cOI",  None)),
            "cLTP": row.get("CE_LTP", row.get("cLTP", None)),
            "pOI":  row.get("PE_OI",  row.get("pOI",  None)),
            "pLTP": row.get("PE_LTP", row.get("pLTP", None)),
        }

    def _apply_cur(row):
        strike = int(row["strikePrice"])
        src    = src_lookup.get(strike, {})
        row["cur_cOI"]  = src.get("cOI")
        row["cur_cLTP"] = src.get("cLTP")
        row["cur_pOI"]  = src.get("pOI")
        row["cur_pLTP"] = src.get("pLTP")
        row["cur_cvalue"] = evaluate_oi_ltp(
            row["930_cOI"], row["930_cLTP"],
            row["cur_cOI"], row["cur_cLTP"],
        )
        row["cur_pvalue"] = evaluate_oi_ltp(
            row["930_pOI"], row["930_pLTP"],
            row["cur_pOI"], row["cur_pLTP"],
        )
        return row

    intra_df = intra_df.apply(_apply_cur, axis=1)
    return intra_df


def shift_cur_to_pre(intra_df: pd.DataFrame) -> pd.DataFrame:
    """
    Called at the START of every candle from 9:40 AM onwards.
    Copies cur_* values into Pre_* — making "current" become "previous".
    """
    intra_df["Pre_cOI"]   = intra_df["cur_cOI"]
    intra_df["Pre_cLTP"]  = intra_df["cur_cLTP"]
    intra_df["Pre_cvalue"]= intra_df["cur_cvalue"]
    intra_df["Pre_pOI"]   = intra_df["cur_pOI"]
    intra_df["Pre_pLTP"]  = intra_df["cur_pLTP"]
    intra_df["Pre_pvalue"]= intra_df["cur_pvalue"]
    return intra_df


def get_strike_row(df: pd.DataFrame, strike: int, expiry: str) -> Optional[pd.Series]:
    """Return the DataFrame row matching strike + expiry, or None."""
    mask = (df["strikePrice"] == strike) & (df["expiry"] == expiry)
    rows = df[mask]
    if rows.empty:
        return None
    return rows.iloc[0]


# ══════════════════════════════════════════════════════════════
# TRADE STATE
# ══════════════════════════════════════════════════════════════

def make_initial_state() -> dict:
    """
    Returns a fresh state dict with all 4 trade slots cleared.
    Call this once at startup and also in daily_reset().
    """
    return {
        # ── BUY CE SLOT ──────────────────────────────────────
        "buy_ce_running":     False,
        "buy_ce_strike":      None,
        "buy_ce_expiry":      None,
        "buy_ce_entry_price": None,
        "buy_ce_entry_time":  None,

        # ── BUY PE SLOT ──────────────────────────────────────
        "buy_pe_running":     False,
        "buy_pe_strike":      None,
        "buy_pe_expiry":      None,
        "buy_pe_entry_price": None,
        "buy_pe_entry_time":  None,

        # ── SELL CE SLOT ─────────────────────────────────────
        "sell_ce_running":     False,
        "sell_ce_strike":      None,
        "sell_ce_expiry":      None,
        "sell_ce_entry_price": None,
        "sell_ce_entry_time":  None,

        # ── SELL PE SLOT ─────────────────────────────────────
        "sell_pe_running":     False,
        "sell_pe_strike":      None,
        "sell_pe_expiry":      None,
        "sell_pe_entry_price": None,
        "sell_pe_entry_time":  None,

        # ── DAY-LEVEL ────────────────────────────────────────
        "base_nifty_spot": None,
    }


def daily_reset(state: dict, weekly_intra_df: pd.DataFrame, monthly_intra_df: pd.DataFrame):
    """
    Call ONCE before 9:30 AM every trading day.
    Clears all trade state and all intraday DataFrame data.
    """
    state.update(make_initial_state())

    for df in [weekly_intra_df, monthly_intra_df]:
        for col in [c for c in INTRA_COLS if c not in ("strikePrice", "expiry")]:
            if col in df.columns:
                df[col] = None

    logger.info("✅ Daily reset complete. Ready for 9:30 AM base capture.")
    send_telegram(f"✅ [{SYMBOL}] Daily reset complete. Waiting for 9:30 AM base.")


# ══════════════════════════════════════════════════════════════
# PAPER TRADE LOGGING
# ══════════════════════════════════════════════════════════════

_TRADE_COLUMNS_B = [
    "trade_type", "strike_side", "strike", "expiry", "qty",
    "entry_datetime", "entry_price",
    "exit_datetime",  "exit_price",
    "pnl", "status", "entry_reason", "exit_reason",
    "account", "broker", "entry_order_id", "sl_order_id"
]

_TRADE_COLUMNS_S = _TRADE_COLUMNS_B.copy()


def _ensure_trade_csv(path: str, columns: List[str]):
    """Create the trade log CSV with headers if it doesn't exist yet."""
    if not os.path.isfile(path):
        pd.DataFrame(columns=columns).to_csv(path, index=False)


def log_paper_entry(
    trade_type:    str,    # "BUY" or "SELL"
    side:          str,    # "CE" or "PE"
    strike:        int,
    expiry:        str,
    entry_price:   float,
    qty:           int,
    entry_reason:  str,
):
    """Write a new OPEN trade row to the paper trading log."""
    path    = TRADES_CSV_B_PATH if trade_type == "BUY" else TRADES_CSV_S_PATH
    columns = _TRADE_COLUMNS_B if trade_type == "BUY" else _TRADE_COLUMNS_S
    _ensure_trade_csv(path, columns)

    row = {
        "trade_type":     trade_type,
        "strike_side":    f"{side}",
        "strike":         strike,
        "expiry":         expiry,
        "qty":            qty,
        "entry_datetime": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "entry_price":    entry_price,
        "exit_datetime":  "",
        "exit_price":     "",
        "pnl":            "",
        "status":         "OPEN",
        "entry_reason":   entry_reason,
        "exit_reason":    "",
        "account":        "Paper",
        "broker":         "Paper",
        "entry_order_id": "Paper",
        "sl_order_id":    "Paper",
    }
    pd.DataFrame([row]).to_csv(path, mode="a", header=False, index=False)
    logger.info(f"[PAPER LOG] {trade_type} {side} ENTRY | strike={strike} | price={entry_price}")


def log_paper_exit(
    trade_type:   str,
    side:         str,
    strike:       int,
    expiry:       str,
    exit_price:   float,
    entry_price:  float,
    qty:          int,
    exit_reason:  str,
):
    """
    Update the most recent OPEN row for this trade in the log with exit details.
    Calculates PnL as (exit_price - entry_price) × qty for BUY,
    or (entry_price - exit_price) × qty for SELL.
    """
    path    = TRADES_CSV_B_PATH if trade_type == "BUY" else TRADES_CSV_S_PATH
    columns = _TRADE_COLUMNS_B if trade_type == "BUY" else _TRADE_COLUMNS_S
    _ensure_trade_csv(path, columns)

    if trade_type == "BUY":
        pnl = (exit_price - entry_price) * qty
    else:
        pnl = (entry_price - exit_price) * qty

    try:
        df = pd.read_csv(path)
        # Find last OPEN row for this trade
        mask = (
            (df["trade_type"]  == trade_type) &
            (df["strike_side"] == side) &
            (df["strike"]      == strike) &
            (df["expiry"]      == expiry) &
            (df["status"]      == "OPEN")
        )
        idx = df[mask].index
        if not idx.empty:
            last = idx[-1]
            df.loc[last, "exit_datetime"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            df.loc[last, "exit_price"]    = exit_price
            df.loc[last, "pnl"]           = round(pnl, 2)
            df.loc[last, "status"]        = "CLOSED"
            df.loc[last, "exit_reason"]   = exit_reason
            df.to_csv(path, index=False)
            logger.info(f"[PAPER LOG] {trade_type} {side} EXIT | strike={strike} | pnl={pnl:.2f}")
    except Exception as e:
        logger.error(f"Error updating trade log: {e}")


def get_token(strike: int, side: str, use_next: bool = False) -> Optional[str]:
    """Lookup token from scrip master for live tick LTP"""
    exp = next_monthly if use_next else current_monthly
    if not exp:
        exp = current_weekly
    if not exp:
        return None

    strike_str = f"{strike * 100:.6f}"
    suffix = side.upper()

    for instr in scrip_master:
        if (instr.get("name") == SYMBOL and
            instr.get("instrumenttype") == "OPTIDX" and
            instr.get("expiry") == exp and
            instr.get("strike") == strike_str and
            str(instr.get("symbol", "")).endswith(suffix)):
            return instr.get("token")
    return None

# ══════════════════════════════════════════════════════════════
# TRADE ENTRY / EXIT PROCESSORS
# ══════════════════════════════════════════════════════════════


def get_paper_ltp(strike: int, side: str, expiry: str, intra_df: pd.DataFrame) -> Optional[float]:
    """
    Gets the paper LTP for the given strike, side, and expiry.
    First tries to get it from the intraday dataframe's current candle (cur_cLTP or cur_pLTP).
    If not found, falls back to the live tick token LTP.
    """
    row = get_strike_row(intra_df, strike, expiry)
    if row is not None:
        ltp_col = "cur_cLTP" if side.upper() == "CE" else "cur_pLTP"
        val = row.get(ltp_col)
        if val is not None and not pd.isna(val):
            return float(val)

    # Fallback to live_ltp via token
    token = get_token(strike, side)
    if token and token in live_ltp:
        return live_ltp[token]

    return None


def process_buy_entry(
    state:      dict,
    side:       str,
    entry_row:  pd.Series,
    weekly_intra_df: pd.DataFrame,
):
    """
    Try to enter a Buy trade.
    Does nothing if that slot is already running.

    entry_row = row from weekly_intra_df for the CURRENT OTM strike.
    """
    slot = f"buy_{side.lower()}_running"
    if state[slot]:
        return

    pre_val = entry_row[f"Pre_{side[0].lower()}value"]  # Pre_cvalue or Pre_pvalue
    cur_val = entry_row[f"cur_{side[0].lower()}value"]

    if not is_buy_entry(str(pre_val), str(cur_val)):
        return

    strike = int(entry_row["strikePrice"])
    expiry = str(entry_row["expiry"])
    ltp    = get_paper_ltp(strike, side, expiry, weekly_intra_df)
    ltp    = ltp if ltp else 0.0

    qty = get_qty(expiry, strike, side)

    state[slot]                         = True
    state[f"buy_{side.lower()}_strike"] = strike
    state[f"buy_{side.lower()}_expiry"] = expiry
    state[f"buy_{side.lower()}_entry_price"] = ltp
    state[f"buy_{side.lower()}_entry_time"]  = datetime.datetime.now()

    msg = f"✅ [PAPER] BUY {side} {strike} | expiry={expiry} | ltp={ltp:.2f}"
    logger.info(msg)
    send_telegram(msg)

    log_paper_entry("BUY", side, strike, expiry, ltp, qty,
                    f"Pre={pre_val} Cur={cur_val}")


def process_buy_exit(
    state:      dict,
    side:       str,
    exit_row:   pd.Series,
    weekly_intra_df: pd.DataFrame,
) -> bool:
    """
    Check exit condition for a running Buy trade.
    exit_row MUST be fetched using the STORED entry strike (not current OTM).

    Returns True if the trade was exited, False otherwise.
    """
    slot = f"buy_{side.lower()}_running"
    if not state[slot]:
        return False

    pre_val = exit_row[f"Pre_{side[0].lower()}value"]
    cur_val = exit_row[f"cur_{side[0].lower()}value"]

    if not is_buy_exit(str(pre_val), str(cur_val)):
        return False

    strike       = state[f"buy_{side.lower()}_strike"]
    expiry       = state[f"buy_{side.lower()}_expiry"]
    entry_price  = state[f"buy_{side.lower()}_entry_price"] or 0.0
    exit_ltp     = get_paper_ltp(strike, side, expiry, weekly_intra_df) or 0.0
    qty = get_qty(expiry, strike, side)

    msg = (f"🚪 [PAPER] EXIT BUY {side} {strike} | expiry={expiry} "
           f"| entry={entry_price:.2f} exit={exit_ltp:.2f} "
           f"| pnl={(exit_ltp - entry_price) * qty:.2f}")
    logger.info(msg)
    send_telegram(msg)

    log_paper_exit("BUY", side, strike, expiry, exit_ltp, entry_price, qty,
                   f"Pre={pre_val} Cur={cur_val}")

    state[slot]                              = False
    state[f"buy_{side.lower()}_strike"]      = None
    state[f"buy_{side.lower()}_expiry"]      = None
    state[f"buy_{side.lower()}_entry_price"] = None
    state[f"buy_{side.lower()}_entry_time"]  = None
    return True


def process_sell_entry(
    state:        dict,
    side:         str,
    weekly_row:   pd.Series,
    monthly_row:  pd.Series,
    monthly_intra_df: pd.DataFrame,
):
    """
    Try to enter a Sell trade.
    Requires: weekly trigger (SHORT+SHORT) AND monthly confirmation (BEARISH+BEARISH).
    Trade executes on the monthly strike (delta ≈ 0.30).
    """
    slot = f"sell_{side.lower()}_running"
    if state[slot]:
        return

    pre_w = weekly_row[f"Pre_{side[0].lower()}value"]
    cur_w = weekly_row[f"cur_{side[0].lower()}value"]
    pre_m = monthly_row[f"Pre_{side[0].lower()}value"]
    cur_m = monthly_row[f"cur_{side[0].lower()}value"]

    weekly_trigger  = is_sell_weekly_trigger(str(pre_w), str(cur_w))
    monthly_confirm = is_sell_monthly_confirm(str(pre_m), str(cur_m))

    if not (weekly_trigger and monthly_confirm):
        return

    strike = int(monthly_row["strikePrice"])
    expiry = str(monthly_row["expiry"])
    ltp    = get_paper_ltp(strike, side, expiry, monthly_intra_df)
    ltp    = ltp if ltp else 0.0
    qty = get_qty(expiry, strike, side)

    state[slot]                           = True
    state[f"sell_{side.lower()}_strike"]  = strike
    state[f"sell_{side.lower()}_expiry"]  = expiry
    state[f"sell_{side.lower()}_entry_price"] = ltp
    state[f"sell_{side.lower()}_entry_time"]  = datetime.datetime.now()

    msg = (f"✅ [PAPER] SELL {side} {strike} | expiry={expiry} | ltp={ltp:.2f} "
           f"| wkly={pre_w}+{cur_w} | mthly={pre_m}+{cur_m}")
    logger.info(msg)
    send_telegram(msg)

    log_paper_entry("SELL", side, strike, expiry, ltp, qty,
                    f"Weekly={pre_w}+{cur_w} Monthly={pre_m}+{cur_m}")


def process_sell_exit(
    state:       dict,
    side:        str,
    exit_row:    pd.Series,
    monthly_intra_df: pd.DataFrame,
) -> bool:
    """
    Check exit condition for a running Sell trade.
    exit_row MUST be fetched using the STORED monthly strike — NEVER recalculated delta.

    Returns True if exited, False otherwise.
    """
    slot = f"sell_{side.lower()}_running"
    if not state[slot]:
        return False

    pre_val = exit_row[f"Pre_{side[0].lower()}value"]
    cur_val = exit_row[f"cur_{side[0].lower()}value"]

    if not is_sell_exit(str(pre_val), str(cur_val)):
        return False

    strike      = state[f"sell_{side.lower()}_strike"]
    expiry      = state[f"sell_{side.lower()}_expiry"]
    entry_price = state[f"sell_{side.lower()}_entry_price"] or 0.0
    exit_ltp    = get_paper_ltp(strike, side, expiry, monthly_intra_df) or 0.0
    qty = get_qty(expiry, strike, side)
    pnl         = (entry_price - exit_ltp) * qty  # Sell: profit when LTP falls

    msg = (f"🚪 [PAPER] EXIT SELL {side} {strike} | expiry={expiry} "
           f"| entry={entry_price:.2f} exit={exit_ltp:.2f} | pnl={pnl:.2f}")
    logger.info(msg)
    send_telegram(msg)

    log_paper_exit("SELL", side, strike, expiry, exit_ltp, entry_price, qty,
                   f"Pre={pre_val} Cur={cur_val}")

    state[slot]                                = False
    state[f"sell_{side.lower()}_strike"]       = None
    state[f"sell_{side.lower()}_expiry"]       = None
    state[f"sell_{side.lower()}_entry_price"]  = None
    state[f"sell_{side.lower()}_entry_time"]   = None
    return True


# ══════════════════════════════════════════════════════════════
# MAIN CANDLE PROCESSING
# ══════════════════════════════════════════════════════════════

def process_candle(
    state:            dict,
    nifty_spot:       float,
    weekly_intra_df:  pd.DataFrame,
    monthly_intra_df: pd.DataFrame,
    weekly_expiry:    str,
    sell_expiry:      str,
    monthly_greeks_df: Optional[pd.DataFrame],
):
    """
    Called on every 5-minute candle close from 9:40 AM.
    Evaluates all 4 trade slots independently.

    KEY RULE:
      - EXIT always uses the STORED entry strike (not recalculated OTM)
      - ENTRY uses the current OTM strike from live Nifty spot
      - After any exit on a slot: immediately re-evaluate entry on same candle
    """
    ce_strike, pe_strike = get_otm_strikes(nifty_spot)
    logger.info(f"[CANDLE] spot={nifty_spot:.2f} | OTM CE={ce_strike} PE={pe_strike}")

    # Find delta ≈ 0.30 strike on monthly expiry (used for sell entry)
    delta_ce_strike = None
    delta_pe_strike = None
    if monthly_greeks_df is not None:
        delta_ce_strike = find_delta_strike(monthly_greeks_df, "CE")
        delta_pe_strike = find_delta_strike(monthly_greeks_df, "PE")

    # ── BUY CE ────────────────────────────────────────────────────
    if state["buy_ce_running"]:
        # Exit check: use STORED strike
        stored = state["buy_ce_strike"]
        stored_exp = state["buy_ce_expiry"]
        exit_row = get_strike_row(weekly_intra_df, stored, stored_exp)
        if exit_row is not None:
            exited = process_buy_exit(state, "CE", exit_row, weekly_intra_df)
            if exited:
                # Same-candle re-entry with fresh OTM
                spot2 = get_live_nifty_spot(weekly_expiry) or nifty_spot
                new_ce, _ = get_otm_strikes(spot2)
                entry_row = get_strike_row(weekly_intra_df, new_ce, weekly_expiry)
                if entry_row is not None:
                    process_buy_entry(state, "CE", entry_row, weekly_intra_df)
    else:
        entry_row = get_strike_row(weekly_intra_df, ce_strike, weekly_expiry)
        if entry_row is not None:
            process_buy_entry(state, "CE", entry_row, weekly_intra_df)

    # ── BUY PE ────────────────────────────────────────────────────
    if state["buy_pe_running"]:
        stored = state["buy_pe_strike"]
        stored_exp = state["buy_pe_expiry"]
        exit_row = get_strike_row(weekly_intra_df, stored, stored_exp)
        if exit_row is not None:
            exited = process_buy_exit(state, "PE", exit_row, weekly_intra_df)
            if exited:
                spot2 = get_live_nifty_spot(weekly_expiry) or nifty_spot
                _, new_pe = get_otm_strikes(spot2)
                entry_row = get_strike_row(weekly_intra_df, new_pe, weekly_expiry)
                if entry_row is not None:
                    process_buy_entry(state, "PE", entry_row, weekly_intra_df)
    else:
        entry_row = get_strike_row(weekly_intra_df, pe_strike, weekly_expiry)
        if entry_row is not None:
            process_buy_entry(state, "PE", entry_row, weekly_intra_df)

    # ── SELL CE ───────────────────────────────────────────────────
    if state["sell_ce_running"]:
        # Exit: use STORED monthly strike — NEVER recalculate delta here
        stored = state["sell_ce_strike"]
        stored_exp = state["sell_ce_expiry"]
        exit_row = get_strike_row(monthly_intra_df, stored, stored_exp)
        if exit_row is not None:
            exited = process_sell_exit(state, "CE", exit_row, monthly_intra_df)
            if exited and delta_ce_strike:
                spot2 = get_live_nifty_spot(weekly_expiry) or nifty_spot
                new_ce, _ = get_otm_strikes(spot2)
                weekly_row  = get_strike_row(weekly_intra_df,  new_ce,         weekly_expiry)
                monthly_row = get_strike_row(monthly_intra_df, delta_ce_strike, sell_expiry)
                if weekly_row is not None and monthly_row is not None:
                    process_sell_entry(state, "CE", weekly_row, monthly_row, monthly_intra_df)
    else:
        if delta_ce_strike:
            weekly_row  = get_strike_row(weekly_intra_df,  ce_strike,      weekly_expiry)
            monthly_row = get_strike_row(monthly_intra_df, delta_ce_strike, sell_expiry)
            if weekly_row is not None and monthly_row is not None:
                process_sell_entry(state, "CE", weekly_row, monthly_row, monthly_intra_df)

    # ── SELL PE ───────────────────────────────────────────────────
    if state["sell_pe_running"]:
        stored = state["sell_pe_strike"]
        stored_exp = state["sell_pe_expiry"]
        exit_row = get_strike_row(monthly_intra_df, stored, stored_exp)
        if exit_row is not None:
            exited = process_sell_exit(state, "PE", exit_row, monthly_intra_df)
            if exited and delta_pe_strike:
                spot2 = get_live_nifty_spot(weekly_expiry) or nifty_spot
                _, new_pe = get_otm_strikes(spot2)
                weekly_row  = get_strike_row(weekly_intra_df,  new_pe,         weekly_expiry)
                monthly_row = get_strike_row(monthly_intra_df, delta_pe_strike, sell_expiry)
                if weekly_row is not None and monthly_row is not None:
                    process_sell_entry(state, "PE", weekly_row, monthly_row, monthly_intra_df)
    else:
        if delta_pe_strike:
            weekly_row  = get_strike_row(weekly_intra_df,  pe_strike,      weekly_expiry)
            monthly_row = get_strike_row(monthly_intra_df, delta_pe_strike, sell_expiry)
            if weekly_row is not None and monthly_row is not None:
                process_sell_entry(state, "PE", weekly_row, monthly_row, monthly_intra_df)


# ══════════════════════════════════════════════════════════════
# FORCE EXIT & EXITS-ONLY MODE
# ══════════════════════════════════════════════════════════════

def force_exit_all(
    state:            dict,
    weekly_intra_df:  pd.DataFrame,
    monthly_intra_df: pd.DataFrame,
):
    """
    Called at 3:15 PM — exit ALL open positions at current LTP.
    No signal check needed.
    """
    for side in ["CE", "PE"]:
        slot = f"buy_{side.lower()}_running"
        if state[slot]:
            strike = state[f"buy_{side.lower()}_strike"]
            expiry = state[f"buy_{side.lower()}_expiry"]
            ep     = state[f"buy_{side.lower()}_entry_price"] or 0.0
            ltp    = get_paper_ltp(strike, side, expiry, weekly_intra_df) or 0.0
            qty = get_qty(expiry, strike, side)
            pnl    = (ltp - ep) * qty
            msg    = f"⏰ FORCE EXIT BUY {side} {strike} | pnl={pnl:.2f}"
            logger.info(msg)
            send_telegram(msg)
            log_paper_exit("BUY", side, strike, expiry, ltp, ep, qty, "FORCE EXIT 3:15PM")
            state[slot] = False
            state[f"buy_{side.lower()}_strike"]      = None
            state[f"buy_{side.lower()}_expiry"]      = None
            state[f"buy_{side.lower()}_entry_price"] = None
            state[f"buy_{side.lower()}_entry_time"]  = None

        slot = f"sell_{side.lower()}_running"
        if state[slot]:
            strike = state[f"sell_{side.lower()}_strike"]
            expiry = state[f"sell_{side.lower()}_expiry"]
            ep     = state[f"sell_{side.lower()}_entry_price"] or 0.0
            ltp    = get_paper_ltp(strike, side, expiry, monthly_intra_df) or 0.0
            qty = get_qty(expiry, strike, side)
            pnl    = (ep - ltp) * qty
            msg    = f"⏰ FORCE EXIT SELL {side} {strike} | pnl={pnl:.2f}"
            logger.info(msg)
            send_telegram(msg)
            log_paper_exit("SELL", side, strike, expiry, ltp, ep, qty, "FORCE EXIT 3:15PM")
            state[slot] = False
            state[f"sell_{side.lower()}_strike"]       = None
            state[f"sell_{side.lower()}_expiry"]       = None
            state[f"sell_{side.lower()}_entry_price"]  = None
            state[f"sell_{side.lower()}_entry_time"]   = None


def process_exits_only(
    state:            dict,
    weekly_intra_df:  pd.DataFrame,
    monthly_intra_df: pd.DataFrame,
):
    """
    Called between 3:05 PM and 3:15 PM.
    Check exits for any running trade. No new entries allowed.
    """
    for side in ["CE", "PE"]:
        if state[f"buy_{side.lower()}_running"]:
            strike = state[f"buy_{side.lower()}_strike"]
            expiry = state[f"buy_{side.lower()}_expiry"]
            exit_row = get_strike_row(weekly_intra_df, strike, expiry)
            if exit_row is not None:
                process_buy_exit(state, side, exit_row, weekly_intra_df)

        if state[f"sell_{side.lower()}_running"]:
            strike = state[f"sell_{side.lower()}_strike"]
            expiry = state[f"sell_{side.lower()}_expiry"]
            exit_row = get_strike_row(monthly_intra_df, strike, expiry)
            if exit_row is not None:
                process_sell_exit(state, side, exit_row, monthly_intra_df)

def start_pnl_reporter(state: dict, weekly_intra_df, monthly_intra_df):
    def report_pnl():
        while not shutdown_event.is_set():
            time.sleep(18 * 60)
            total_pnl = 0.0
            msg = "📊 [18-min PnL Report]\n"
            for slot, side, df, is_buy in [
                ("buy_ce", "CE", weekly_intra_df, True),
                ("buy_pe", "PE", weekly_intra_df, True),
                ("sell_ce", "CE", monthly_intra_df, False),
                ("sell_pe", "PE", monthly_intra_df, False),
            ]:
                if state.get(f"{slot}_running"):
                    strike = state.get(f"{slot}_strike")
                    expiry = state.get(f"{slot}_expiry")
                    entry = state.get(f"{slot}_entry_price", 0)
                    ltp = get_paper_ltp(strike, side, expiry, df) or 0
                    qty = get_qty(expiry, strike, side)
                    pnl = (ltp - entry) * qty if is_buy else (entry - ltp) * qty
                    total_pnl += pnl
                    msg += f"{slot.upper()}: {strike} | PnL: {pnl:.1f}\n"
            msg += f"**Total Unrealized PnL: {total_pnl:.1f}**"
            send_telegram(msg)
    threading.Thread(target=report_pnl, daemon=True).start()


# ══════════════════════════════════════════════════════════════
# MARKET TIME HELPERS
# ══════════════════════════════════════════════════════════════

NO_NEW_ENTRY_AFTER = datetime.time(15, 5)    # After 3:05 PM — exits only
FORCE_EXIT_AT      = datetime.time(15, 15)   # At 3:15 PM — force exit all
BASE_CAPTURE_TIME  = datetime.time(9, 30)    # 9:30 AM — capture base values
FIRST_DATA_TIME    = datetime.time(9, 35)    # 9:35 AM — first data candle
FIRST_TRADE_TIME   = datetime.time(9, 40)    # 9:40 AM — first trade evaluation


def is_market_open() -> bool:
    now = datetime.datetime.now()
    day = now.strftime("%a").upper()[:3]
    if day in WEEKLY_OFF:
        return False
    sh, sm = map(int, MARKET_START.split(":"))
    eh, em = map(int, MARKET_END.split(":"))
    t_start = now.replace(hour=sh, minute=sm, second=0, microsecond=0)
    t_end   = now.replace(hour=eh, minute=em, second=0, microsecond=0)
    return t_start <= now <= t_end


def seconds_until(hour: int, minute: int) -> float:
    """Return seconds until the next occurrence of HH:MM today (or tomorrow)."""
    now = datetime.datetime.now()
    t   = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if now >= t:
        t += datetime.timedelta(days=1)
    return (t - now).total_seconds()


def next_5min_candle_wait() -> float:
    """
    Wait until the next 5-minute candle boundary (9:35, 9:40, 9:45 … 15:25 …).
    Returns seconds to sleep.
    """
    now     = datetime.datetime.now()
    minute  = now.minute
    # Round UP to next multiple of 5
    next_m  = ((minute // 5) + 1) * 5
    delta_m = next_m - minute
    delta_s = delta_m * 60 - now.second - now.microsecond / 1e6
    return max(delta_s, 0)


# ══════════════════════════════════════════════════════════════
# ON-CANDLE-CLOSE WRAPPER
# ══════════════════════════════════════════════════════════════

def on_candle_close(
    state:             dict,
    weekly_intra_df:   pd.DataFrame,
    monthly_intra_df:  pd.DataFrame,
    weekly_expiry:     str,
    sell_expiry:       str,
    monthly_greeks_df: Optional[pd.DataFrame],
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Main entry point called every 5 minutes.
    Decides what to do based on current time:
      - ≥ 3:15 PM  → force exit all
      - > 3:05 PM  → exits only, no new entries
      - normal     → full candle processing
    Returns updated (weekly_intra_df, monthly_intra_df).
    """
    now = datetime.datetime.now().time()

    if now >= FORCE_EXIT_AT:
        force_exit_all(state, weekly_intra_df, monthly_intra_df)
        return weekly_intra_df, monthly_intra_df

    if now > NO_NEW_ENTRY_AFTER:
        process_exits_only(state, weekly_intra_df, monthly_intra_df)
        return weekly_intra_df, monthly_intra_df

    # Normal candle — shift cur → pre, load fresh data, then process
    weekly_intra_df  = shift_cur_to_pre(weekly_intra_df)
    monthly_intra_df = shift_cur_to_pre(monthly_intra_df)

    weekly_src  = read_source_csv(weekly_expiry)
    monthly_src = read_source_csv(sell_expiry)

    if weekly_src is not None:
        weekly_intra_df  = update_current_candle(weekly_intra_df, weekly_src)
    if monthly_src is not None:
        monthly_intra_df = update_current_candle(monthly_intra_df, monthly_src)

    # Save updated intraday CSVs
    save_intra_df(weekly_intra_df,  weekly_expiry)
    save_intra_df(monthly_intra_df, sell_expiry)

    nifty_spot = get_live_nifty_spot(weekly_expiry)
    if nifty_spot is None:
        logger.warning("Could not get Nifty spot — skipping candle processing.")
        return weekly_intra_df, monthly_intra_df

    process_candle(
        state, nifty_spot,
        weekly_intra_df, monthly_intra_df,
        weekly_expiry, sell_expiry,
        monthly_greeks_df,
    )
    return weekly_intra_df, monthly_intra_df


# ══════════════════════════════════════════════════════════════
# MAIN LOOP
# ══════════════════════════════════════════════════════════════

def main():
    logger.info(f"═══ OI INTRADAY STRATEGY — PAPER TRADING | {SYMBOL} ═══")
    send_telegram(f"🚀 [{SYMBOL}] Strategy starting (Paper Trading)")

    # Initialise NSE session for Greeks
    init_nse_session()

    state            = make_initial_state()
    weekly_intra_df  = build_empty_intra_df()
    monthly_intra_df = build_empty_intra_df()
    monthly_greeks_df: Optional[pd.DataFrame] = None
    start_pnl_reporter(state, weekly_intra_df, monthly_intra_df)

    base_captured   = False   # True after 9:30 AM base is saved
    first_candle    = False   # True after 9:35 AM data is loaded

    while not shutdown_event.is_set():
        now  = datetime.datetime.now()
        t    = now.time()
        today = now.date()

        # ── Before market opens ───────────────────────────────────
        if not is_market_open():
            logger.info("Market closed — waiting…")
            shutdown_event.wait(timeout=60)
            continue

        # ── Determine expiries for today ──────────────────────────
        weekly_expiry = get_current_weekly_expiry(today)
        sell_expiry   = get_sell_trade_expiry(today)

        if not weekly_expiry or not sell_expiry:
            logger.error("Could not determine expiry dates. Check ALL_EXPIRIES list.")
            shutdown_event.wait(timeout=60)
            continue

        # ── Daily reset (before 9:30 AM) ──────────────────────────
        if t < BASE_CAPTURE_TIME:
            daily_reset(state, weekly_intra_df, monthly_intra_df)
            base_captured  = False
            first_candle   = False
            wait = seconds_until(9, 30)
            logger.info(f"Waiting {wait:.0f}s for 9:30 AM base capture…")
            shutdown_event.wait(timeout=wait)
            continue

        # ── 9:30 AM — Capture base values ─────────────────────────
        if t >= BASE_CAPTURE_TIME and not base_captured:
            logger.info("=== 9:30 AM — Capturing base values ===")
            weekly_src  = read_source_csv(weekly_expiry)
            monthly_src = read_source_csv(sell_expiry)

            if weekly_src is not None:
                weekly_intra_df = capture_930_base(weekly_intra_df, weekly_src, weekly_expiry)
            if monthly_src is not None:
                monthly_intra_df = capture_930_base(monthly_intra_df, monthly_src, sell_expiry)

            nifty_spot = get_live_nifty_spot(weekly_expiry)
            if nifty_spot:
                state["base_nifty_spot"] = nifty_spot
                logger.info(f"[9:30 BASE] Nifty spot = {nifty_spot:.2f}")

            # Fetch Greeks for monthly expiry (used to find delta ≈ 0.30 strike)
            monthly_greeks_df = fetch_greeks_for_expiry(sell_expiry)

            save_intra_df(weekly_intra_df,  weekly_expiry)
            save_intra_df(monthly_intra_df, sell_expiry)

            base_captured = True
            send_telegram(
                f"📊 [{SYMBOL}] 9:30 AM base captured | "
                f"weekly={weekly_expiry} | sell_expiry={sell_expiry} | "
                f"spot={nifty_spot:.2f}"
            )
            # Wait for 9:35 AM
            wait = seconds_until(9, 35)
            logger.info(f"Base captured. Waiting {wait:.0f}s for 9:35 AM first candle…")
            shutdown_event.wait(timeout=wait)
            continue

        # ── 9:35 AM — First data candle (no trades yet) ───────────
        if t >= FIRST_DATA_TIME and not first_candle and base_captured:
            logger.info("=== 9:35 AM — Loading first data candle (no trades) ===")
            weekly_src  = read_source_csv(weekly_expiry)
            monthly_src = read_source_csv(sell_expiry)

            if weekly_src is not None:
                weekly_intra_df  = update_current_candle(weekly_intra_df, weekly_src)
            if monthly_src is not None:
                monthly_intra_df = update_current_candle(monthly_intra_df, monthly_src)

            save_intra_df(weekly_intra_df,  weekly_expiry)
            save_intra_df(monthly_intra_df, sell_expiry)

            first_candle = True
            # Wait for 9:40 AM (first trade candle)
            wait = seconds_until(9, 40)
            logger.info(f"First candle done. Waiting {wait:.0f}s for 9:40 AM first trade…")
            shutdown_event.wait(timeout=wait)
            continue

        # ── 9:40 AM onwards — Normal candle processing ────────────
        if t >= FIRST_TRADE_TIME and base_captured and first_candle:
            logger.info(f"=== {t.strftime('%H:%M')} — Candle close processing ===")
            weekly_intra_df, monthly_intra_df = on_candle_close(
                state,
                weekly_intra_df,
                monthly_intra_df,
                weekly_expiry,
                sell_expiry,
                monthly_greeks_df,
            )

            # Stop at 3:15 PM
            if t >= FORCE_EXIT_AT:
                logger.info("3:15 PM — Force exit done. Waiting for market close.")
                shutdown_event.wait(timeout=3600)
                continue

            # Sleep until next 5-minute candle
            wait = next_5min_candle_wait()
            logger.info(f"Sleeping {wait:.0f}s until next candle…")
            shutdown_event.wait(timeout=wait)
            continue

        # Fallback sleep if none of the above matched
        shutdown_event.wait(timeout=10)


# ══════════════════════════════════════════════════════════════
# ENTRY POINT
# ══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import signal

    def _handle_shutdown(sig, frame):
        logger.info("Shutdown signal received.")
        shutdown_event.set()

    signal.signal(signal.SIGINT,  _handle_shutdown)
    signal.signal(signal.SIGTERM, _handle_shutdown)

    main()