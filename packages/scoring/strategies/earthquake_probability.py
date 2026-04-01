"""
Earthquake / Seismic Risk Signal

Based on research in:
  - Earthquake_Prediction_Claims_and_Precursor_Researchers_Evidence.pdf
  - Practical,_Reliable_Sources_for_Real_Seismic_Risk_Updates_and_Forecast.pdf
  - Separating_Operational_Seismology_From_Earthquake_Prediction_and.pdf
  - High-Skill_Earthquake_Forecasting_Experts_Who_Publish_Checkable.pdf

Strategy:
  1. Detect if a Polymarket question relates to earthquakes / seismic events.
  2. Query the USGS FDSN/Event API for recent significant earthquakes in the
     relevant region.
  3. Compare the USGS-implied probability with the current market price.
  4. Return an edge value (positive = YES is underpriced, negative = NO bias).

USGS API: https://earthquake.usgs.gov/fdsnws/event/1/query
  - Free, no key required
  - Returns earthquakes by location, magnitude, date
"""

import logging
import re
from datetime import datetime, timedelta
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------- #
# Region keyword → bounding box (minlat, maxlat, minlon, maxlon)         #
# Extended to cover all regions commonly referenced in Polymarket         #
# earthquake markets.                                                     #
# ---------------------------------------------------------------------- #
REGION_MAP = {
    # North America
    "California":       (32.5, 42.0,  -124.5, -114.0),
    "San Francisco":    (37.2, 38.2,  -122.8, -121.8),
    "Los Angeles":      (33.5, 34.5,  -118.9, -117.6),
    "Seattle":          (47.0, 48.5,  -122.5, -121.0),
    "Pacific Northwest":(42.0, 49.0,  -125.0, -116.0),
    "Alaska":           (54.0, 71.5,  -168.0, -130.0),
    "Cascadia":         (40.0, 52.0,  -130.0, -115.0),
    "New Madrid":       (34.0, 38.0,   -92.0,  -87.0),
    # South/Central America
    "Chile":            (-56.0, -17.0, -76.0, -65.0),
    "Mexico":           (14.5,  32.7, -118.0, -86.5),
    "Peru":             (-18.5,  -0.0, -82.0, -68.0),
    # Europe / Mediterranean
    "Turkey":           (35.8,  42.3,  25.0,  44.8),
    "Italy":            (36.5,  47.1,   6.6,  18.5),
    "Greece":           (34.8,  41.8,  19.3,  29.7),
    "Romania":          (43.7,  48.3,  20.2,  30.0),
    # Middle East / Asia
    "Iran":             (25.0,  39.8,  44.0,  63.3),
    "Nepal":            (26.4,  30.4,  80.0,  88.2),
    "India":            ( 8.0,  37.5,  68.0,  97.5),
    "Pakistan":         (23.5,  37.0,  60.5,  77.0),
    "Afghanistan":      (29.5,  38.5,  60.5,  75.0),
    "China":            (18.0,  53.5,  73.5, 135.0),
    # East Asia / Pacific
    "Japan":            (30.0,  45.5, 129.5, 146.0),
    "Taiwan":           (21.5,  25.5, 119.5, 122.5),
    "Philippines":      ( 4.5,  20.5, 116.5, 127.0),
    "Indonesia":        (-11.0,   6.0, 95.0, 141.0),
    "New Zealand":      (-47.5, -34.0, 166.0, 178.5),
    # Generic
    "Pacific Rim":      (-60.0,  60.0, 120.0, -60.0),   # wrap-around OK for USGS
    "Ring of Fire":     (-60.0,  60.0, 100.0, -60.0),
}

# Earthquake-related keywords for market detection
EARTHQUAKE_KEYWORDS = {
    "earthquake", "seismic", "quake", "tremor", "magnitude",
    "richter", "usgs", "epicenter", "aftershock", "tsunami",
    "fault", "tectonic", "seismicity", "shaking"
}

# If the question only contains "earthquake/quake" (not the stronger keywords)
# AND also contains sports-context words, it is a team-name false positive.
_SPORTS_CONTEXT = {"win", "mls", "cup", "championship", "playoff", "season",
                   "league", "soccer", "football", "basketball", "baseball",
                   "nfl", "nba", "mlb", "nhl", "title", "finals", "coach"}
_STRONG_EQ_KEYWORDS = EARTHQUAKE_KEYWORDS - {"earthquake", "quake"}


def is_earthquake_market(question: str) -> bool:
    """Return True if the market question is about an earthquake / seismic event."""
    q_lower = question.lower()
    if not any(kw in q_lower for kw in EARTHQUAKE_KEYWORDS):
        return False
    # If the only match is the generic word "earthquake"/"quake" and the question
    # is clearly in a sports context (e.g. San Jose Earthquakes MLS team), skip it.
    if not any(kw in q_lower for kw in _STRONG_EQ_KEYWORDS):
        if any(sw in q_lower for sw in _SPORTS_CONTEXT):
            return False
    return True


def _extract_region(question: str) -> Optional[str]:
    """Extract a known region name from the question text."""
    for region in REGION_MAP:
        if region.lower() in question.lower():
            return region
    return None


_MONTH_MAP = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10, "november": 11, "december": 12,
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "jun": 6, "jul": 7,
    "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}


def _extract_magnitude_threshold(question: str) -> Optional[float]:
    """
    Extract a magnitude threshold from the question.
    Handles: 'magnitude 6', 'M6.0', 'M 7.5', '≥6.5',
             '7.0 or above', 'above 6.5', 'at least 7'.
    """
    patterns = [
        r"magnitude\s*(\d+(?:\.\d+)?)",
        r"\bm\s*(\d+(?:\.\d+)?)\b",
        r"[≥>=]\s*(\d+(?:\.\d+)?)",
        # natural language: "7.0 or above/higher/greater", "at least 7", "above 6.5"
        r"(\d+(?:\.\d+)?)\s*(?:or\s+above|or\s+higher|or\s+greater|and\s+above|and\s+higher)",
        r"(?:above|over|exceed(?:ing)?|at\s+least)\s+(\d+(?:\.\d+)?)",
    ]
    for pat in patterns:
        m = re.search(pat, question, re.IGNORECASE)
        if m:
            try:
                return float(m.group(1))
            except ValueError:
                continue
    return None


def _extract_days_window(question: str) -> int:
    """
    Extract the forecast window in days.
    Handles: 'within 7 days', 'next 30 days', 'X weeks', 'X months',
             'by April 30, 2026', 'by March 31'.
    Falls back to 30 days.
    """
    # Explicit day/week/month counts
    m = re.search(r"(\d+)\s*days?", question, re.IGNORECASE)
    if m:
        return int(m.group(1))
    m = re.search(r"(\d+)\s*weeks?", question, re.IGNORECASE)
    if m:
        return int(m.group(1)) * 7
    m = re.search(r"(\d+)\s*months?", question, re.IGNORECASE)
    if m:
        return int(m.group(1)) * 30

    # "by <Month> <Day>[, <Year>]"
    date_pat = re.search(
        r"by\s+([A-Za-z]+)\s+(\d{1,2})(?:,?\s+(\d{4}))?",
        question, re.IGNORECASE
    )
    if date_pat:
        month_str = date_pat.group(1).lower()
        day       = int(date_pat.group(2))
        year_str  = date_pat.group(3)
        month     = _MONTH_MAP.get(month_str)
        if month:
            today = datetime.utcnow()
            year  = int(year_str) if year_str else today.year
            try:
                target = datetime(year, month, day)
                if target < today:
                    target = datetime(year + 1, month, day)
                delta = (target - today).days
                return max(1, delta)
            except ValueError:
                pass

    return 30  # default


async def compute_earthquake_alpha(
    question: str,
    outcome_label: str,
    current_price: float,
) -> Optional[float]:
    """
    Compute an alpha edge for a seismic / earthquake prediction market.

    Methodology (based on USGS operational seismology research):
      1. Determine the target region and magnitude threshold from the question.
      2. Query USGS for the number of qualifying earthquakes in the past window
         equal to the question's time horizon.
      3. Estimate a base rate probability using a simple Poisson model:
             P(at_least_one) = 1 − exp(−λ * t)
         where λ = historical rate per day, t = days in the question window.
      4. Compute edge = forecast_prob − current_price.

    Returns None if the market cannot be mapped to a region/magnitude, or if
    the USGS request fails.
    """
    if not is_earthquake_market(question):
        return None

    region = _extract_region(question)
    if not region:
        # No named region → treat as global (worldwide seismicity)
        # Use a broad bounding box and bump up the minimum magnitude
        region = "_global"
        REGION_MAP["_global"] = (-90.0, 90.0, -180.0, 180.0)

    min_mag = _extract_magnitude_threshold(question)
    if min_mag is None:
        min_mag = 5.0  # default: significant earthquakes

    days_window = _extract_days_window(question)

    # ------------------------------------------------------------------ #
    # Query USGS for historical rate (past 365 days as base rate)         #
    # ------------------------------------------------------------------ #
    bbox = REGION_MAP[region]
    end_dt   = datetime.utcnow()
    start_dt = end_dt - timedelta(days=365)

    usgs_url = "https://earthquake.usgs.gov/fdsnws/event/1/query"
    params = {
        "format":    "geojson",
        "starttime": start_dt.strftime("%Y-%m-%d"),
        "endtime":   end_dt.strftime("%Y-%m-%d"),
        "minlatitude":  bbox[0],
        "maxlatitude":  bbox[1],
        "minlongitude": bbox[2],
        "maxlongitude": bbox[3],
        "minmagnitude": min_mag,
        "orderby":   "time",
        "limit":     1000,
    }

    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.get(usgs_url, params=params)
            resp.raise_for_status()
            data = resp.json()
    except Exception as e:
        logger.warning(f"USGS API request failed for {region}: {e}")
        return None

    count_in_year = len(data.get("features", []))
    if count_in_year == 0:
        # No historical events → very low probability, market likely fair
        return None

    # Poisson model: λ = annual count / 365 days
    import math
    lam_per_day = count_in_year / 365.0
    
    # Safety: if global, cap lambda to prevent 100% outcomes for rare individual events
    # research Section 3.2: global base rate is too high for a single magnitude event
    if region == "_global":
        lam_per_day = min(lam_per_day, 0.05) 
        
    forecast_prob = 1.0 - math.exp(-lam_per_day * days_window)
    forecast_prob = max(0.01, min(0.95, forecast_prob))

    # Also check for very recent high-magnitude events (last 7 days) that
    # may elevate probability beyond the base rate (aftershock sequences).
    recent_start = end_dt - timedelta(days=7)
    recent_params = {
        "format": "geojson",
        "starttime": recent_start.strftime("%Y-%m-%d"),
        "endtime": end_dt.strftime("%Y-%m-%d"),
        "minlatitude": bbox[0],
        "maxlatitude": bbox[1],
        "minlongitude": bbox[2],
        "maxlongitude": bbox[3],
        "minmagnitude": max(min_mag, 5.5),
    }

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            recent_resp = await client.get(usgs_url, params=recent_params)
            recent_resp.raise_for_status()
            recent_data = recent_resp.json()
            recent_count = len(recent_data.get("features", []))
            if recent_count > 0:
                # Aftershock sequences increase near-term probability
                # Apply a modest boost capped at +20 pp
                boost = min(0.20, recent_count * 0.04)
                forecast_prob = min(0.99, forecast_prob + boost)
                logger.info(
                    f"[EQ] {region}: {recent_count} recent M{min_mag}+ events — "
                    f"boosting forecast by {boost:.2f}"
                )
    except Exception:
        pass  # recent-event check is best-effort

    edge = forecast_prob - current_price
    
    # Narrative generation for "data news" requirement
    region_label = "Global" if region == "_global" else region
    narrative = (
        f"USGS Basin Model: {count_in_year} historical M{min_mag}+ events/year in {region_label}. "
        f"Poisson-derived probability is {forecast_prob*100:.1f}% vs {current_price*100:.1f}% market price."
    )
    
    explanation = (
        f"[Seismic Audit] {region_label} M{min_mag}+: {narrative} "
        f"Edge={edge:+.3f}."
    )

    logger.info(f"[EQ] {region_label}: {explanation}")
    
    final_edge = edge if abs(edge) >= 0.05 else 0.0
    return final_edge, explanation

