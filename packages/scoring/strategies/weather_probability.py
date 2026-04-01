"""
Weather / Meteorological Alpha Signal

Based on research in:
  - polymarket_weather_edge_research.pdf
  - Best_Individual_Long_Range_Weather_Forecasters_Worldwide_and_Where.pdf
  - Who_Is_Actually_Best_at_Long_Range_Weather_Forecasting_Globally.pdf
  - Obscure_but_High_Skill_Individual_Long_Range_Weather_Forecasters.pdf

Strategy:
  1. Detect if a market question relates to a weather event.
  2. Identify the city/region and the date from the question text.
  3. Fetch an objective forecast from Open-Meteo (free, no key needed).
  4. Compute the probability that the forecast meets the question condition.
  5. Return edge = forecast_prob − market_price.
"""

import logging
import re
from typing import Any, Dict, Optional, Tuple

import httpx
import numpy as np
from scipy.stats import norm

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------- #
# Extended city map — covers major Polymarket weather-market locations    #
# ---------------------------------------------------------------------- #
CITY_MAP: Dict[str, Dict[str, float]] = {
    # North America
    "New York":       {"lat": 40.7128, "lon": -74.0060},
    "NYC":            {"lat": 40.7128, "lon": -74.0060},
    "Los Angeles":    {"lat": 34.0522, "lon": -118.2437},
    "LA":             {"lat": 34.0522, "lon": -118.2437},
    "Chicago":        {"lat": 41.8781, "lon": -87.6298},
    "Houston":        {"lat": 29.7604, "lon": -95.3698},
    "Phoenix":        {"lat": 33.4484, "lon": -112.0740},
    "Philadelphia":   {"lat": 39.9526, "lon": -75.1652},
    "San Antonio":    {"lat": 29.4241, "lon": -98.4936},
    "San Diego":      {"lat": 32.7157, "lon": -117.1611},
    "Dallas":         {"lat": 32.7767, "lon": -96.7970},
    "Miami":          {"lat": 25.7617, "lon": -80.1918},
    "Atlanta":        {"lat": 33.7490, "lon": -84.3880},
    "Seattle":        {"lat": 47.6062, "lon": -122.3321},
    "Boston":         {"lat": 42.3601, "lon": -71.0589},
    "Denver":         {"lat": 39.7392, "lon": -104.9903},
    "Nashville":      {"lat": 36.1627, "lon": -86.7816},
    "Las Vegas":      {"lat": 36.1699, "lon": -115.1398},
    "Portland":       {"lat": 45.5231, "lon": -122.6765},
    "Washington":     {"lat": 38.9072, "lon": -77.0369},
    "Minneapolis":    {"lat": 44.9778, "lon": -93.2650},
    "Detroit":        {"lat": 42.3314, "lon": -83.0458},
    "New Orleans":    {"lat": 29.9511, "lon": -90.0715},
    "Kansas City":    {"lat": 39.0997, "lon": -94.5786},
    "Tampa":          {"lat": 27.9506, "lon": -82.4572},
    "Orlando":        {"lat": 28.5383, "lon": -81.3792},
    "Charlotte":      {"lat": 35.2271, "lon": -80.8431},
    "San Francisco":  {"lat": 37.7749, "lon": -122.4194},
    "SF":             {"lat": 37.7749, "lon": -122.4194},
    "Toronto":        {"lat": 43.6532, "lon": -79.3832},
    "Vancouver":      {"lat": 49.2827, "lon": -123.1207},
    "Mexico City":    {"lat": 19.4326, "lon": -99.1332},
    # Europe
    "London":         {"lat": 51.5074, "lon": -0.1278},
    "Paris":          {"lat": 48.8566, "lon":  2.3522},
    "Berlin":         {"lat": 52.5200, "lon": 13.4050},
    "Madrid":         {"lat": 40.4168, "lon": -3.7038},
    "Rome":           {"lat": 41.9028, "lon": 12.4964},
    "Amsterdam":      {"lat": 52.3676, "lon":  4.9041},
    "Vienna":         {"lat": 48.2082, "lon": 16.3738},
    "Zurich":         {"lat": 47.3769, "lon":  8.5417},
    "Stockholm":      {"lat": 59.3293, "lon": 18.0686},
    "Oslo":           {"lat": 59.9139, "lon": 10.7522},
    "Copenhagen":     {"lat": 55.6761, "lon": 12.5683},
    "Athens":         {"lat": 37.9838, "lon": 23.7275},
    "Warsaw":         {"lat": 52.2297, "lon": 21.0122},
    "Prague":         {"lat": 50.0755, "lon": 14.4378},
    "Brussels":       {"lat": 50.8503, "lon":  4.3517},
    "Lisbon":         {"lat": 38.7223, "lon": -9.1393},
    "Barcelona":      {"lat": 41.3851, "lon":  2.1734},
    "Munich":         {"lat": 48.1351, "lon": 11.5820},
    "Milan":          {"lat": 45.4642, "lon":  9.1900},
    "Istanbul":       {"lat": 41.0082, "lon": 28.9784},
    "Kyiv":           {"lat": 50.4501, "lon": 30.5234},
    "Moscow":         {"lat": 55.7558, "lon": 37.6173},
    # Asia / Pacific
    "Tokyo":          {"lat": 35.6895, "lon": 139.6917},
    "Beijing":        {"lat": 39.9042, "lon": 116.4074},
    "Shanghai":       {"lat": 31.2304, "lon": 121.4737},
    "Hong Kong":      {"lat": 22.3193, "lon": 114.1694},
    "Singapore":      {"lat":  1.3521, "lon": 103.8198},
    "Seoul":          {"lat": 37.5665, "lon": 126.9780},
    "Mumbai":         {"lat": 19.0760, "lon": 72.8777},
    "Delhi":          {"lat": 28.6139, "lon": 77.2090},
    "Bangkok":        {"lat": 13.7563, "lon": 100.5018},
    "Jakarta":        {"lat": -6.2088, "lon": 106.8456},
    "Sydney":         {"lat": -33.8688, "lon": 151.2093},
    "Melbourne":      {"lat": -37.8136, "lon": 144.9631},
    "Dubai":          {"lat": 25.2048, "lon": 55.2708},
    "Riyadh":         {"lat": 24.7136, "lon": 46.6753},
    # Africa / South America
    "Cairo":          {"lat": 30.0444, "lon": 31.2357},
    "Lagos":          {"lat":  6.5244, "lon":  3.3792},
    "Johannesburg":   {"lat": -26.2041, "lon": 28.0473},
    "Nairobi":        {"lat": -1.2921, "lon": 36.8219},
    "Sao Paulo":      {"lat": -23.5505, "lon": -46.6333},
    "Buenos Aires":   {"lat": -34.6037, "lon": -58.3816},
    "Rio de Janeiro": {"lat": -22.9068, "lon": -43.1729},
}

# Weather condition keywords for market type detection
_TEMP_KW   = {"high", "low", "temperature", "temp", "degrees", "heat", "cold", "hot",
               "warm", "cool", "freeze", "frozen", "record high", "record low"}
_PRECIP_KW = {"rain", "rainfall", "precipitation", "snow", "snowfall", "inches",
               "flood", "flooding", "wet", "dry", "drought"}
_WIND_KW   = {"wind", "hurricane", "typhoon", "cyclone", "mph", "knots", "gust"}
_STORM_KW  = {"storm", "tornado", "blizzard", "ice storm", "hail", "severe weather",
               "tropical storm", "depression", "derecho"}
_DISASTER_KW = {"warning", "watch", "advisory", "declare", "emergency", "state of emergency",
                "red flag", "evacuation", "disaster"}

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"


def _find_city(question: str) -> Optional[Tuple[str, Dict[str, float]]]:
    """Return (city_name, {lat, lon}) for the first known city found in the question."""
    q = question.lower()
    # Longer names first so "San Francisco" matches before "San"
    for city in sorted(CITY_MAP.keys(), key=len, reverse=True):
        if city.lower() in q:
            return city, CITY_MAP[city]
    return None


def _find_date(question: str) -> Optional[str]:
    """Extract a YYYY-MM-DD date from the question, if present."""
    from datetime import datetime as _dt, timedelta as _td

    # ISO format: 2024-07-20
    m = re.search(r"\d{4}-\d{2}-\d{2}", question)
    if m:
        return m.group(0)

    # "July 20, 2024" or "July 20 2024"
    m2 = re.search(
        r"(January|February|March|April|May|June|July|August|September|"
        r"October|November|December)\s+(\d{1,2}),?\s+(\d{4})",
        question, re.IGNORECASE,
    )
    if m2:
        try:
            parsed = _dt.strptime(re.sub(r"\s+", " ", m2.group(0)).replace(",", ""), "%B %d %Y")
            return parsed.strftime("%Y-%m-%d")
        except ValueError:
            pass

    # "March 31" or "on March 31" — no year; pick nearest future occurrence
    m3 = re.search(
        r"(January|February|March|April|May|June|July|August|September|"
        r"October|November|December)\s+(\d{1,2})(?!\s*,?\s*\d{4})",
        question, re.IGNORECASE,
    )
    if m3:
        month_name, day_str = m3.group(1), m3.group(2)
        today = _dt.utcnow()
        for year_offset in (0, 1):
            try:
                candidate = _dt.strptime(f"{month_name} {day_str} {today.year + year_offset}", "%B %d %Y")
                if candidate.date() >= (today - _td(days=1)).date():
                    return candidate.strftime("%Y-%m-%d")
            except ValueError:
                pass

    return None


def _detect_weather_type(question: str) -> str:
    q = question.lower()
    if any(kw in q for kw in _DISASTER_KW):
        return "disaster"
    if any(kw in q for kw in _STORM_KW):
        return "storm"
    if any(kw in q for kw in _WIND_KW):
        return "wind"
    if any(kw in q for kw in _PRECIP_KW):
        return "precipitation"
    return "temperature"



async def _fetch_forecast(
    lat: float, lon: float, target_date: str, unit: str, weather_type: str,
    past_days: int = 0,
) -> Dict[str, Any]:
    """Fetch hourly data from Open-Meteo (forecast or historical).

    When past_days > 0 the response includes that many days of historical data
    before today. Used when the event date already passed but the market is
    still open for trading — we use actual observed weather instead of a forecast.
    """
    hourly_vars = ["temperature_2m"]
    if weather_type in ("precipitation", "storm"):
        hourly_vars += ["precipitation", "rain", "snowfall"]
    if weather_type in ("wind", "storm"):
        hourly_vars += ["windspeed_10m", "windgusts_10m"]

    params: Dict[str, Any] = {
        "latitude":           lat,
        "longitude":          lon,
        "hourly":             ",".join(hourly_vars),
        "forecast_days":      1 if past_days > 0 else 16,
        "temperature_unit":   "fahrenheit" if unit == "F" else "celsius",
        "windspeed_unit":     "mph",
        "precipitation_unit": "inch" if unit == "F" else "mm",
        "timezone":           "auto",
    }
    if past_days > 0:
        params["past_days"] = past_days

    try:
        async with httpx.AsyncClient(timeout=12.0) as client:
            resp = await client.get(OPEN_METEO_URL, params=params)
            resp.raise_for_status()
            return resp.json()
    except Exception as e:
        logger.error(f"Open-Meteo fetch failed ({lat},{lon}): {e}")
        return {}


def _temperature_probability(
    forecast_data: Dict[str, Any], target_date: str, question: str, unit: str
) -> Optional[float]:
    hourly = forecast_data.get("hourly", {})
    times  = hourly.get("time", [])
    temps  = hourly.get("temperature_2m", [])
    day_temps = [t for ti, t in zip(times, temps)
                 if ti and ti.startswith(target_date) and t is not None]
    if not day_temps:
        return None

    daily_max = max(day_temps)
    daily_min = min(day_temps)
    std_dev   = 2.7 if unit == "F" else 1.5
    q_lower   = question.lower()

    # "between X and Y" or "between X-Y" (range format)
    between = re.search(r"between\s+(\d+(?:\.\d+)?)\s*(?:and|-)\s*(\d+(?:\.\d+)?)", q_lower)
    if between:
        lo, hi = int(between.group(1)), int(between.group(2))
        prob = float(norm.cdf(hi, loc=daily_max, scale=std_dev) -
                     norm.cdf(lo, loc=daily_max, scale=std_dev))
        return max(0.01, min(0.99, prob))

    thresh_m = re.search(r"(\d+(?:\.\d+)?)", question)
    if not thresh_m:
        return None
    try:
        threshold = float(thresh_m.group(1))
    except ValueError:
        return None

    if any(kw in q_lower for kw in ("exceed", "above", "over", "at least", "high above")):
        prob = float(1.0 - norm.cdf(threshold, loc=daily_max, scale=std_dev))
    elif any(kw in q_lower for kw in ("below", "under", "at most", "low below")):
        prob = float(norm.cdf(threshold, loc=daily_min, scale=std_dev))
    else:
        prob = float(1.0 - norm.cdf(threshold, loc=daily_max, scale=std_dev))

    return max(0.01, min(0.99, prob))


def _precipitation_probability(
    forecast_data: Dict[str, Any], target_date: str, question: str, unit: str
) -> Optional[float]:
    hourly = forecast_data.get("hourly", {})
    times  = hourly.get("time", [])
    precip = hourly.get("precipitation", [])
    snow   = hourly.get("snowfall", [])
    q_lower = question.lower()

    is_snow = "snow" in q_lower
    vals = [s for ti, s in zip(times, snow if is_snow else precip)
            if ti and ti.startswith(target_date) and s is not None]
    if not vals:
        return None

    total = sum(vals)
    thresh_m = re.search(r"(\d+(?:\.\d+)?)\s*(?:inch|mm|in\.?)", question, re.IGNORECASE)
    if thresh_m:
        threshold = float(thresh_m.group(1))
        std_dev   = max(0.05, total * 0.30)
        if any(kw in q_lower for kw in ("exceed", "above", "more than", "over", "at least")):
            prob = float(1.0 - norm.cdf(threshold, loc=total, scale=std_dev))
        else:
            prob = float(norm.cdf(threshold, loc=total, scale=std_dev))
        return max(0.01, min(0.99, prob))

def _disaster_probability(
    forecast_data: Dict[str, Any], target_date: str, question: str
) -> Optional[float]:
    """
    Compute probability for categorical 'Disaster Warning' markets.
    These markets (Hurricane Warning, Red Flag, etc.) are often binary and 
    triggered by forecasted intensity rather than a specific number in the question.
    """
    hourly = forecast_data.get("hourly", {})
    times  = hourly.get("time", [])
    gusts  = hourly.get("windgusts_10m", [])
    precip = hourly.get("precipitation", [])
    q_lower = question.lower()

    # Get max daily values
    day_gusts = [g for ti, g in zip(times, gusts) if ti.startswith(target_date) and g is not None]
    day_precip = [p for ti, p in zip(times, precip) if ti.startswith(target_date) and p is not None]

    max_gust = max(day_gusts) if day_gusts else 0
    total_precip = sum(day_precip) if day_precip else 0

    # Hurricane Warning: Gusts > 74 mph (Category 1)
    if "hurricane" in q_lower:
        threshold = 74.0
        std_dev = 10.0
        return max(0.01, min(0.99, float(1.0 - norm.cdf(threshold, loc=max_gust, scale=std_dev))))

    # Tropical Storm Warning: Gusts > 39 mph
    if "tropical storm" in q_lower:
        threshold = 39.0
        std_dev = 5.0
        return max(0.01, min(0.99, float(1.0 - norm.cdf(threshold, loc=max_gust, scale=std_dev))))

    # Red Flag Warning / Fire Weather: High Wind + Low Humidity
    # (Simplified: using wind gusts > 25 mph as a proxy for fire danger)
    if "red flag" in q_lower or "fire" in q_lower:
        threshold = 25.0
        std_dev = 5.0
        return max(0.01, min(0.99, float(1.0 - norm.cdf(threshold, loc=max_gust, scale=std_dev))))

    # Flood Warning: High precipitation
    if "flood" in q_lower:
        # > 3 inches is usually a serious flood risk
        threshold = 3.0 
        std_dev = 1.0
        return max(0.01, min(0.99, float(1.0 - norm.cdf(threshold, loc=total_precip, scale=std_dev))))

    # Default disaster probability for unspecified warnings: 
    # Use max wind/precip intensity combinations
    wind_prob = float(1.0 - norm.cdf(40, loc=max_gust, scale=10.0))
    precip_prob = float(1.0 - norm.cdf(2.0, loc=total_precip, scale=0.8))
    return max(wind_prob, precip_prob)


async def compute_weather_alpha(
    question: str,
    outcome_label: str,
    current_price: float,
) -> Optional[float]:
    """
    Compute an alpha edge for a weather prediction market.
    Returns edge = forecast_prob - current_price, or None if the market
    cannot be resolved against an objective forecast.
    """
    city_result = _find_city(question)
    if not city_result:
        return None

    city_name, coords = city_result
    target_date = _find_date(question)
    if not target_date:
        return None

    # ------------------------------------------------------------------ #
    # Check if the event date has already passed in the city's local     #
    # time. Use longitude to estimate UTC offset: offset ≈ lon / 15 h.  #
    # If past but within 7 days: fetch ACTUAL historical weather data —  #
    # the market may still be open pending resolution and the real        #
    # outcome is now known, giving maximum edge.                          #
    # If more than 7 days ago: skip (market would be resolved by now).   #
    # ------------------------------------------------------------------ #
    from datetime import datetime as _dt2, timedelta as _td2, timezone as _tz2, date as _date2
    utc_offset_h = round(coords["lon"] / 15.0)
    local_now    = _dt2.now(_tz2.utc) + _td2(hours=utc_offset_h)
    local_today  = local_now.strftime("%Y-%m-%d")

    past_days = 0
    if target_date < local_today:
        try:
            days_back = (_date2.fromisoformat(local_today) - _date2.fromisoformat(target_date)).days
        except ValueError:
            days_back = 99
        if days_back > 7:
            logger.debug(
                f"[Weather] {city_name} date {target_date} is {days_back}d ago — "
                f"market should be resolved. Skipping."
            )
            return None
        # Use historical data — actual observed weather, not a forecast
        past_days = days_back + 1
        logger.info(
            f"[Weather] {city_name} date {target_date} is {days_back}d ago — "
            f"fetching historical data (market still open pending resolution)."
        )

    unit         = "F" if ("F" in question or "fahrenheit" in question.lower()) else "C"
    weather_type = _detect_weather_type(question)

    forecast_data = await _fetch_forecast(
        coords["lat"], coords["lon"], target_date, unit=unit, weather_type=weather_type,
        past_days=past_days,
    )
    if not forecast_data:
        return None

    forecast_prob: Optional[float] = None

    if weather_type == "disaster":
        forecast_prob = _disaster_probability(forecast_data, target_date, question)
    elif weather_type in ("temperature",):
        forecast_prob = _temperature_probability(forecast_data, target_date, question, unit)
    elif weather_type == "precipitation":
        forecast_prob = _precipitation_probability(forecast_data, target_date, question, unit)
    elif weather_type in ("wind", "storm"):
        forecast_prob = _precipitation_probability(forecast_data, target_date, question, unit)
        hourly   = forecast_data.get("hourly", {})
        times    = hourly.get("time", [])
        gusts    = hourly.get("windgusts_10m", [])
        day_gusts = [g for ti, g in zip(times, gusts)
                     if ti and ti.startswith(target_date) and g is not None]
        if day_gusts:
            max_gust  = max(day_gusts)
            wind_m    = re.search(r"(\d+(?:\.\d+)?)\s*(?:mph|knots|kph)", question, re.IGNORECASE)
            if wind_m:
                threshold = float(wind_m.group(1))
                std_dev   = max(2.0, max_gust * 0.15)
                forecast_prob = max(0.01, min(0.99, float(
                    1.0 - norm.cdf(threshold, loc=max_gust, scale=std_dev)
                )))

    else:
        forecast_prob = _temperature_probability(forecast_data, target_date, question, unit)

    if forecast_prob is None:
        return 0.0, "Weather forecast unavailable for this location/date."

    edge = forecast_prob - current_price
    source = "actual" if past_days > 0 else "forecast"
    
    # Narrative generation for "data news" requirement
    if weather_type == "temperature":
        narrative = f"Open-Meteo {source} peak of {forecast_prob*100:.1f}th percentile vs market {current_price*100:.0f}%."
    elif weather_type == "disaster":
        narrative = f"Intensity check: {forecast_prob*100:.1f}% probability of warning-threshold breach detected."
    else:
        narrative = f"{weather_type.capitalize()} divergence: {forecast_prob*100:.1f}% probability vs {current_price*100:.0f}% market price."

    explanation = (
        f"[Weather {source.capitalize()}] {city_name} {target_date}: {narrative} "
        f"Edge={edge:+.3f}."
    )

    logger.info(f"[Weather] {city_name} {target_date} ({weather_type}): {explanation}")
    
    final_edge = edge if abs(edge) >= 0.08 else 0.0
    return final_edge, explanation

