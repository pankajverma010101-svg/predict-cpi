from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
import pandas as pd
import joblib
import re, os, bisect
from math import ceil
from collections import defaultdict
import re
from .countries import countries

# ---------------- Paths ----------------
CONSUMER_MODEL = "ml/consumer_pricing_model.pkl"
CONSUMER_FEATURES = "ml/consumer_pricing_features.pkl"
CONSUMER_LOOKUP = "ml/consumer_pricing_lookup.pkl"
CONSUMER_BUCKETS = "ml/consumer_pricing_buckets.pkl"
B2B_LOOKUP = "ml/qlab_b2b_pricing_lookup.pkl"
B2B_ACQUITY_LOOKUP = "ml/b2b_cpi_pricing_acquity_lookup.pkl"
B2C_ACQUITY_LOOKUP = "ml/b2c_cpi_pricing_acquity_lookup.pkl"


# ---------------- USA synonyms (normalized to uppercase, dots removed) ----------------
COUNTRY_SYNONYMS = {
    "USA": {
        "USA", "US", "U S", "UNITED STATES", "UNITED STATES OF AMERICA",
        "UNITED STATES (USA)", "UNITED STATES AMERICA", "U S A",
        "U.S.", "U.S.A", "AMERICA", "THE UNITED STATES",
        "UNITED-STATES", "UNITED STATE", "US OF A", "STATES"
    },
    "UK": {
        "UK", "U K", "UNITED KINGDOM", "BRITAIN", "GREAT BRITAIN",
        "ENGLAND", "SCOTLAND", "WALES", "NORTHERN IRELAND",
        "GB", "GBR"
    },
    "GERMANY": {
        "GERMANY", "DE", "DEU", "DEUTSCHLAND",
        "FEDERAL REPUBLIC OF GERMANY", "GER"
    },
    "FRANCE": {
        "FRANCE", "FRENCH REPUBLIC", "FR", "FRA"
    },
    "BRAZIL": {
        "BRAZIL", "BR", "BRASIL", "FEDERATIVE REPUBLIC OF BRAZIL", "BRA"
    },
    "JAPAN": {
        "JAPAN", "JP", "NIPPON", "NIHON", "JPN"
    },
    "AUSTRALIA": {
        "AUSTRALIA", "AU", "COMMONWEALTH OF AUSTRALIA", "AUS"
    },
    "CANADA": {
        "CANADA", "CA", "CAN"
    },
    "INDIA": {
        "INDIA", "IN", "BHARAT", "IND"
    },
    "CHINA": {
        "CHINA", "CN", "CHN", "PEOPLE'S REPUBLIC OF CHINA", "PRC"
    },
    "ITALY": {
        "ITALY", "IT", "ITA", "ITALIA"
    },
    "SPAIN": {
        "SPAIN", "ES", "ESP", "ESPAÑA"
    },
     "LATAM": {
        "LATAM", "LATAM America"
    },
    "MENA":{
        "UAE",
    "United Arab Emirates",
    "Emirates",
    "AE",  # ISO country code
    "Dubai",
    "Abu Dhabi",
    "Sharjah",
    "Ajman",
    "Umm Al Quwain",
    "Ras Al Khaimah",
    "Fujairah",
    "Northern Emirates",
    }

}

def _normalize_country_or_market(name: str) -> str:
    """
    Normalize country/market to canonical form.
    - Trim, uppercase, collapse spaces, remove dots
    - Map any synonym to its canonical name
    """
    s = (name or "").upper()
    s = s.replace(".", " ").strip()
    s = re.sub(r"\s+", " ", s)  # collapse multiple spaces
    s = s.replace("-", " ").strip()

    # lookup in COUNTRY_SYNONYMS
    for canonical, synonyms in COUNTRY_SYNONYMS.items():
        if s in synonyms:
            return canonical

    return s  # fallback (unchanged if no match)

# ---------------- Load consumer artifacts ----------------
if os.path.exists(CONSUMER_MODEL):
    consumer_model = joblib.load(CONSUMER_MODEL)
    consumer_features = joblib.load(CONSUMER_FEATURES)
    consumer_lookup = joblib.load(CONSUMER_LOOKUP)
    consumer_buckets = joblib.load(CONSUMER_BUCKETS)
    ir_buckets = consumer_buckets.get('ir_buckets', [])
    loi_buckets = consumer_buckets.get('loi_buckets', [])
else:
    consumer_model, consumer_features, consumer_lookup = None, [], {}
    ir_buckets, loi_buckets = [], []

# ---------------- Load B2B lookup ----------------
raw_b2b = joblib.load(B2B_LOOKUP) if os.path.exists(B2B_LOOKUP) else []

raw_b2b_acquity = joblib.load(B2B_ACQUITY_LOOKUP) if os.path.exists(B2B_ACQUITY_LOOKUP) else []
raw_b2c_acquity = joblib.load(B2C_ACQUITY_LOOKUP) if os.path.exists(B2C_ACQUITY_LOOKUP) else []

by_name_b2b_acquity = defaultdict(list)
for r in raw_b2b_acquity:
    cn_norm = _normalize_country_or_market(r.get('country_name'))
    by_name_b2b_acquity[cn_norm].append(r)

by_name_b2c_acquity = defaultdict(list)
for r in raw_b2c_acquity:
    cn_norm = _normalize_country_or_market(r.get('country_name'))
    by_name_b2c_acquity[cn_norm].append(r)


# Build by_name dict with normalized keys (so 'US', 'United States', etc. all map to 'USA')
by_name = defaultdict(list)
for r in raw_b2b:
    cn_raw = (r.get('country_name') or "")
    cn_norm = _normalize_country_or_market(cn_raw)
    by_name[cn_norm].append(r)

# ---------------- Helpers ----------------
def _parse_range(val):
    """
    Parse a value that may be a single number or a range:
      - Handles 'ir- 5-9%', '5–9%', '5—9%', '5 to 9', '5 - 9', '8.5-12.2'
      - Returns (min_int, max_int) with ceil for decimals
    """
    if val is None:
        return None
    s = str(val).lower().strip()

    # Normalize unicode dashes to '-'
    s = s.replace("–", "-").replace("—", "-")
    # Remove percent signs and common labels like 'ir', 'loi', 'minutes', 'mins', etc. (only for parsing)
    s_clean = re.sub(r"(percent|%|ir|loi|minutes|minute|mins|min)", "", s)
    s_clean = s_clean.replace("to", "-")  # convert "5 to 9" to "5-9"
    # Keep only digits, dot, dash and spaces
    s_clean = re.sub(r"[^0-9\.\-\s]", " ", s_clean)
    s_clean = re.sub(r"\s+", " ", s_clean).strip()

    nums = re.findall(r"\d+\.?\d*", s_clean)
    if "-" in s_clean and len(nums) >= 2:
        a = float(nums[0]); b = float(nums[1])
        lo = int(ceil(min(a, b)))
        hi = int(ceil(max(a, b)))
        return (lo, hi)
    if nums:
        v = float(nums[0])
        iv = int(ceil(v))
        return (iv, iv)
    return None

def parse_loi(val):
    return _parse_range(val)

def parse_ir(val):
    return _parse_range(val)

def rows_for_country(country_upper: str):
    cu = _normalize_country_or_market(country_upper)
    if cu in by_name:  # exact (normalized) match
        return by_name[cu], 'by_name'
    if "INTERNATIONAL" in by_name:  # fallback
        return by_name["INTERNATIONAL"], 'international'
    return [], 'none'

def cover_match(rows, loi_range, ir_range):
    Lmin, Lmax = loi_range
    Imin, Imax = ir_range
    for r in rows:
        if r['loi_min'] <= Lmin and Lmax <= r['loi_max'] and \
           r['incidence_min'] <= Imin and Imax <= r['incidence_max']:
            return r
    return None

def nearest_match(rows, loi_range, ir_range):
    Lmid = (loi_range[0] + loi_range[1]) / 2
    Imid = (ir_range[0] + ir_range[1]) / 2
    best, best_d = None, None
    for r in rows:
        d_loi = abs(Lmid - (r['loi_min'] + r['loi_max'])/2)
        d_ir  = abs(Imid - (r['incidence_min'] + r['incidence_max'])/2)
        d = d_loi + d_ir
        if best is None or d < best_d:
            best, best_d = r, d
    return best

def b2b_find_price(country_upper, ir_input, loi_input):
    rows, matched_type = rows_for_country(country_upper)
    if not rows:
        return None, "no_rows", {"matched_type": matched_type}

    parsed_loi, parsed_ir = parse_loi(loi_input), parse_ir(ir_input)
    if not parsed_loi or not parsed_ir:
        return None, "invalid_input", {"matched_type": matched_type}

    r = cover_match(rows, parsed_loi, parsed_ir)
    if r:
        return r['price'], "cover", {"row": r, "matched_type": matched_type}

    r = nearest_match(rows, parsed_loi, parsed_ir)
    if r:
        return r['price'], "nearest", {"row": r, "matched_type": matched_type}

    return None, "no_match", {"matched_type": matched_type}

def to_num_and_ceil(val):
    """
    Legacy single-value parser. Kept for compatibility but not used for IR/LOI anymore.
    """
    try:
        val = float(re.sub(r'[^0-9.]', '', str(val)))
        return int(ceil(val))
    except:
        return None

def map_to_next_bucket(value, bucket_list):
    if value is None or not bucket_list:
        return value
    idx = bisect.bisect_left(bucket_list, value)
    return bucket_list[min(idx, len(bucket_list)-1)]

def nearest_lookup_price_for_market(market, ir, loi):
    candidates = []
    for (mkt, ir_k, loi_k), price in consumer_lookup.items():
        if mkt != market:
            continue
        dist = abs(ir_k - ir) + abs(loi_k - loi)
        candidates.append((dist, price, ir_k, loi_k))
    if not candidates:
        return None, None, None
    candidates.sort(key=lambda x: x[0])
    _, price, matched_ir, matched_loi = candidates[0]
    return float(price), matched_ir, matched_loi


def load_acuity_b2b_rows():
    try:
        return joblib.load("ml/acuity_b2b_pricing_lookup.pkl")
    except FileNotFoundError:
        return []


def acuity_b2b_find_price(country_upper, ir_input, loi_input):
    rows = load_acuity_b2b_rows()
    rows = [r for r in rows if r['country_name'] == country_upper.upper()]
    matched_type = "acuity_b2b"

    if not rows:
        return None, "no_rows", {"matched_type": matched_type}

    parsed_loi, parsed_ir = parse_loi(loi_input), parse_ir(ir_input)
    if parsed_loi is None or parsed_ir is None:
        return None, "invalid_input", {"matched_type": matched_type}

    r = cover_match(rows, parsed_loi, parsed_ir)
    if r:
        return r['price'], "cover", {"row": r, "matched_type": matched_type}

    r = nearest_match(rows, parsed_loi, parsed_ir)
    if r:
        return r['price'], "nearest", {"row": r, "matched_type": matched_type}

    return None, "no_match", {"matched_type": matched_type}


def load_acuity_b2c_rows():
    try:
        return joblib.load("ml/acuity_b2c_pricing_lookup.pkl")
    except FileNotFoundError:
        return []

def acuity_b2c_find_price(country_upper, ir_input, loi_input):
    rows = load_acuity_b2c_rows()
    rows = [r for r in rows if r['country_name'] == country_upper.upper()]
    matched_type = "acuity_b2b"

    if not rows:
        return None, "no_rows", {"matched_type": matched_type}

    parsed_loi, parsed_ir = parse_loi(loi_input), parse_ir(ir_input)
    if parsed_loi is None or parsed_ir is None:
        return None, "invalid_input", {"matched_type": matched_type}

    r = cover_match(rows, parsed_loi, parsed_ir)
    if r:
        return r['price'], "cover", {"row": r, "matched_type": matched_type}

    r = nearest_match(rows, parsed_loi, parsed_ir)
    if r:
        return r['price'], "nearest", {"row": r, "matched_type": matched_type}

    return None, "no_match", {"matched_type": matched_type}


def b2b_with_client_find_price(client_name, dir="no", clevel="no"):
    # load lookup
    records = joblib.load("ml/b2b_with_client_pricing_lookup.pkl")

    client_name = str(client_name).lower().strip()
    matched = next((r for r in records if r["client_name"] == client_name), None)

    if not matched:
        return None, {"message": f"No pricing found for client {client_name}"}

    # base CPI → take min_cpi
    price = matched["min_cpi"]

    # add premiums if applicable
    if dir == "yes":
        price += matched["dir_premium"]
    if clevel == "yes":
        price += matched["clevel_premium"]

    return price, {
        "client_name": client_name,
        "base_cpi": matched["min_cpi"],
        "dir": dir,
        "clevel": clevel,
        "dir_premium": matched["dir_premium"],
        "clevel_premium": matched["clevel_premium"]
    }

# def b2b_with_client_find_price(client_name, dir="no", clevel="no"):
#     # load lookup
#     records = joblib.load("ml/b2b_with_client_pricing_lookup.pkl")

#     client_name = str(client_name).lower().strip()
#     matched = next((r for r in records if r["client_name"] == client_name), None)

#     if not matched:
#         return None, {"message": f"No pricing found for client {client_name}"}

#     # base CPI → take min_cpi
#     price = matched["min_cpi"]

#     # add premiums if applicable
#     if dir == "yes":
#         price += matched["dir_premium"]
#     if clevel == "yes":
#         price += matched["clevel_premium"]

#     # ✅ cap at max_cpi
#     if "max_cpi" in matched and matched["max_cpi"] is not None:
#         price = min(price, matched["max_cpi"])

#     return price, {
#         "client_name": client_name,
#         "base_cpi": matched["min_cpi"],
#         "dir": dir,
#         "clevel": clevel,
#         "dir_premium": matched["dir_premium"],
#         "clevel_premium": matched["clevel_premium"],
#         "max_cpi": matched.get("max_cpi")
#     }


def find_region(text: str) -> str:
    """
    Detects the region based on country code, country name, or region keyword.
    Special rule: LATAM -> LATAM America
    """
    text_upper = text.upper()

    # Direct region keywords
    for keyword in ["MENA", "APAC", "EU", "USA", "CANADA", "UK", "LATAM"]:
        if re.search(r'\b' + re.escape(keyword) + r'\b', text_upper):
            return "LATAM America" if keyword == "LATAM" else keyword

    # Search by country code or country name
    for code, data in countries.items():
        country_name_upper = data["name"].upper()
        if re.search(r'\b' + re.escape(code) + r'\b', text_upper) or re.search(r'\b' + re.escape(country_name_upper) + r'\b', text_upper):
            region = data["region"]
            return "LATAM America" if region == "LATAM" else region

    return "Unknown"



# =========================
# DRF View
# =========================
class PredictCPI(APIView):
    def post(self, request):
        try:
            business_type = str(request.data.get("business_type", "")).lower().strip()
            client_name=request.data.get("client_name")

            # -------- B2B ACUITY CASE --------
            if business_type == "b2b" and client_name == "acuity":
                country_name = _normalize_country_or_market(str(request.data.get("market", "")))
                country=find_region(country_name)

                ir_in, loi_in = request.data.get("ir"), request.data.get("loi")
                if not country or ir_in is None or loi_in is None:
                    return Response({"status": "error", "message": "country, ir, loi required"}, status=400)

                price, source, meta = acuity_b2b_find_price(country, ir_in, loi_in)

                if price is not None:
                    return Response({
                        "status": "success",
                        "predicted_price": round(float(price), 2),
                        "source": f"b2b_acquity_{source}",
                        "matched_type": meta.get("matched_type"),
                        "matched_row": meta.get("row")
                    })
                return Response({"status": "error", "message": "No matching B2B Acuity rule found", "source": f"b2b_acquity_{source}", "meta": meta}, status=404)

    # -------- B2C ACUITY CASE --------
            elif business_type == "b2c" and client_name == "acuity":
                country_name = _normalize_country_or_market(str(request.data.get("market", "")))
                country=find_region(country_name)

                ir_in, loi_in = request.data.get("ir"), request.data.get("loi")
                if not country or ir_in is None or loi_in is None:
                    return Response({"status": "error", "message": "country, ir, loi required"}, status=400)

                price, source, meta = acuity_b2c_find_price(country, ir_in, loi_in)
                if price is not None:
                    return Response({
                        "status": "success",
                        "predicted_price": round(float(price), 2),
                        "source": f"b2c_acquity_{source}",
                        "matched_type": meta.get("matched_type"),
                        "matched_row": meta.get("row")
                    })
                return Response({"status": "error", "message": "No matching B2C Acuity rule found", "source": f"b2c_acquity_{source}", "meta": meta}, status=404)

            elif business_type == "b2b" and client_name and client_name.lower() != "acuity":
                dir_flag = request.data.get("dir")
                clevel_flag = request.data.get("clevel")
                
                price, meta = b2b_with_client_find_price(client_name, dir_flag, clevel_flag)

                if price is not None:
                    return Response({
                        "status": "success",
                        "predicted_price": round(float(price), 2),
                        "source": "b2b_clientwise",
                        "meta": meta
                    })
                return Response({"status": "error", "message": meta["message"], "source": "b2b_clientwise"}, status=404)
                        # -------- B2B CASE --------
            elif business_type == "b2b":

                country = _normalize_country_or_market(str(request.data.get("market", "")))
                print("*******withoutb2bClientcou",country)

                ir_in, loi_in = request.data.get("ir"), request.data.get("loi")
                if not country or ir_in is None or loi_in is None:
                    return Response({"status": "error", "message": "country, ir, loi required"}, status=400)

                price, source, meta = b2b_find_price(country, ir_in, loi_in)
                if price is not None:
                    return Response({
                        "status": "success",
                        "predicted_price": round(float(price), 2),
                        "source": f"b2b_{source}",
                        "matched_type": meta.get("matched_type"),
                        "matched_row": meta.get("row")
                    })
                return Response({"status": "error", "message": "No matching B2B rule found", "source": f"b2b_{source}", "meta": meta}, status=404)

            # -------- CONSUMER CASE --------
            else:
                if consumer_model is None and not consumer_lookup:
                    return Response({"status": "error", "message": "Consumer model not trained"}, status=400)

                # Normalize market with USA synonyms; everything else => INTERNATIONAL
                market_in = _normalize_country_or_market(str(request.data.get("market", "")))
                market = "USA" if market_in == "USA" else "INTERNATIONAL"

                # Robust IR/LOI parsing (accept ranges like 'ir- 5-9%')
                ir_range = parse_ir(request.data.get("ir"))
                loi_range = parse_loi(request.data.get("loi"))
                if not ir_range or not loi_range:
                    return Response({"status": "error", "message": "market, ir, loi required"}, status=400)

                # Use UPPER bound for pricing conservatism before bucketing
                ir_val = ir_range[1]
                loi_val = loi_range[1]

                mapped_ir = map_to_next_bucket(ir_val, ir_buckets)
                mapped_loi = map_to_next_bucket(loi_val, loi_buckets)

                # 1) Exact lookup
                key = (market, mapped_ir, mapped_loi)
                if key in consumer_lookup:
                    return Response({
                        "status": "success",
                        "predicted_price": round(float(consumer_lookup[key]), 2),
                        "source": "consumer_exact_lookup",
                        "market_used": market,
                        "mapped_ir": mapped_ir,
                        "mapped_loi": mapped_loi
                    })

                # 2) Nearest lookup for that market
                price_nearest, matched_ir, matched_loi = nearest_lookup_price_for_market(market, mapped_ir, mapped_loi)
                if price_nearest is not None:
                    return Response({
                        "status": "success",
                        "predicted_price": round(price_nearest, 2),
                        "source": "consumer_nearest_lookup",
                        "market_used": market,
                        "mapped_ir": mapped_ir,
                        "mapped_loi": mapped_loi,
                        "matched_bucket_ir": matched_ir,
                        "matched_bucket_loi": matched_loi
                    })

                # 3) Fallback to model
                if consumer_model is None:
                    return Response({"status": "error", "message": "No lookup match and model unavailable"}, status=400)

                input_market_df = pd.get_dummies(pd.Series([market]), prefix='market')
                input_numeric_df = pd.DataFrame([{'incidence_rate': mapped_ir, 'loi_minutes': mapped_loi}])
                input_encoded = pd.concat(
                    [input_market_df.reset_index(drop=True), input_numeric_df.reset_index(drop=True)],
                    axis=1
                ).reindex(columns=consumer_features, fill_value=0)

                predicted_price = consumer_model.predict(input_encoded)[0]
                return Response({
                    "status": "success",
                    "predicted_price": round(float(predicted_price), 2),
                    "source": "consumer_model",
                    "market_used": market,
                    "mapped_ir": mapped_ir,
                    "mapped_loi": mapped_loi
                })

        except Exception as e:
            return Response({"status": "error", "message": str(e)}, status=400)


