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
import random

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
    print("IRinput",ir_input,loi_input,country_upper)
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


def generate_cpi_message(request, country, price, surveytype): 
    ir_in = parse_ir(request.data.get("ir"))
    loi_in = parse_loi(request.data.get("loi"))

    # ✅ Apply condition for country
    if surveytype == "Consumer":
        country = country   # use argument
    else:
        country = request.data.get("market")  # use request data


    explanations = []

    # -------------------------------
    # All three missing (IR+LOI+Country)
    # -------------------------------
    if not ir_in and not loi_in and (not country or country.upper() in ["UNKNOWN", "N/A", "NONE"]):
        msgs = [
            f"Since IR, LOI, and country were all missing, I assumed IR=30%, LOI=15 minutes, and standard market benchmarks. Under these defaults, the CPI is **{price} USD**.",
            f"No inputs for IR, LOI, or country were provided. I proceeded with IR=30%, LOI=15, and general assumptions, giving a CPI of **{price} USD**.",
            f"As none of IR, LOI, or country were specified, I defaulted to IR=30%, LOI=15 minutes, and average market baselines. Estimated CPI is **{price} USD**.",
            f"Because IR, LOI, and country were missing, I applied industry norms: IR=30%, LOI=15, and global standards. CPI works out to **{price} USD**.",
            f"Inputs for IR, LOI, and country weren’t received. Using default IR=30%, LOI=15 mins, and typical rates, the CPI estimate is **{price} USD**.",
            f"Without IR, LOI, or country, I assumed IR=30%, LOI=15 minutes, and standard references. This yields a CPI of **{price} USD**.",
            f"Since all three inputs (IR, LOI, country) were absent, I proceeded with default IR=30%, LOI=15, and average assumptions. Predicted CPI: **{price} USD**.",
            f"IR, LOI, and country weren’t provided. I used default IR=30%, LOI=15 minutes, and baseline benchmarks. CPI comes to **{price} USD**.",
            f"With no IR, LOI, or country specified, I relied on fallback defaults (IR=30%, LOI=15, typical rates). CPI is estimated at **{price} USD**.",
            f"All inputs missing — I applied IR=30%, LOI=15 minutes, and standard assumptions. The CPI projection is **{price} USD**."
        ]
        explanations.append(random.choice(msgs))

    # -------------------------------
    # Two missing (IR+LOI)
    # -------------------------------
    elif not ir_in and not loi_in:
        msgs = [
            f"Both IR and LOI were missing, so I assumed IR=30% and LOI=15 minutes. Based on {country}, the CPI is **{price} USD**.",
            f"IR and LOI weren’t specified. I defaulted to IR=30%, LOI=15 mins for calculation. Estimated CPI: **{price} USD** in {country}.",
            f"No IR or LOI provided, so I proceeded with IR=30% and LOI=15 minutes. This results in a CPI of **{price} USD** for {country}.",
            f"Because both IR and LOI were absent, I used default IR=30% and LOI=15 mins. The CPI is **{price} USD** for {country}.",
            f"Inputs IR and LOI weren’t available, so I estimated with IR=30% and LOI=15 minutes. CPI = **{price} USD** in {country}.",
            f"Without IR and LOI, I applied standard defaults (IR=30%, LOI=15 mins). The CPI works out to **{price} USD** in {country}.",
            f"IR and LOI not specified — I assumed IR=30% and LOI=15 minutes. That produces a CPI of **{price} USD** for {country}.",
            f"As IR and LOI were missing, I continued with defaults: IR=30%, LOI=15. CPI = **{price} USD** in {country}.",
            f"Since IR and LOI weren’t given, I relied on standard IR=30% and LOI=15 minutes. CPI estimate: **{price} USD** ({country}).",
            f"Both IR and LOI absent — defaults applied (30%, 15 minutes). Predicted CPI for {country}: **{price} USD**."
        ]
        explanations.append(random.choice(msgs))

    # -------------------------------
    # Two missing (IR+Country)
    # -------------------------------
    elif not ir_in and (not country or country.upper() in ["UNKNOWN", "N/A", "NONE"]):
        msgs = [
            f"IR and country were missing, so I assumed IR=30% and general market benchmarks. With LOI={loi_in}, CPI = **{price} USD**.",
            f"No IR or country specified. Using IR=30% and standard assumptions, CPI is estimated at **{price} USD** (LOI={loi_in}).",
            f"Inputs for IR and country weren’t given. I used IR=30% and baseline standards. Resulting CPI: **{price} USD**.",
            f"IR and country absent — I defaulted to IR=30% and general averages. With LOI={loi_in}, CPI = **{price} USD**.",
            f"Because IR and country were missing, I worked with IR=30% and typical references. CPI = **{price} USD**.",
            f"Without IR and country, I assumed IR=30% and global benchmarks. CPI = **{price} USD**.",
            f"As IR and country weren’t provided, I applied defaults IR=30% and standard references. The CPI is **{price} USD**.",
            f"No IR or country detected. I used IR=30% and global estimates. Predicted CPI = **{price} USD**.",
            f"IR and country not available — defaulted to IR=30% and averages. CPI = **{price} USD**.",
            f"Since IR and country missing, fallback applied: IR=30%, global benchmarks. CPI = **{price} USD**."
        ]
        explanations.append(random.choice(msgs))

    # -------------------------------
    # Two missing (LOI+Country)
    # -------------------------------
    elif not loi_in and (not country or country.upper() in ["UNKNOWN", "N/A", "NONE"]):
        msgs = [
            f"LOI and country were not specified. I assumed LOI=15 minutes and global benchmarks. CPI = **{price} USD**.",
            f"No LOI or country input. Using LOI=15 and average assumptions, CPI = **{price} USD**.",
            f"LOI and country missing — I defaulted to LOI=15 mins and standard baselines. CPI works out to **{price} USD**.",
            f"Because LOI and country weren’t given, I applied LOI=15 minutes and global references. CPI = **{price} USD**.",
            f"As LOI and country absent, I used LOI=15 minutes and general assumptions. Estimated CPI: **{price} USD**.",
            f"Inputs for LOI and country were missing. I defaulted to LOI=15 and global standards. CPI = **{price} USD**.",
            f"Without LOI and country values, I assumed 15 minutes and typical references. CPI is **{price} USD**.",
            f"No LOI or country specified — I used defaults: LOI=15 mins, global rates. CPI: **{price} USD**.",
            f"LOI and country not provided — fallback applied: LOI=15, standard assumptions. CPI = **{price} USD**.",
            f"Since LOI and country missing, I proceeded with LOI=15 mins and market benchmarks. CPI = **{price} USD**."
        ]
        explanations.append(random.choice(msgs))

    # -------------------------------
    # Single missing (IR only)
    # -------------------------------
    elif not ir_in:
        msgs = [
            f"Since incidence rate (IR) wasn’t specified, I assumed 30% as a baseline and calculated the CPI at **{price} USD**. Providing the actual IR will refine this estimate.",
            f"IR was not provided, so I defaulted to 30% (a common benchmark). Based on this, the CPI works out to **{price} USD**. Sharing the exact IR will yield a more accurate result.",
            f"No IR input was given. I used 30% as a midpoint assumption, leading to a CPI prediction of **{price} USD**. Supplying the actual IR will improve precision.",
            f"IR wasn’t included, so I proceeded with 30% as a standard assumption. This results in an estimated CPI of **{price} USD**. Adding the real IR would enhance accuracy.",
            f"Because IR is missing, I estimated with a 30% assumption, which gives a CPI of **{price} USD**. Providing the true IR value will fine-tune the prediction.",
            f"No incidence rate detected — I applied 30% as the default and derived a CPI of **{price} USD**. Sharing the actual IR will strengthen the forecast.",
            f"To continue without an IR value, I assumed 30% (a moderate feasibility rate). This produces an estimated CPI of **{price} USD**. The actual IR will deliver a sharper estimate.",
            f"As IR wasn’t specified, I applied a conservative 30% rate. Under this assumption, the CPI is **{price} USD**. For better accuracy, please provide the actual IR.",
            f"IR input was blank, so I relied on a 30% midpoint assumption. This results in a CPI of **{price} USD**. The exact IR will help narrow the prediction range.",
            f"I didn’t receive an IR value, so I based the calculation on 30%, resulting in an estimated CPI of **{price} USD**. Adding the true IR would make the outcome more precise."
        ]
        explanations.append(random.choice(msgs))

    # -------------------------------
    # Single missing (LOI only)
    # -------------------------------
    elif not loi_in:
        msgs = [
            f"LOI wasn’t specified, so I assumed 15 minutes and estimated the CPI at **{price} USD**. Providing the real LOI will make this more accurate.",
            f"Since no LOI was provided, I worked with a 15-minute baseline. This leads to a CPI of **{price} USD**. Sharing the actual LOI would refine this estimate.",
            f"LOI input is missing, so I applied a default of 15 minutes. Based on that, the CPI is **{price} USD**. Supplying the exact LOI will improve precision.",
            f"No LOI was given, so I relied on a 15-minute assumption. The resulting CPI is **{price} USD**. Entering the actual LOI would enhance accuracy.",
            f"Because LOI wasn’t included, I considered 15 minutes as the survey length. That results in a CPI of **{price} USD**. Providing the correct LOI will fine-tune the prediction.",
            f"To proceed, I assumed a standard LOI of 15 minutes. This yields a CPI of **{price} USD**. For a sharper forecast, please provide the actual LOI.",
            f"As LOI wasn’t specified, I defaulted to 15 minutes. Under this assumption, the CPI is **{price} USD**. Supplying the real LOI would give a closer estimate.",
            f"LOI input was blank, so I calculated using 15 minutes as a typical benchmark. This gives a CPI of **{price} USD**. Entering the true LOI will improve accuracy.",
            f"I didn’t receive an LOI value, so I assumed 15 minutes for estimation. The predicted CPI is **{price} USD**. Sharing the actual LOI would enhance reliability.",
            f"No valid LOI provided — I worked with 15 minutes as the default. That leads to a CPI of **{price} USD**. Providing the exact LOI will strengthen the result."
        ]
        explanations.append(random.choice(msgs))

    # -------------------------------
    # Single missing (Country only)
    # -------------------------------
    elif not country or country.upper() in ["UNKNOWN", "N/A", "NONE"]:
        msgs = [
            f"Since country wasn’t specified, I applied general benchmarks. The estimated CPI is **{price} USD**. Providing the correct country will improve accuracy.",
            f"Country input was missing, so I defaulted to average market rates. This results in a CPI of **{price} USD**. Sharing the actual country would refine this.",
            f"No country was provided, so I assumed general averages. The CPI works out to **{price} USD**. Entering the true country will yield a more precise estimate.",
            f"Because no valid country was detected, I used baseline pricing references. This gives a CPI of **{price} USD**. Providing the real country will sharpen the forecast.",
            f"Country wasn’t included, so I applied standard baselines. The resulting CPI is **{price} USD**. For better accuracy, please specify the country.",
            f"As no country input was given, I worked with global assumptions. This leads to an estimated CPI of **{price} USD**. Supplying the country would enhance precision.",
            f"No valid country input, so I relied on benchmarks. Under this assumption, the CPI is **{price} USD**. Adding the actual country will fine-tune the estimate.",
            f"To proceed without a country value, I assumed market standards. That produces a CPI of **{price} USD**. Sharing the real country will improve reliability.",
            f"Country field was blank, so I used generic references. The CPI under this assumption is **{price} USD**. Providing the country will refine the calculation.",
            f"I didn’t receive a country input, so I applied fallback assumptions. This results in a CPI of **{price} USD**. Supplying the true country will strengthen accuracy."
        ]
        explanations.append(random.choice(msgs))

    # -------------------------------
    # Success (all present)
    # -------------------------------
    else:
        success_msgs = [
            f"Based on the provided inputs, the predicted CPI is **{price} USD**.",
            f"According to the given parameters, the estimated CPI is **{price} USD**.",
            f"Using the supplied details, our model calculates a CPI of **{price} USD**.",
            f"With the provided information, the projected CPI is **{price} USD**.",
            f"From the entered values, the expected CPI is **{price} USD**.",
            f"Based on your specifications, the CPI works out to **{price} USD**.",
            f"According to the input data, the suggested CPI is **{price} USD**.",
            f"Using your survey details, the calculated CPI comes to **{price} USD**.",
            f"Given the provided conditions, the estimated CPI is **{price} USD**.",
            f"With the supplied survey parameters, the model predicts a CPI of **{price} USD**."
        ]
        explanations.append(random.choice(success_msgs))

    return ir_in, loi_in, country, " ".join(explanations)

def generate_cpi_message_clientbutnotacuity(client,price):

    
    client = client

    explanations = []

    # If everything is valid → success message
    if not explanations:
        success_msgs = [
            f"Based on the provided details for client {client}, the predicted CPI is **{price} USD**.",
            f"Using the inputs for client {client}, the estimated CPI is **{price} USD**.",
            f"For client {client}, our model suggests a CPI of **{price} USD**.",
            f"With the supplied parameters for client {client}, the expected CPI is **{price} USD**.",
            f"According to the provided information, client {client} has a projected CPI of **{price} USD**.",
            f"From the given details, the calculated CPI for client {client} is **{price} USD**.",
            f"Based on the specifications, the CPI for client {client} works out to **{price} USD**.",
            f"Given the input data, client {client} is expected to have a CPI of **{price} USD**.",
            f"Using your survey details, the model predicts a CPI of **{price} USD** for client {client}.",
            f"With the entered parameters, client {client} has an estimated CPI of **{price} USD**."
        ]
        explanations.append(random.choice(success_msgs))

    return  " ".join(explanations)




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
                print("Running.....................BY_B2B acuity")

                country_name = _normalize_country_or_market(str(request.data.get("market", "")))
                country=find_region(country_name)

                # If blank or unknown, force fallback to USA
                if not country or country.upper() in ["UNKNOWN", "N/A", "NONE"]:
                    country = "USA"

                # Apply defaults for IR/LOI if not provided
                ir_in = request.data.get("ir") or 30
                loi_in = request.data.get("loi") or 15

                price, source, meta = acuity_b2b_find_price(country,ir_in,loi_in)
                # Step 3: generate message (pass resolved values + price)
                ir_in, loi_in, country, final_msg = generate_cpi_message(request, country, price,surveytype=None)

                if price is not None:
                    return Response({
                        "status": "success",
                        "predicted_price": round(float(price), 2),
                        "source": f"b2b_acquity_{source}",
                        "matched_type": meta.get("matched_type"),
                        "matched_row": meta.get("row"),
                        "assumptions": final_msg  # 👈 tell user what defaults were applied

                    })
                return Response({"status": "error", "message": "No matching B2B Acuity rule found", "source": f"b2b_acquity_{source}", "meta": meta}, status=404)

    # -------- B2C ACUITY CASE --------
            elif business_type == "b2c" and client_name == "acuity":
                print("Running.....................BY_B2C acuity")

                country_name = _normalize_country_or_market(str(request.data.get("market", "")))
                country=find_region(country_name)

                # If blank or unknown, force fallback to USA
                if not country or country.upper() in ["UNKNOWN", "N/A", "NONE"]:
                    country = "USA"

                # Apply defaults for IR/LOI if not provided
                ir_in = request.data.get("ir") or 30
                loi_in = request.data.get("loi") or 15

                price, source, meta = acuity_b2c_find_price(country, ir_in, loi_in)
                # Step 3: generate message (pass resolved values + price)
                ir_in, loi_in, country, final_msg = generate_cpi_message(request, country, price,surveytype=None)

                if price is not None:
                    return Response({
                        "status": "success",
                        "predicted_price": round(float(price), 2),
                        "source": f"b2c_acquity_{source}",
                        "matched_type": meta.get("matched_type"),
                        "matched_row": meta.get("row"),
                        "assumptions": final_msg  # 👈 tell user what defaults were applied
                    })
                return Response({"status": "error", "message": "No matching B2C Acuity rule found", "source": f"b2c_acquity_{source}", "meta": meta}, status=404)

            elif business_type == "b2b" and client_name and client_name.lower() != "acuity":
                print("Running.....................BY_Clint_wise_but not acuity")

                dir_flag = request.data.get("dir")
                clevel_flag = request.data.get("clevel")
                
                price, meta = b2b_with_client_find_price(client_name, dir_flag, clevel_flag)
                final_msg=generate_cpi_message_clientbutnotacuity(client_name,price)

                if price is not None:
                    return Response({
                        "status": "success",
                        "predicted_price": round(float(price), 2),
                        "source": "b2b_clientwise",
                        "meta": meta,
                        "assumptions": final_msg 
                    })
                return Response({"status": "error", "message": meta["message"], "source": "b2b_clientwise"}, status=404)
                        # -------- B2B CASE --------
            elif business_type == "b2b":
                print("Running.....................BY_default_B2B")

                country = _normalize_country_or_market(str(request.data.get("market", "")))

                if not country or country.upper() in ["UNKNOWN", "N/A", "NONE"]:
                    country = "USA"

                # Apply defaults for IR/LOI if not provided
                
                ir_in = parse_ir(request.data.get("ir") or 30) 
                loi_in = parse_loi(request.data.get("loi") or 15) 
                
                print("***details...",ir_in,loi_in,country)

                price, source, meta = b2b_find_price(country, ir_in, loi_in)
                # Step 3: generate message (pass resolved values + price)
                ir_in, loi_in, country, final_msg = generate_cpi_message(request, country, price,surveytype=None)

                if price is not None:
                    return Response({
                        "status": "success",
                        "predicted_price": round(float(price), 2),
                        "source": f"b2b_{source}",
                        "matched_type": meta.get("matched_type"),
                        "matched_row": meta.get("row"),
                        "assumptions": final_msg  # 👈 tell user what defaults were applied

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
                ir_range = parse_ir(request.data.get("ir") or 30) 
                loi_range = parse_loi(request.data.get("loi") or 15) 
                
                print("Running.....................BY_default_consumer")


                # Use UPPER bound for pricing conservatism before bucketing
                ir_val = ir_range[1]
                loi_val = loi_range[1]

                mapped_ir = map_to_next_bucket(ir_val, ir_buckets)
                mapped_loi = map_to_next_bucket(loi_val, loi_buckets)

                # 1) Exact lookup
                key = (market, mapped_ir, mapped_loi)
                if key in consumer_lookup:
                    ir_in, loi_in, country, final_msg = generate_cpi_message(request, market, consumer_lookup[key],surveytype='Consumer')

                    return Response({
                        "status": "success",
                        "predicted_price": round(float(consumer_lookup[key]), 2),
                        "source": "consumer_exact_lookup",
                        "market_used": market,
                        "mapped_ir": mapped_ir,
                        "mapped_loi": mapped_loi,
                        "assumptions": final_msg
                    })

                # 2) Nearest lookup for that market
                price_nearest, matched_ir, matched_loi = nearest_lookup_price_for_market(market, mapped_ir, mapped_loi)
                ir_in, loi_in, country, final_msg = generate_cpi_message(request, market, price_nearest,surveytype='Consumer')

                if price_nearest is not None:
                    return Response({
                        "status": "success",
                        "predicted_price": round(price_nearest, 2),
                        "source": "consumer_nearest_lookup",
                        "market_used": market,
                        "mapped_ir": mapped_ir,
                        "mapped_loi": mapped_loi,
                        "matched_bucket_ir": matched_ir,
                        "matched_bucket_loi": matched_loi,
                        "assumptions": final_msg
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
                ir_in, loi_in, country, final_msg = generate_cpi_message(request, market, predicted_price,surveytype='Consumer')

                return Response({
                    "status": "success",
                    "predicted_price": round(float(predicted_price), 2),
                    "source": "consumer_model",
                    "market_used": market,
                    "mapped_ir": mapped_ir,
                    "mapped_loi": mapped_loi,
                    "assumptions": final_msg
                })

        except Exception as e:
            return Response({"status": "error", "message": str(e)}, status=400)


