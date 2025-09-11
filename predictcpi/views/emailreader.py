from rest_framework.views import APIView

from django.http import JsonResponse
from django.db import connection, transaction
from django.db.utils import IntegrityError, DataError
from django.utils.decorators import method_decorator
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.conf import settings
import msal 
from dotenv import load_dotenv
import re
import json
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import time, requests

import random
import os
import numpy as np
import pickle
import pandas as pd
import unicodedata



load_dotenv()

# Get access token using username-password
def get_access_token():

    msal_app = msal.ConfidentialClientApplication(
        settings.CLIENT_ID,
        authority=f'https://login.microsoftonline.com/{settings.TENANT_ID}',
        client_credential=settings.CLIENT_SECRET
    )

    scopes = ['https://graph.microsoft.com/.default']
    result = msal_app.acquire_token_by_username_password(
        username=settings.EMAIL,
        password=settings.PASSWORD,
        scopes=scopes
    )

    if "access_token" in result:
        return result["access_token"]
    else:
        raise Exception("Failed to get access token: " + str(result.get("error_description")))



key_aliases = {
    "market": [
        "country", "market", "market area", "country of field", "geo", "geos", "geo/s", "country/geos", 
        "country/geo", "markets", "market/region", "geography", "countries", "fieldwork country", 
        "region", "regions", "sample country", "survey market", "survey markets", "city", "cities", "zip code", 
        "zipcode", "postal code", "postalcode", "area", "location", "locations", "Market (geographical location)", 
        "country(ies)", "country (ies)", "country of field", "target countries", "target countries", "target country(ies)", "Geographically", "geographical",
    ],
    "methodology": ["methodology", "method"],
    "industries": ["industries", "industry", "industry and role", "target industry", "target industries", "company industry", "industry vertical",
                   "sector/industry",],
    "target_audience": ["target", "target audience", "targeting", "audience", "targeting audience", 
        "targeting details", "targeting detail", "target Definition", "target role", "target role(s)", "target role (s)",
        "target group", "target respondent"
    ],
    "n": [
        "required", "n size", "n", "sample", "completes", "req. n", "required n", "n required" ,"needed n", 
        "sample size", "size", "n needed", "needed completes", "needed complete", "number of completes",
        "number of completes(n)", "number of completes (n)", "total n", "sample specs", "sample specification",
        "Number of completes needed", "no. of completes" ,"needed", "need", "n-size", "n-needed"
    ],
    "ir": [
        "ir", "incidence", "expected ir", "assumed incidence", "incidence rate", "incidence rate -ir", "estimated ir", 
        "assumed incidence (targeted)", "assumed incidence (target)"
    ],
    "loi": [
        "loi", "estimated loi", "estimated loi(min)", "estimated loi (mins)", "estimated loi(mins)", 
        "estimated loi (min)", "survey length", "survey length(min)", "survey length (min)",
        "survey length(mins)", "survey length (mins)", "loi (mins)", "loi(min)", "loi(mins)",
        "loi (min)", "length of interview (loi)", "length of interview(loi)", "estimated online loi",
        "survey loi", "loi (minutes)", "loi(minutes)", "loi (minute)", "loi(minute)"
    ],
    "devices": ["devices", "device compatibility", "device", "device/s", "device type", "device(s)", "devices allowed", 
                "device allowed", "device agnostic", "device agnostics" , "device(s) agnostic", "device(s) agnostics"],
    "field_time": [
        "field time", "field time(days)", "field time (days)", "field time(day)",
        "field time (day)", "required field time", "required field time(day)", "Business days in field", 
        "days in field" , "required field time (day)", "required field time(days)", "required field time (days)",
        "field end", "time in field", "time in field (day)", "time in field (days)",  "required time in field"
    ],
    "requested_cpi": ["requested cpi", "cpi", "your cpi in usd", "cpc", "requested cpc", "budget", "cpi needed"],
    "feasibility": ["your feasibility", "feasibility"],
    "quotas": ["quota", "quotas", "quota details", "quota detail", "quotas details", "quotas detail",
               "specific quotas / mins", "specific quotas", "specific quota", "quotas (soi)", "quotas(soi)", "targetable quota",
               "targetable quotas"
    ],
    "survey_type": ["survey type"],
    "survey_topic": ["survey topic"],
    "departments" : ["department", "departments"],
    "languages" : ["language", "languages", "language/s","survey language", "questionnaire language", 
        "questionnaire languages", "questionnaire language(s)", "questionnaire language (s)", "Language(s)",
        "language (s)"
    ],
    "number_of_open_ends" : [ "number of open ends", "number of open end", "no. of open ends", "no. of open end", 
        "# of open ends", "the number of open ends", "open end", "open ends", "# of Open Ends (OEs)?", 
        "# of open ends (oes) ?", "number of open-ended questions", 
    ],
    # "company_size" : ["company size"],
    "field_work" : ["field work"],
    "eligibility_criteria" : ["eligibility criteria", "respondent criteria"],
    "from" : ["from"],
    "sent" : ["sent", "date"],
    "to" : ["to"],
    "cc" : ["cc"],
    "subject": ["subject"],
    "dm_type" : [],
    "decision_maker" : [],
    "client_name" : [],
}


known_countries = [

    "Afghanistan",  # list of all countries in the world
    "Albania", 
    "Algeria", "Andorra", "Angola", "Antigua and Barbuda", "Argentina", "Armenia",
    "Austria", "Azerbaijan", "Bahamas", "Bahrain", "Bangladesh", "Barbados", "Belarus", "Belgium",
    "Belize", "Benin", "Bhutan", "Bolivia", "Bosnia and Herzegovina", "Botswana", "Brunei", "Bulgaria",
    "Burkina Faso", "Burundi", "Cabo Verde", "Cambodia", "Cameroon", "Central African Republic", "Chad",
    "Colombia", "Comoros", "Congo (Congo-Brazzaville)", "Costa Rica", "Croatia", "Cuba", "Cyprus",
    "Czech Republic (Czechia)", "Democratic Republic of the Congo", "Denmark", "Djibouti", "Dominica",
    "Dominican Republic", "Ecuador", "Egypt", "El Salvador", "Equatorial Guinea", "Eritrea", "Estonia",
    "Eswatini (fmr. Swaziland)", "Ethiopia", "Fiji", "Finland", "Gabon", "Gambia", "Georgia",
    "Ghana", "Greece", "Grenada", "Guatemala", "Guinea", "Guinea-Bissau", "Guyana", "Haiti", "Honduras", "Hungary",
    "Iceland", "Iran", "Iraq", "Ireland", "Israel", "Ivory Coast", "Jamaica",
    "Jordan", "Kazakhstan", "Kenya", "Kiribati", "Kuwait", "Kyrgyzstan", "Laos", "Latvia", "Lebanon", "Lesotho",
    "Liberia", "Libya", "Liechtenstein", "Lithuania", "Luxembourg", "Madagascar", "Malawi",  "Maldives",
    "Mali", "Malta", "Marshall Islands", "Mauritania", "Mauritius", "Micronesia", "Moldova", "Monaco",
    "Mongolia", "Montenegro", "Morocco", "Mozambique", "Myanmar (formerly Burma)", "Namibia", "Nauru", "Nepal",
    "Nicaragua", "Niger", "Nigeria", "North Korea", "North Macedonia", "Norway", "Oman",
    "Pakistan", "Palau", "Palestine State", "Panama", "Papua New Guinea", "Paraguay", "Peru", "Poland",
    "Qatar", "Romania", "Rwanda", "Saint Kitts and Nevis", "Saint Lucia",
    "Saint Vincent and the Grenadines", "Samoa", "San Marino", "Sao Tome and Principe", "Saudi Arabia", "Senegal",
    "Serbia", "Seychelles", "Sierra Leone", "Slovakia", "Slovenia", "Solomon Islands", "Somalia", 
    "South Sudan", "Sri Lanka", "Sudan", "Suriname",
    "Syria", "Taiwan", "Tajikistan", "Tanzania", "Thailand", "Timor-Leste", "Togo", "Tonga", "Trinidad and Tobago",
    "Tunisia", "Turkmenistan", "Tuvalu", "Uganda", "Ukraine", "United Arab Emirates",
    "Uruguay", "Uzbekistan", "Vanuatu", "Vatican City", "Venezuela", "Yemen",
    "Zambia", "Zimbabwe",

    "USA",          # list of countries already saved in DB
    "United States", 
    "India", "United Kingdom", "Canada", "Germany",
    "France", "Australia", "Brazil", "China", "Japan", "Italy", "Spain", "Russia", "Mexico", "portugal",
    "South Africa", "Indonesia", "Netherlands", "Sweden", "Switzerland", "Turkey", "South Korea",
    "alberta", "singapore", "United States of america", "Malaysia", "Hong Kong",
    "Philippines", "DE", "LATAM", "Chile", "ASEAN", "Vietnam", "New Zealand", 
    "Sao Paulo", "London", "Austin", "NYC (tri-state area)", "NYC", "Chicago", "San Francisco",
    "UK", "UAE",
    "CA","AU", "BR", "CL", "CN", "ES", "FR", "JP", "SG", "MX", "KSA", "SW",
    "GER", "BRA",
    # "US",     # also present in sentences
    # "IN",      # also present in sentences
    # "IT",      # also present in sentences

    "European Union", # list of international country groups
    "G7", "G20", "BRICS", "OPEC", "SAARC", "NATO",  

    "europe",           # list of continents
    "africa", "antartica", "asia", "north america", "south america", "oceania",
   
]


def extract_industries(full_text):

    full_text = remove_any_slash_n(full_text)
    separators = [":", "-"]

    industries_aliases = key_aliases["industries"]

    # Flatten all other aliases (excluding industries)
    other_aliases = [alias for key, aliases in key_aliases.items() if key != "industries" for alias in aliases]

    # Build boundary pattern: alias + separator
    boundary_pattern = (
        r"(?:" + "|".join(map(re.escape, other_aliases)) + r")\s*(?:" + "|".join(map(re.escape, separators)) + r")"
    )

    #   alias + separator → capture everything until another "alias + separator"
    pattern = (
        r"(?i)(?:" + "|".join(map(re.escape, industries_aliases)) + r")\s*(?:" + "|".join(map(re.escape, separators)) + r")\s*(.*?)(?=" + boundary_pattern + r"|$)"
    )

    match = re.search(pattern, full_text)

    if not match:
        return ""
    
    result =  match.group(1).strip()

    keys_to_combine = ["n", "loi", "devices" + "field_time" + "requested_cpi" + "feasibility" + "quotas" + "number_of_open_ends" + "eligibility_criteria"]

    combined_list = []
    for key in keys_to_combine:
        combined_list.extend(key_aliases.get(key, []))  # .get to avoid KeyError if key missing

    # ⬇️ New part: post-processing
    stop_phrases = ["thank", "regards", "thanks", "disclaimer", "caution", "kind", "best" ]  # 👈 add any stop strings here
    stop_phrases =  combined_list + stop_phrases

    # ✅ Fixed stop phrase handling
    for stop in stop_phrases:
        m = re.search(r"\b" + re.escape(stop) + r"\b", result, flags=re.IGNORECASE)
        if m:
            return result[:m.start()].strip()

    return result



def classify_household(full_text: str) -> str | None:
    """
    Classify text as 'b2c' if it contains household + decision maker keywords,
    'b2b' if it only contains decision maker keywords,
    or None if neither is found.
    """

    dm_keywords = [
        r"decision\s*maker[s]?",
        r"decision-?maker[s]?",
        r"dm[s]?",
        r"decisionmaker[s]?"
    ]

    # household exception → B2C
    b2c_pattern = re.compile(
        r"(house\s*hold|household)\s*(" + "|".join(dm_keywords) + r")",
        re.IGNORECASE
    )

    # General DM keywords → B2B
    b2b_pattern = re.compile(
        r"(" + "|".join(dm_keywords) + r")",
        re.IGNORECASE
    )

    if b2c_pattern.search(full_text):
        return "b2c"
    elif b2b_pattern.search(full_text):
        return "b2b"
    else:
        return None


def find_decision_maker(full_text):
   
    matches = set()
    pet_keywords = [ "pet store", "pet shop", "pet" ]
    pet_matches = [kw for kw in pet_keywords if kw.lower() in full_text.lower()]

    if pet_matches:
        return "b2b"
    
    
    liquor_keywords = [ "liquor store", "liquor shop", "liquor" ]
    liquor_matches = [kw for kw in liquor_keywords if kw.lower() in full_text.lower()]

    if liquor_matches:
        return "b2b"

    b2b_keywords = [ "b2b", "b-2-b", "Business-to-Business", "Business-2-Business", "Business 2 Business", "Manager+ & Sr. Manager", "Director+ & VP+ Titles", "C-level"]
    b2b_matches = [kw for kw in b2b_keywords if kw.lower() in full_text.lower()]

    if b2b_matches:
        return "b2b"
    
    automotive_keywords = [ "automative dealership", "automotive dealership", "automotive" ]
    automotive_matches = [kw for kw in automotive_keywords if kw.lower() in full_text.lower()]

    if automotive_matches:
        return "b2b"
    
    result = classify_household(full_text)

    if result == "b2c":
        return result
    
    elif result == "b2b":
        return result

    b2c_keywords = [  
        "General Population", "gen. pop", "gen pop.", "gen. pop.", "gen. population", "gen population",
        "Males and Females of any specific above criteria's", 
        "Primary Grocery Shoppers", "grocery stores", "grocery shops", "grocery store", "grocery shop",
        "Household Decision Makers",
        "Those who have taken loan", "borrowed loan", "loan with", "loan",
        "Entertainment Survey or those who like to watch TV/ Movie etc.", 
        "Online Viewing App Subscribers/ streamers like Netflix etc.",
        "Travellers and those who have taken flight for business or leisure",
        "High net worth income individuals",
        "Gamers who play online vs offline games",
        "Music enthusiasts or music listeners/ Youtube videos",
        "Vehicle owners or those who intend to buy a vehicle",
        "Registered Voters",
        "Luxury Product Buyers bags, watches etc.",
        "Credit Card Users with Reward Programs",
        "Smart Home Device Users (Alexa, Google Home, etc.)",
        "Tech Enthusiasts / Early Adopters of New Gadgets",
        "Mobile App Users (e.g., finance, health, fitness, etc.)",
        "Food Delivery App Users (e.g., Uber Eats)", 
        "Parents of Young Children",
        "Pet Owners / Pet Care Buyers",
        "Chronic Illness Patients / Caregivers",
        "First-Time Parents or pregnant women's",
        "College/University Students", "college student", "university student",
    ]

    b2c_matches = [kw for kw in b2c_keywords if kw.lower() in full_text.lower()]

    if b2c_matches:
        return "b2c"

    b2c_keywords_2 = [  
        "Mobile phone users",
        "Parents of 18 YO",
        "Feale/ Male Shoppers",
        "Owners of PS5- gamers",
        "Users of generative AI chatbot platforms",
        "Music streamers – Spotify, YouTube, or Apple Music",
        "Multiple language speakers",
        "Leisure activity/Traverllers",
        "Parents of kids",
        "Vehicle owners",
        "Intenders/purchased vehicle brand within the next 2 years",
        "Banked individuals aged 18+",
        "Primary Grocery Shoppers",
        "House hold Decision makers",
        "House hold DMs for Insurance",
        "Homeowners",
        "Gamers",
        "Credit card holders",
        "Students",
        "Smokers",
        "Alocohol/Drinkers",
        "HH DMs for Baby food/milk",
    ]

    b2c_matches_2 = [kw for kw in b2c_keywords_2 if kw.lower() in full_text.lower()]

    if b2c_matches_2:
        return "b2c"

    # dm_keywords = ["decision maker", "decision makers", "decision-makers", "decision-maker", "DM" , "DMs", "decisionmaker", "decisionmakers"]
    # dm_matches = [kw for kw in dm_keywords if kw.lower() in full_text.lower()]

    # if dm_matches:
    #     matches.add("b2b")    

    return "b2c" 


def is_acuity(full_text):

    keywords = ["acuity"]
    
    # Find which keywords appear in full_text (case-insensitive)
    matches = [kw for kw in keywords if kw.lower() in full_text.lower()]

    if matches:
        return True
    
    return False


def loi_fallback(full_text):

    # Aliases for minutes (can be extended easily)
    minute_aliases = ["min", "min.", "mins", "mins.", "minute", "minutes", "-minute", "-minutes", "-min", "-mins"]
    
    # Regex:
    #   (\d+(?:-\d+)?) → matches either a single number (15) or a range (10-15)
    #   \s*            → optional whitespace
    #   (?:aliases)\b  → minute aliases
    pattern = r"(\d+(?:-\d+)?)\s*(?:" + "|".join(minute_aliases) + r")\b"
    
    # Find all matches (case-insensitive)
    matches = re.findall(pattern, full_text, flags=re.IGNORECASE)
    
    # Return the last match if found, else None
    return matches[-1] if matches else ""


def ir_fallback(full_text): 
    # Aliases for percentage
    percent_aliases = ["%", "percent", "percentage", "-percent", "-percentage"]
    
    # Regex to capture number + alias together
    pattern = r"(\d+(?:\.\d+)?)\s*(?:" + "|".join(map(re.escape, percent_aliases)) + r")"
    
    # finditer → keeps the full match, not just digits
    matches = [m.group(0) for m in re.finditer(pattern, full_text, flags=re.IGNORECASE)]
    
    if not matches:
        return ""
    
    else:                                    #[first_match, last_match]
        # return [matches[0].strip(), matches[-1].strip()]       
        return matches[0].strip()    



def clean_ir(text):

    # combine and sort by length (longest first) to avoid partial matches
    all_aliases = sorted(key_aliases["loi"] + key_aliases["n"], key=len, reverse=True)

    # escape regex special characters in aliases
    escaped_aliases = [re.escape(alias) for alias in all_aliases]

    # build regex pattern for word boundaries
    pattern = r"(.*?)\b(" + "|".join(escaped_aliases) + r")\b"

    match = re.search(pattern, text, flags=re.IGNORECASE)
    if match:
        return match.group(1).strip(" :.-")  # clean extra separators
    return text.strip()  # if no alias found, return original


# def loi_fallback(full_text: str):
#     # Aliases for minutes (can be extended easily)
#     minute_aliases = ["min", "mins", "minute", "minutes"]
    
#     # Regex:
#     #   (?<!%) → ensure the number is not immediately after a % (avoid percentages like 10-20%)
#     #   (\d+(?:-\d+)?) → match either a single number (12) or a range (12-23)
#     #   \s* → allow optional spaces
#     #   (?:aliases)\b → require one of the minute aliases
#     pattern = r"(?<![%\d])(\d+(?:-\d+)?)\s*(?:" + "|".join(minute_aliases) + r")\b"
    
#     matches = re.findall(pattern, full_text, flags=re.IGNORECASE)
    
#     return matches[-1] if matches else None


def extract_key_value_pairs(text):

    alias_map = {alias.lower(): norm_key for norm_key, aliases in key_aliases.items() for alias in aliases}
    known_keys = set(alias_map.keys())

    lines = text.splitlines()
    extracted = {}
    current_key = None
    buffer = []

    i = 0
    while i < len(lines):
        line = lines[i].strip()
        if not line:
            i += 1
            continue

        # Check for a known key pattern (e.g., "Target-" or "IR-")
        # key_match = re.match(r"^([A-Za-z0-9\s&+/]+)\s?[\-:]\s*(.*)", line)  # old
        key_match = re.match(r"^([A-Za-z0-9\s&+/()%-]+)\s?[\-:]\s*(.*)", line)      # handles bracket. eg.) LOI (min): 10, but not LOI(min) : 10
        # key_match = re.match(r"^([A-Za-z0-9\s&+/()%-]+?)\s?[\-:]\s*(.+)", line)     # latest - working on eg.) target - manager - of bank , loi - 10-15 min

        if key_match:
            raw_key, value = key_match.groups()
            key_normalized = raw_key.strip().lower()

            normalized_key = alias_map.get(key_normalized)
            if normalized_key:
                # Save previous buffer if any
                if current_key and buffer:
                    extracted[current_key] = '\n'.join(buffer).strip()
                    buffer = []

                # Start new key
                current_key = normalized_key
                if value:
                    extracted[current_key] = value.strip()
                    current_key = None  # clear current key unless we need to collect
                else:
                    buffer = []
                i += 1
                continue

        # If line itself is a known key on its own (like "Target"), and next line is ": value"
        if i + 1 < len(lines):
            next_line = lines[i + 1].strip()
            if re.match(r"^:\s*(.+)", next_line):
                key_normalized = line.strip().lower()
                normalized_key = alias_map.get(key_normalized)
                if normalized_key:
                    if current_key and buffer:
                        extracted[current_key] = '\n'.join(buffer).strip()
                        buffer = []

                    current_key = normalized_key
                    value = next_line[1:].strip()
                    extracted[current_key] = value
                    current_key = None
                    i += 2
                    continue

        # Otherwise, treat as part of previous key's value if current_key is active
        if current_key:
            # Check if line matches a new key to stop buffering
            maybe_new_key = re.match(r"^([A-Za-z0-9\s&+/]+)[\-:]", line)
            if maybe_new_key:
                maybe_key = maybe_new_key.group(1).strip().lower()
                if maybe_key in known_keys:
                    extracted[current_key] = '\n'.join(buffer).strip()
                    buffer = []
                    current_key = None
                    continue  # don't consume line yet; let main loop process it

            buffer.append(line)

        i += 1

    # Final flush
    if current_key and buffer:
        extracted[current_key] = '\n'.join(buffer).strip()
    
    # Post-processing: Add 'devices' if "all device(s)" is mentioned and devices key is missing or empty
    full_text = text.lower()

    if ("all device" in full_text or "all devices" in full_text):
        if "devices" not in extracted or not extracted["devices"].strip():
            extracted["devices"] = "all devices"
    
    # Check if 'all' is present in devices value (case-insensitive)
    if 'devices' in extracted and re.search(r'\ball\b', extracted["devices"], re.IGNORECASE):
        extracted["devices"] = "all devices"
    
    # for cases like - eg.) "desktop only\nplease share your best costs and feasibility!"
    if 'devices' in extracted:
        extracted["devices"] = re.sub(r'\n.*', '', extracted["devices"])     # takes text before \n
    
    if 'devices' not in extracted:
        extracted['devices'] = extract_device_keywords(text)

    # Check if 'n' key exists and contains '@'
    if "n" in extracted and  re.search(r'@', extracted["n"]):
        match = re.match(r'(.+?)\s*@\s*([$€₹]?\d+(?:\.\d{1,2})?)', extracted["n"])  # also handles spaces in btw, eg.) N- 100 per country @ $10.00

        if match:
            extracted["n"] = match.group(1)         # "1000"
            extracted["requested_cpi"] = match.group(2)       # "$8.00"

    # if "field_time" in extracted and extracted["field_time"].strip():
    if "field_time" in extracted :
        match = re.match(r'^(.*?)\n', extracted['field_time'], re.IGNORECASE) 

        if match:
            extracted['field_time'] = match.group(1)

    # Use regex to get everything before the first newline 
    if "methodology" in extracted:
        match = re.match(r'^(.*?)(?:\n|$)', extracted['methodology'])

        if match:
            extracted['methodology'] = match.group(1)

    if "loi" in extracted:
        match = re.match(r'^(.*?)(?:\n|$)', extracted['loi'])

        if match:
            extracted['loi'] = match.group(1)

    if "ir" in extracted:
        match = re.match(r'^(.*?)(?:\n|$)', extracted['ir'])

        if match:
            extracted['ir'] = match.group(1)
    
    # for case where "Incidence rate - IR : 35%", it will not catch { ir : IR }
    if 'ir' in extracted and not re.search(r'\d', extracted['ir']):
        del extracted['ir']
    
    # extract first part before a newline
    if "n" in extracted:
        match = re.match(r'^(.*?)(?:\n|$)', extracted['n'])

        if match:
            extracted['n'] = match.group(1)   

    extracted_2 = extract_key_value_pairs_2(text)

    if 'target_audience' in extracted_2:
        extracted['target_audience'] = extracted_2['target_audience']

    if 'loi' in extracted_2:
        extracted['loi'] =  extracted_2['loi']
    
    if 'ir' in extracted_2:
        extracted['ir'] =  extracted_2['ir']

    if 'ir' in extracted:
        extracted['ir'] = clean_ir(extracted['ir'])
    
    if 'industries' in extracted_2:
        extracted['industries'] =  extracted_2['industries']
    
    if 'field_time' in extracted_2:
        extracted['field_time'] =  extracted_2['field_time']
    
    if 'quotas' in extracted_2:
        extracted['quotas'] =  extracted_2['quotas']

    if 'market' in extracted_2:
        extracted['market'] =  extracted_2['market']

    if 'eligibility_criteria' in extracted_2:
        extracted['eligibility_criteria'] =  extracted_2['eligibility_criteria']

    if 'loi' not in extracted :
        temp_loi = loi_fallback(full_text)
        if temp_loi:
            extracted['loi'] = temp_loi

    quota_keywords = ["no quotas", "no quota"]

    if any(keyword in full_text for keyword in quota_keywords):
        if "quotas" not in extracted or not extracted["quotas"].strip():
            extracted["quotas"] = "no quotas"

    extracted['dm_type'] = find_decision_maker(full_text)

    if extracted["dm_type"] == "b2b" :
        extracted['decision_maker'] = "Yes"
    else :
        extracted['decision_maker'] = "No"


    full_industries = extract_industries(full_text)
    if full_industries and extracted.get('industries', '') and len(full_industries) > len( extracted['industries']):
        extracted['industries'] = full_industries

    
    # if full_industries:
    #     extracted['industries'] = full_industries

    if is_acuity(full_text):
        extracted['client_name'] = "Acuity"
    
    extracted = filter_required_keys_only(extracted)

    return extracted


# only for target - xyz genz - abc, loi - 10-15 min
def extract_key_value_pairs_2(text):

    alias_map = {alias.lower(): norm_key for norm_key, aliases in key_aliases.items() for alias in aliases}
    known_keys = set(alias_map.keys())

    lines = text.splitlines()
    extracted = {}
    current_key = None
    buffer = []

    i = 0
    while i < len(lines):
        line = lines[i].strip()
        if not line:
            i += 1
            continue

        # Check for a known key pattern (e.g., "Target-" or "IR-")
        # key_match = re.match(r"^([A-Za-z0-9\s&+/]+)\s?[\-:]\s*(.*)", line)  # old
        # key_match = re.match(r"^([A-Za-z0-9\s&+/()%-]+)\s?[\-:]\s*(.*)", line)      # handles bracket. eg.) LOI (min): 10, but not LOI(min) : 10
        key_match = re.match(r"^([A-Za-z0-9\s&+/()%-]+?)\s?[\-:]\s*(.+)", line)     # latest - working on eg.) target - manager - of bank , loi - 10-15 min

        if key_match:
            raw_key, value = key_match.groups()
            key_normalized = raw_key.strip().lower()

            normalized_key = alias_map.get(key_normalized)
            if normalized_key:
                # Save previous buffer if any
                if current_key and buffer:
                    extracted[current_key] = '\n'.join(buffer).strip()
                    buffer = []

                # Start new key
                current_key = normalized_key
                if value:
                    extracted[current_key] = value.strip()
                    current_key = None  # clear current key unless we need to collect
                else:
                    buffer = []
                i += 1
                continue

        # If line itself is a known key on its own (like "Target"), and next line is ": value"
        if i + 1 < len(lines):
            next_line = lines[i + 1].strip()
            if re.match(r"^:\s*(.+)", next_line):
                key_normalized = line.strip().lower()
                normalized_key = alias_map.get(key_normalized)
                if normalized_key:
                    if current_key and buffer:
                        extracted[current_key] = '\n'.join(buffer).strip()
                        buffer = []

                    current_key = normalized_key
                    value = next_line[1:].strip()
                    extracted[current_key] = value
                    current_key = None
                    i += 2
                    continue

        # Otherwise, treat as part of previous key's value if current_key is active
        if current_key:
            # Check if line matches a new key to stop buffering
            maybe_new_key = re.match(r"^([A-Za-z0-9\s&+/]+)[\-:]", line)
            if maybe_new_key:
                maybe_key = maybe_new_key.group(1).strip().lower()
                if maybe_key in known_keys:
                    extracted[current_key] = '\n'.join(buffer).strip()
                    buffer = []
                    current_key = None
                    continue  # don't consume line yet; let main loop process it

            buffer.append(line)

        i += 1

    # Final flush
    if current_key and buffer:
        extracted[current_key] = '\n'.join(buffer).strip()
    
    # Post-processing: Add 'devices' if "all device(s)" is mentioned and devices key is missing or empty
    full_text = text.lower()


    # if "field_time" in extracted and extracted["field_time"].strip():
    if "field_time" in extracted :
        match = re.match(r'^(.*?)\n', extracted['field_time'], re.IGNORECASE) 

        if match:
            extracted['field_time'] = match.group(1)

    if "loi" in extracted:
        match = re.match(r'^(.*?)(?:\n|$)', extracted['loi'])

        if match:
            extracted['loi'] = match.group(1)


    extracted = filter_required_keys_only(extracted)

    return extracted


# industry and role : xyz - extract this type of text

# "abc industry zyx"
# def extract_fuzzy_key_value_pairs(text):

#     # Normalize all common Unicode dashes and invisible characters
#     dash_variants = ['–', '—', '−', '‑', '‒', '―', '‐', '-']  # en-dash, em-dash, minus, non-breaking, figure dash, etc.
#     for dash in dash_variants:
#         text = text.replace(dash, '-')

#     # Normalize weird dashes and invisible characters
#     text = text.replace('–', '-').replace('—', '-').replace('\xa0', ' ').replace('\u200b', '')
    
#     # Create mapping from all aliases to their main keys
#     alias_to_main_key = {}
#     for main_key, aliases in key_aliases.items():
#         for alias in aliases:
#             alias_to_main_key[alias.lower()] = main_key
    
#     # Get all aliases sorted by length (longest first) for precise matching
#     all_aliases = sorted(alias_to_main_key.keys(), key=lambda x: -len(x))
    
#     # Prepare lines
#     lines = [line.strip() for line in text.splitlines() if line.strip()]
#     extracted = {}
#     i = 0

#     while i < len(lines):
#         line = lines[i]
#         next_line = lines[i+1] if i+1 < len(lines) else None
        
#         found_main_keys = set()
#         value = None
        
#         # Check all possible patterns for each line
#         for alias in all_aliases:
#             main_key = alias_to_main_key[alias]
#             if main_key in extracted:  # Skip if we already have this key
#                 continue
                
#             # Pattern 1: Key appears anywhere before separator in same line
#             if re.search(rf'(^|\W){re.escape(alias)}\W.*[:=]\s*(.+)', line, re.IGNORECASE):
#                 match = re.search(rf'.*[:=]\s*(.+)', line)
#                 if match:
#                     value = match.group(1).strip()
#                     found_main_keys.add(main_key)
#                     break  # Found a match, move to next line
            
#             # Pattern 2: Key appears in line ending with separator
#             elif (re.search(rf'(^|\W){re.escape(alias)}\W.*[:=]\s*$', line, re.IGNORECASE) 
#                   and next_line):
#                 value = next_line.strip()
#                 found_main_keys.add(main_key)
#                 i += 1  # Skip next line
#                 break
            
#             # Pattern 3: Key appears in line, next line starts with separator
#             elif (re.search(rf'(^|\W){re.escape(alias)}\W', line, re.IGNORECASE) 
#                   and next_line and (next_line.startswith(':') or next_line.startswith('='))):
#                 value = next_line[1:].strip()
#                 found_main_keys.add(main_key)
#                 i += 1  # Skip next line
#                 break
        
#         # Store all found main keys with the value
#         for main_key in found_main_keys:
#             if value:  # Only add if we have a value
#                 extracted[main_key] = value
        
#         i += 1

#     extracted = filter_required_keys_only(extracted)

#     return extracted


# working even more fine - currently using
def extract_fuzzy_key_value_pairs(text):
    
    # Create reverse mapping from aliases to normalized keys
    alias_to_key = {}
    for norm_key, aliases in key_aliases.items():
        for alias in aliases:
            alias_to_key[alias.lower()] = norm_key
    
    # Prepare lines
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    extracted = {}
    i = 0

    while i < len(lines):
        line = lines[i]
        next_line = lines[i+1] if i+1 < len(lines) else None
        
        # Try all possible patterns for this line
        found_keys = set()
        value = None
        
        # Pattern 1: Key(s) and value in same line (key: value)
        if ':' in line or '=' in line:
            parts = re.split(r'[:=]', line, 1)
            if len(parts) == 2:
                key_part, value_part = parts
                value = value_part.strip()
                # Find all matching keys in key_part
                for alias in alias_to_key:
                    if re.search(rf'\b{re.escape(alias)}\b', key_part, re.IGNORECASE):
                        found_keys.add(alias)
        
        # Pattern 2: Key line ends with separator, value on next line
        elif next_line and (line.endswith(':') or line.endswith('=')):
            value = next_line.strip()
            # Find all matching keys in current line
            for alias in alias_to_key:
                if re.search(rf'\b{re.escape(alias)}\b', line, re.IGNORECASE):
                    found_keys.add(alias)
            if found_keys:
                i += 1  # Skip next line
        
        # Pattern 3: Key in current line, next line starts with separator
        elif next_line and (next_line.startswith(':') or next_line.startswith('=')):
            value = next_line[1:].strip()
            # Find all matching keys in current line
            for alias in alias_to_key:
                if re.search(rf'\b{re.escape(alias)}\b', line, re.IGNORECASE):
                    found_keys.add(alias)
            if found_keys:
                i += 1  # Skip next line
        
        # Store all found keys with the value
        for alias in found_keys:
            norm_key = alias_to_key[alias]
            if norm_key not in extracted and value:
                extracted[norm_key] = value
        
        i += 1

    extracted = filter_required_keys_only(extracted)

    return extracted


# working fine
# def extract_fuzzy_key_value_pairs(text):
#     # Normalize special characters
#     text = text.replace('–', '-').replace('—', '-').replace('\xa0', ' ').replace('\u200b', '')

#     alias_map = {alias.lower(): norm_key for norm_key, aliases in key_aliases.items() for alias in aliases}
#     known_keys = set(alias_map.keys())

#     lines = [line.strip() for line in text.splitlines() if line.strip()]
#     extracted = {}
#     i = 0

#     while i < len(lines):
#         line = lines[i]
#         next_line = lines[i + 1] if i + 1 < len(lines) else ""
#         matched = False

#         for alias in sorted(known_keys, key=lambda x: -len(x)):  # longest aliases first
#             alias_re = re.escape(alias)
#             normalized_key = alias_map[alias]
            
#             # Skip if we already have this key
#             if normalized_key in extracted:
#                 continue

#             # Case 1: All in one line (key: value)
#             pattern_inline = rf'^(.*\b{alias_re}\b.*?)[:=]\s*(.+)$'
#             match_inline = re.search(pattern_inline, line, flags=re.IGNORECASE)
#             if match_inline:
#                 value = match_inline.group(2).strip()
#                 if value:
#                     extracted[normalized_key] = value
#                     matched = True
#                     break

#             # Case 2: Key line ends with separator (key:), value on next line
#             pattern_key_only = rf'^(.*\b{alias_re}\b.*?)[:=]\s*$'
#             if re.search(pattern_key_only, line, flags=re.IGNORECASE) and next_line:
#                 extracted[normalized_key] = next_line.strip()
#                 matched = True
#                 i += 1  # skip next line
#                 break

#             # Case 3: Key in line, next line starts with separator (: value)
#             if re.search(rf'\b{alias_re}\b', line, flags=re.IGNORECASE):
#                 match_next = re.match(r'^[:=]\s*(.+)', next_line)
#                 if match_next:
#                     value = match_next.group(1).strip()
#                     if value:
#                         extracted[normalized_key] = value
#                         matched = True
#                         i += 1
#                         break

#         if not matched:
#             i += 1

#     extracted = filter_required_keys_only(extracted)

#     return extracted



# def extract_value_without_key(text):
#     result = {
#         "country": "",
#         "n": "",
#         "ir": "",
#         "loi": "",
#         "field_time": ""
#     }

#     text_lower = text.lower()
#     lines = text.splitlines()

#     # Extract country
#     country_lines = []
#     for line in lines:
#         for country in known_countries:
#             if country.lower() in line.lower():
#                 country_lines.append(line.strip())
#                 break
#     if country_lines:
#         result["country"] = " | ".join(country_lines)

#     # Extract n
#     n_pattern = re.compile(r'(\d+)[\s\-]*(' + '|'.join(key_aliases['n']) + r')', re.IGNORECASE)
#     n_pattern_alt = re.compile(r'(' + '|'.join(key_aliases['n']) + r')[\s\-:]*([0-9]+)', re.IGNORECASE)
#     n_match = n_pattern.search(text) or n_pattern_alt.search(text)
#     if n_match:
#         result["n"] = n_match.group(1) if n_match.group(1).isdigit() else n_match.group(2)

#     # Extract ir (look for percent)
#     ir_pattern = re.compile(r'(\d+\.?\d*)\s*%[\s\-]*(' + '|'.join(key_aliases['ir']) + r')?', re.IGNORECASE)
#     ir_match = ir_pattern.search(text)
#     if ir_match:
#         result["ir"] = ir_match.group(1)

#     # Extract loi (look for minutes or alias)
#     loi_pattern = re.compile(r'(\d+)\s*(min|minutes)', re.IGNORECASE)
#     loi_match = loi_pattern.search(text)
#     if not loi_match:
#         loi_alias_pattern = re.compile(r'(' + '|'.join(key_aliases['loi']) + r')[\s\-:]*([0-9]+)', re.IGNORECASE)
#         loi_match = loi_alias_pattern.search(text)
#         if loi_match:
#             result["loi"] = loi_match.group(2)
#     else:
#         result["loi"] = loi_match.group(1)

#     # Extract field_time (look for days or aliases)
#     field_day_pattern = re.compile(r'(\d+)\s*(day|days)', re.IGNORECASE)
#     field_time_match = field_day_pattern.search(text)
#     if not field_time_match:
#         field_time_alias = re.compile(r'(' + '|'.join(key_aliases['field_time']) + r')[\s\-:]*([0-9]+)', re.IGNORECASE)
#         field_time_match = field_time_alias.search(text)
#         if field_time_match:
#             result["field_time"] = field_time_match.group(2)
#     else:
#         result["field_time"] = field_day_pattern.search(text).group(1)

#     result = filter_required_keys_only(result)

#     return result


# currently using
def extract_value_without_key(text):

    result = {
        "market": "",
        "n": "",
        "ir": "",
        "loi": "",
        "field_time": ""
    }

    text_lower = text.lower()
    lines = text.splitlines()

    # --- Country Fix ---
    found_countries = set()
    for country in known_countries:
        # Match as whole word using word boundaries (\b)
        pattern = re.compile(r'\b' + re.escape(country) + r'\b', re.IGNORECASE)
        if pattern.search(text):
            found_countries.add(country)

    if found_countries:
        result["market"] = ", ".join(found_countries)

    # --- N ---
    n_pattern = re.compile(r'(\d+)[\s\-]*(' + '|'.join(key_aliases['n']) + r')', re.IGNORECASE)
    n_pattern_alt = re.compile(r'(' + '|'.join(key_aliases['n']) + r')[\s\-:]*([0-9]+)', re.IGNORECASE)
    n_match = n_pattern.search(text) or n_pattern_alt.search(text)
    if n_match:
        result["n"] = n_match.group(1) if n_match.group(1).isdigit() else n_match.group(2)
 
    # --- IR (Improved) ---    
    ir_aliases = key_aliases["ir"]
    ir_patterns = [
        # Case 1: "IR 20%", "IR20", "IR:20%", etc. (alias + number)
        re.compile(rf'\b(?:{"|".join(ir_aliases)})[:\-\s]*(\d+\.?\d*)\s*%?\b', re.IGNORECASE),
        # Case 2: "20% IR", "20 IR", etc. (number + alias)
        re.compile(rf'\b(\d+\.?\d*)\s*%?\s*(?:{"|".join(ir_aliases)})\b', re.IGNORECASE),
        # Case 3: "20%" (standalone percentage)
        re.compile(r'\b(\d+\.?\d*)\s*%\b', re.IGNORECASE)
    ]

    for pattern in ir_patterns:
        ir_match = pattern.search(text)
        if ir_match:
            result["ir"] = ir_match.group(1)
            break

    # --- LOI ---
    loi_pattern = re.compile(r'(\d+)\s*(min|minutes)', re.IGNORECASE)
    loi_match = loi_pattern.search(text)
    if not loi_match:
        loi_alias_pattern = re.compile(r'(' + '|'.join(key_aliases['loi']) + r')[\s\-:]*([0-9]+)', re.IGNORECASE)
        loi_match = loi_alias_pattern.search(text)
        if loi_match:
            result["loi"] = loi_match.group(2)
    else:
        result["loi"] = loi_match.group(1)

    # --- Field Time ---
    field_day_pattern = re.compile(r'(\d+)\s*(day|days)', re.IGNORECASE)
    field_time_match = field_day_pattern.search(text)
    if not field_time_match:
        field_time_alias = re.compile(r'(' + '|'.join(key_aliases['field_time']) + r')[\s\-:]*([0-9]+)', re.IGNORECASE)
        field_time_match = field_time_alias.search(text)
        if field_time_match:
            result["field_time"] = field_time_match.group(2)
    else:
        result["field_time"] = field_time_match.group(1)
    
    # Convert None values to empty string
    cleaned_dict = {key: ("" if value is None else value) for key, value in result.items()}
    result = cleaned_dict
    
    result = filter_required_keys_only(result)

    return result

# removed def normalize_broken_key_lines(text):
# removes \n just after key name. eg.)  Target\n: automobile

def html_to_text(html):
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(separator="\n").strip()    
           
    return text


def remove_single_newline_after_aliases(text):
    # Flatten all aliases to a single list and escape for regex
    all_aliases = [re.escape(alias.strip()) for aliases in key_aliases.values() for alias in aliases]
    
    # Create a pattern: alias followed by exactly one newline (but not two)
    # (?<!\n) ensures we are not inside multiple newlines
    # Note: we use a capturing group to keep the alias, and drop only the \n
    pattern = r'(?i)\b(' + '|'.join(all_aliases) + r')\b\n(?!\n)'

    # Replace alias\n with alias
    cleaned_text = re.sub(pattern, r'\1 ', text)

    return cleaned_text


def clean_text(text):

    # Normalize all common Unicode dashes and invisible characters
    dash_variants = ['–', '—', '−', '‑', '‒', '―', '‐', '-',  '–', '-', '—']  # en-dash, em-dash, minus, non-breaking, figure dash, etc.
    for dash in dash_variants:
        text = text.replace(dash, '-')

    # Normalize weird dashes and invisible characters
    text = text.replace('–', '-').replace('—', '-').replace('\xa0', ' ').replace('\u200b', '')
    text = text.replace(':=',':').replace('=',':').replace('-:', ':').replace(':-',':')

    # # Remove only single \n (not part of \n\n or more) , and brings the surrounding characters together
    # text =  re.sub(r'(?<!\n)\n(?!\n)', '', text)

    text = remove_single_newline_after_aliases(text)

    # Remove bullet characters and strip leading/trailing whitespace
    lines = text.splitlines()
    cleaned = [line.replace('•', '').lstrip() for line in lines]
    text = '\n'.join(cleaned)

    ascii_text = unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode('ascii')
    text = ascii_text

    # for c in text:
    #     print(f"{repr(c)} -> U+{ord(c):04X}")

    # print(text)

    return text


def remove_any_slash_n(text):

    # normalize multiple newlines (\n\n\n) into a single newline (\n)
    text = re.sub(r'\n+', '\n', text)
    
    # To replace all \n (newlines) with space
    text = re.sub(r'\n', ' ', text)

    return text


def extract_device_keywords(text):

    # Keywords in order of priority (longest first to avoid substring overlap)
    keywords = [ 
        "not mobile friendly",
        "no device restrictions","no device restriction", "desktop/laptop", "laptop/desktop",
        "any device", "any devices", "any devices", "all device", "all devices", "all device/s", 
        "agnostics", "agnostic", "tablets", "tablet", "mobiles", "mobile", "smartphones", "smartphone", 
        "desktops", "desktop", "laptops", "laptop"
    ]

    text_lower = text.lower()
    found = set()
    matched_spans = []

    for keyword in keywords:
        # Escape for regex and search with word boundaries where applicable
        pattern = re.escape(keyword)
        match = re.search(rf'\b{pattern}\b', text_lower)
        if match:
            # Check if the match overlaps with any previously matched phrase
            span = match.span()
            if all(span[1] <= s[0] or span[0] >= s[1] for s in matched_spans):
                found.add(keyword)
                matched_spans.append(span)

    return ' & '.join(sorted(found))


def extract_email_metadata(data):

    from_raw = data.get("from", "")
    sent_raw = data.get("sent", "")
    to_raw = data.get("to", "")
    cc_raw = data.get("cc", "")
    subject_raw = data.get("subject", "")

    subject_line = subject_raw.split('\n')[0].strip()
    client_name = ""   
    
    match = re.search(r'@([^.]+)\.com', from_raw)
    if match:
        client_name = match.group(1)

    return {
        "from" : from_raw.strip(),
        "sent" : sent_raw.strip(),
        "to" : to_raw.strip(),
        "cc" : cc_raw.strip(),
        "client_name" : client_name.strip(),        # client name is extracted from "FROM"
        "subject" : subject_line.strip()
    }

 
# sample needed -> n, estimated loi -> loi
def normalize_dict_keys(dict_list):

    alias_map = {alias.lower(): norm_key for norm_key, aliases in key_aliases.items() for alias in aliases}

    normalized_list = []

    for data in dict_list:
        normalized_dict = {}
        for key, value in data.items():
            key_lower = key.strip().lower()
            normalized_key = alias_map.get(key_lower, key)  # fallback to original key if not in aliases
            normalized_dict[normalized_key] = value.strip() if isinstance(value, str) else value

        # Optional: special handling for 'n' and 'requested_cpi' - used for tables
        if "n" in normalized_dict and isinstance(normalized_dict["n"], str) and "@" in normalized_dict["n"]:
            # match = re.match(r'([\w+]+)\s*@\s*([$€₹]?\d+(?:\.\d{1,2})?)', normalized_dict["n"])
            match = re.match(r'(.+?)\s*@\s*([$€₹]?\d+(?:\.\d{1,2})?)', normalized_dict["n"])  # also handles spaces in btw, eg.) N- 100 per country @ $10.00

            if match:
                normalized_dict["n"] = match.group(1)
                normalized_dict["requested_cpi"] = match.group(2)

        normalized_list.append(normalized_dict)

    return normalized_list


# takes a list of dictionary
def filter_required_keys_only_2(dict_list):

    required_keys = set( key_aliases.keys() )
    filtered = []

    for d in dict_list:
        clean_d = filter_required_keys_only(d)
        if clean_d:
            filtered.append(clean_d)

    return filtered


# takes a dictionary, removes empty value and unknown keys
def filter_required_keys_only(dict_name):

    required_keys = set( key_aliases.keys() )    
    clean_d = {}  

    for key, value in dict_name.items():
        normalized_key = key.lower().strip() 
        value = value.lower().strip() 

        if value == "" or value == "null" or value is None:
            continue

        if normalized_key in required_keys:
                clean_d[normalized_key] = value

    return clean_d


# ✅ Detect presence of "ir" or "loi" using alias keys - for vertical header
def contains_required_alias_keys(dict_list):
    
    valid_aliases = set(alias.strip().lower() for alias in key_aliases["ir"] + key_aliases["loi"])
    for d in dict_list:
        for k in d:
            if k.strip().lower() in valid_aliases:
                return True
    return False


def extract_table_data_from_html(raw_html):

    soup = BeautifulSoup(raw_html, "html.parser")
    tables = soup.find_all("table")

    # gives all tables after removing the colon from table text
    # tables = remove_separator_from_table(raw_html)

    all_data = []  # List to store data from the first matching table

    ir_loi_aliases = set(alias.strip().lower() for alias in key_aliases["ir"] + key_aliases["loi"])

    for table in tables:
        rows = table.find_all("tr")
        if not rows:
            continue

        # headers = [td.get_text(strip=True) for td in rows[0].find_all("td")]
        headers = [ remove_separator_from_table_header(td.get_text(strip=True)) for td in rows[0].find_all("td")]
        normalized_headers = [h.lower().strip() for h in headers]

        # ✅ Check if any alias of IR or LOI is present
        if not any(header in ir_loi_aliases for header in normalized_headers):
            continue

        # ✅ Extract data rows
        for row in rows[1:]:
            cells = row.find_all("td")
            if len(cells) != len(headers):
                continue

            row_data = {headers[i]: cells[i].get_text(strip=True) for i in range(len(headers))}
            all_data.append(row_data)

        break  # ✅ Stop after first matching table

    # ✅ Assuming normalize_dict_keys is defined elsewhere
    all_data_2 = normalize_dict_keys(all_data)        
    all_data_2 = filter_required_keys_only_2(all_data_2)

    return all_data_2


def remove_separator_from_table(raw_html):

    soup = BeautifulSoup(raw_html, "html.parser")
    tables = soup.find_all("table")

    SEPARATORS = [":", "-", ":-", '=']  # You can add more in the future

    # Build a regex pattern like "[:\-]"
    pattern = "[" + re.escape("".join(SEPARATORS)) + "]"

    for table in tables:
        for row in table.find_all("tr"):
            for cell in row.find_all(["td", "th"]):
                if cell.string:
                    cell.string = re.sub(pattern, "", cell.string).strip()
                else:
                    cell_text = cell.get_text()
                    cleaned_text = re.sub(pattern, "", cell_text).strip()
                    cell.clear()
                    cell.append(cleaned_text)

    return soup.find_all("table")


def remove_separator_from_table_header(key: str) -> str:

    SEPARATORS = [":-", ":", "-", "="]  # Keep longest first to avoid partial matches

    new_key = key
    for sep in SEPARATORS:
        new_key = new_key.replace(sep, "")
    
    return new_key.strip()

def extract_vertical_table_data(raw_html):

    soup = BeautifulSoup(raw_html, "html.parser")
    tables = soup.find_all("table")

    # gives all tables after removing the colon from table text
    # tables = remove_separator_from_table(raw_html)

    for table in tables:
        rows = table.find_all("tr")
        if not rows:
            continue

        columns = []

        for row in rows:
            cells = row.find_all("td")
            if len(cells) < 2:
                continue  # Skip rows with less than 2 columns

            key = cells[0].get_text(strip=True)
            key = remove_separator_from_table_header(key)
            values = [cell.get_text(strip=True) for cell in cells[1:]]
            # print("val",values)

            # Append values to the correct column index
            for i, val in enumerate(values):
                if len(columns) <= i:
                    columns.append({})
                columns[i][key] = val

        # ✅ Check if table contains any IR or LOI alias
        if contains_required_alias_keys(columns):
            # ✅ Normalize & filter to only required keys
            normalized_columns = normalize_dict_keys(columns)
            normalized_columns = filter_required_keys_only_2(normalized_columns)

            return normalized_columns  # ✅ Return first valid table

    return []


def extract_final_agreed_cpi(email_text):

    # Clean line breaks
    clean_text = email_text.replace('\r\n', '\n').replace('\n\n', '\n')

    # Split into lines for top-down parsing
    lines = clean_text.splitlines()

    # Patterns to look for agreement language and CPI values
    agreement_keywords = ['close at', 'close',  'approved', 'agreed', 'go down to' , 'close this at', 'try at' , 'match', 'final offer', 'offer', 'at last', 'is max', 'at max', 'is last',  'cpi', 'cpi at']
    
    for line in lines:
        lower_line = line.lower()
        if any(keyword in lower_line for keyword in agreement_keywords):

            # Try to extract a $ value from this line
            match = re.search(r'\$\d{1,3}(?:\.\d{2})?', line)
            if match:
                # print("line = ", line)
                return match.group(0)  # return first match found
            
            # Regex pattern for either $ or USD
            match = re.search(r'(?:\$|usd\s*)\d{1,3}(?:\.\d{2})?', line)
            if match:
                # print("line : ", line)
                return match.group(0)  # return first match found

    # Fallback: pick a dollar amount surrounded by whitespace or punctuation
    fallback_pattern = r'([^\w]|^)((?:\$|usd\s*)\d{1,3}(?:\.\d{2})?)((?=\W|$))'
    match = re.search(fallback_pattern, clean_text, re.IGNORECASE)
    final_cpi = ""
    if match:
        return match.group(2)  # return only the actual matched price    

    # take final cpi before the new line (if data is too long bychance)
    if final_cpi:
        match = re.match(r'^(.*?)(?:\n|$)', final_cpi)

        if match:
            final_cpi = match.group(1)  
            
    return final_cpi


column_max_lengths = {
    "conversation_id": 255,
    "receive_date": 255,

    "from_email": 255,
    "to_email": 255,
    "cc_email": 255,
    "client_name": 255,
    "subject_line": 65535,     # TEXT in MySQL (up to 64KB depending on collation)
    
    "n": 255,
    "ir": 255,
    "loi": 255,
    "market": 65535,
    "target_audience": 65535,  # TEXT
    "industries": 65535,       # TEXT
    "methodology": 65535,      # TEXT
    "feasibility": 65535,      # TEXT
    "devices": 65535,          # TEXT
    "field_time": 255,
    "requested_cpi": 255,
    "final_cpi": 255,

    "languages": 65535,
    "quotas": 65535,
    "departments": 65535,
    "number_of_open_ends": 65535,
    "field_work": 65535,
    "eligibility_criteria": 65535, 
    "survey_type": 65535,
    "survey_topic": 65535

}


def save_unique_mail_in_db( conversation_id, mail_details, extracted, final_cpi, table_data_list ):

    try:              
        # check if same conversation_id exist in table or not
        with connection.cursor() as cursor:
            cursor.execute(
                    "SELECT 1 FROM email_data WHERE conversation_id LIKE %s LIMIT 1", 
                    [f"{conversation_id}%"]
            )
            result = cursor.fetchone()

        if result: 
            return    # Skip this mail

        if final_cpi :
            final_cpi_found = True
        else:
            final_cpi_found = False
                        
        def get_value(dict_name, field):
            val = dict_name.get(field)
            if val is not None and str(val).strip() != "":
                return str(val).strip()
            return None
        
        from_email = get_value( mail_details, "from")
        sent = get_value(mail_details, "sent")
        to_email = get_value( mail_details, "to")
        cc_email = get_value( mail_details, "cc")
        client_name = get_value( mail_details, 'client_name')
        subject_line = get_value( mail_details, 'subject')
        
        receive_date = sent
        insertion_date  = datetime.now()        

        # Step 2: Fields from extracted or table_data (with correct priority)
        common_fields = [
            "n", "ir", "loi", "market", "target_audience", "industries", "methodology", "feasibility", "devices", 
            "field_time", "requested_cpi", "languages", "quotas", "departments", 
            "number_of_open_ends", "field_work", "eligibility_criteria", "survey_type", "survey_topic"
        ]

        # Step 4: Build data for DB insert
        field_names = [
            "conversation_id", "receive_date", "from_email", "to_email", "cc_email", 
            "client_name", "subject_line", "final_cpi", "final_cpi_found",  *common_fields
        ]
        
        # # combine text data and table data if there is one table with one bid detail
        if len(table_data_list) == 1:
            temp_dict = table_data_list[0]
            extracted.update(temp_dict)
            
        values = [
            conversation_id,  receive_date , from_email, to_email, cc_email, 
            client_name, subject_line, final_cpi, final_cpi_found,
            *[get_value(extracted, field) for field in common_fields]
        ]   


        with transaction.atomic():

            # if there is only text data
            if extracted :
                
                try:
                    with connection.cursor() as cursor:
                        cursor.execute(f"""
                            INSERT INTO email_data (
                                insertion_date , {', '.join(field_names)}
                            ) VALUES (
                                %s, {', '.join(['%s'] * len(field_names))}
                            )
                        """, [insertion_date] + values)
                    
                except (IntegrityError, DataError) as e:
                        if 'Data too long in MAIN bid' in str(e):
                            print("Skipping MAIN bid - main row due to data too long error:", e)

                            '''

                            # Get conversation_id from the values list
                            try:
                                conversation_id_index = field_names.index("conversation_id")
                                conversation_id_val = values[conversation_id_index]
                            except Exception:
                                conversation_id_val = "UNKNOWN"

                            print(f">>> Problem occurred for conversation_id: {conversation_id_val}")

                            for field, value in zip(field_names, values):
                                max_len = column_max_lengths.get(field)
                                if max_len and isinstance(value, str) and len(value) > max_len:
                                    print(f"Field '{field}' too long ({len(value)} > {max_len}): {value}")
                            '''
                            # continue
                        raise e

            # for table with multiple bid details
            if len(table_data_list) > 1:

                # Merge missing common fields from main extracted data into each table_data
                for i, table_data in enumerate(table_data_list):

                    for field in common_fields:
                        val = table_data.get(field)
                        if val is None or str(val).strip() == "":
                            fallback_val = extracted.get(field)
                            if fallback_val is not None and str(fallback_val).strip() != "":
                                table_data[field] = str(fallback_val).strip()


                conversation_id_counter = {}

                # insert table data into DB separately
                for table_data in table_data_list:

                    # Initialize the counter for this conversation_id
                    base_conversation_id = conversation_id

                    # Count how many times this ID has already been used
                    count = conversation_id_counter.get(base_conversation_id, 0) + 1
                    conversation_id_counter[base_conversation_id] = count

                    # Append the counter to make it unique
                    conversation_id_unique = f"{base_conversation_id}_{str(count).zfill(2)}"
                    
                    # if there is r_cpi, make it f_cpi for table - becoz table can have multiple entries with different r_cpi
                    if 'requested_cpi' in table_data:
                        final_cpi = table_data.get('requested_cpi', '')

                    field_names = [
                        "conversation_id", "receive_date", "from_email", "to_email", "cc_email", 
                        "client_name", "subject_line", "final_cpi", "final_cpi_found",  *common_fields
                    ]

                    values = [
                        conversation_id_unique, receive_date, from_email, to_email, cc_email, 
                        client_name, subject_line , final_cpi, final_cpi_found,
                        *[get_value(table_data, field) for field in common_fields]
                    ] 

                    try:
                        with connection.cursor() as cursor:
                            cursor.execute(f"""
                                INSERT INTO email_data (
                                    insertion_date , {', '.join(field_names)}
                                ) VALUES (
                                    %s, {', '.join(['%s'] * len(field_names))}
                                )
                            """, [insertion_date] + values)
                    except (IntegrityError, DataError) as e:
                        if 'Data too long in TABLE Bid' in str(e):
                            print("Skipping TABLE Bid - main row due to data too long error:", e)

                            '''

                            # Get conversation_id from the values list
                            try:
                                conversation_id_index = field_names.index("conversation_id")
                                conversation_id_val = values[conversation_id_index]
                            except Exception:
                                conversation_id_val = "UNKNOWN"

                            print(f">>> Problem occurred for conversation_id: {conversation_id_val}")

                            for field, value in zip(field_names, values):
                                max_len = column_max_lengths.get(field)
                                if max_len and isinstance(value, str) and len(value) > max_len:
                                    print(f"Field '{field}' too long ({len(value)} > {max_len}): {value}")
                            '''
                            continue
                        raise e
    
    except Exception as e:
        print("error a gya XYZ")

        # Get conversation_id from the values list
        try:
            conversation_id_index = field_names.index("conversation_id")
            conversation_id_val = values[conversation_id_index]
        except Exception:
            conversation_id_val = "UNKNOWN"

        print(f">>> Problem occurred for conversation_id: {conversation_id_val}")

        for field, value in zip(field_names, values):
            max_len = column_max_lengths.get(field)
            if max_len and isinstance(value, str) and len(value) > max_len:
                print(f"Field '{field}' too long ({len(value)} > {max_len}): {value}")
        print(e)


def handle_mail(email):

    conversation_id = email.get("conversationId", "" ) 
    html = email.get("body").get("content", "")
    raw_text = html_to_text(html)
    cleaned_text = clean_text(raw_text)

    extracted = extract_key_value_pairs(cleaned_text)
    mail_details = extract_email_metadata(extracted)

    # remove 'from', 'sent', 'subject' etc. key from dictionary, if present
    if 'from' in extracted:
        del extracted['from']
    
    if 'sent' in extracted:
        del extracted['sent']

    if 'to' in extracted:
        del extracted['to']
    
    if 'cc' in extracted:
        del extracted['cc']
    
    if 'subject' in extracted:
        del extracted['subject']

    final_cpi = extract_final_agreed_cpi(cleaned_text)
    horizontal_table_data = extract_table_data_from_html(html)
    vertical_table_data = extract_vertical_table_data(html)

    # decide which table data to save  - send horizontal or vertical
    table_data = horizontal_table_data

    if vertical_table_data == [] and len(horizontal_table_data) > 0:
        table_data = horizontal_table_data

    if horizontal_table_data == [] and  len(vertical_table_data) > 0:
        table_data = vertical_table_data
    
    # if 'devices' not in extracted and len(table_data) <= 1:
    #     extracted['devices'] = extract_device_keywords(cleaned_text)
    
    save_unique_mail_in_db(conversation_id=conversation_id, mail_details=mail_details, extracted=extracted, 
                           final_cpi=final_cpi, table_data_list = table_data )


# Class-based view for reading emails
@method_decorator(csrf_exempt, name='dispatch')
class ReadEmailAPIView(APIView):
    def get(self, request):
        
        try:
            access_token = get_access_token()

            # print(access_token)

            email_endpoint = "https://graph.microsoft.com/v1.0/me/messages"
            headers = {
                'Authorization': f'Bearer {access_token}',
                'Content-Type': 'application/json'
            }

            # format is - YYYY, MM, DD
            target_date = datetime(2025, 8, 19).date()

            # Calculate date range (00:00 to 23:59:59 of target date)
            start_datetime = datetime.combine(target_date, datetime.min.time())
            end_datetime = start_datetime + timedelta(days=1) - timedelta(seconds=1)

            # Format for API (ISO 8601 format)
            start_str = start_datetime.isoformat() + 'Z'
            end_str = end_datetime.isoformat() + 'Z'
            top = 5

            # Get or insert sync log entry
            with connection.cursor() as cursor:
                cursor.execute("SELECT next_link, status FROM email_sync_log WHERE date = %s", [target_date])
                row = cursor.fetchone()

                if row:
                    next_link, status = row
                else:
                    cursor.execute("INSERT INTO email_sync_log (date, status) VALUES (%s, 'in_progress')", [target_date])
                    next_link = None

            # Only use filter on first request
            params = None

            if not next_link:
                params = {
                    # '$filter': f"isRead eq true and receivedDateTime ge {start_str} and receivedDateTime le {end_str}",
                    '$filter': f"receivedDateTime ge {start_str} and receivedDateTime le {end_str}",
                    '$select': 'id,subject,body,toRecipients,from,receivedDateTime,ccRecipients,bccRecipients,conversationId',
                    '$top': top,
                    '$orderby': 'receivedDateTime desc'  # Newest first
                }  

             # Collect all emails
            all_emails = []
            next_url = next_link or email_endpoint

            while next_url:
                # Retry up to 5 times if status code 504
                max_retries = 5
                retries = 0
               
                while retries < max_retries:
                    response = requests.get(
                        next_url,
                        headers=headers,
                        params=params if next_url == email_endpoint else None
                    )

                    if response.status_code in [500, 504, 503]:
                        print(f"Got {response.status_code}. Retrying ({retries+1}/{max_retries})...")
                        retries += 1
                        time.sleep(2)
                        continue

                    if response.status_code == 401:
                        print("Access token expired or invalid. Refreshing token...")
                        access_token = get_access_token()
                        headers['Authorization'] = f'Bearer {access_token}'
                        retries += 1
                        time.sleep(1)
                        continue  # ✅ Try the request again with new token

                    else:
                        break  # No retry needed

                if response.status_code != 200:

                    with connection.cursor() as cursor:
                        cursor.execute("UPDATE email_sync_log SET status = 'failed' WHERE date = %s", [target_date])

                    print( "response header : ", response.headers)  # for debugging
                    return JsonResponse({"error12": response.text , "headers" :  dict(response.headers) }, status=response.status_code)

                data = response.json()
                emails = data.get("value", [])
                print(f"Fetched {len(emails)} emails in this batch : {datetime.now()}")

                for email in emails:
                    handle_mail(email)
                    
                all_emails.extend(emails)
                
                # Save new nextLink (or NULL if none)
                next_url = data.get('@odata.nextLink')

                with connection.cursor() as cursor:
                    cursor.execute("UPDATE email_sync_log SET next_link = %s, status = 'in_progress' WHERE date = %s", [next_url, target_date])
                
                params = None  # Only used on first request

            # All done, mark as complete
            with connection.cursor() as cursor:
                cursor.execute("UPDATE email_sync_log SET next_link = NULL, status = 'done' WHERE date = %s", [target_date])

            return JsonResponse({
                "no. of emails saved": len(all_emails),
                # "mails" : all_emails,
                "status": "success"
            }, status=200)

        except Exception as e:
            return JsonResponse({"error11": str(e)}, status=500)


            
@method_decorator(csrf_exempt, name='dispatch')
class GetThreadEmailsAPIView(APIView):
    def post(self, request):
        try:
            data = json.loads(request.body)
            email_id = data.get("email_id")

            if not email_id:
                return JsonResponse({"error": "Missing 'email_id' in request body."}, status=400)

            access_token = get_access_token()
            headers = {
                'Authorization': f'Bearer {access_token}',
                'Content-Type': 'application/json'
            }

            # Step 1: Fetch the selected email by ID
            email_url = f"https://graph.microsoft.com/v1.0/me/messages/{email_id}"
            email_response = requests.get(email_url, headers=headers)

            if email_response.status_code != 200:
                return JsonResponse({"error": "Email not found", "details": email_response.text}, status=email_response.status_code)

            email_data = email_response.json()

            # return JsonResponse({"email_message": email_data}, status=200)

            conversation_id = email_data.get("conversationId")

            if not conversation_id:
                return JsonResponse({"error": "No conversation ID found in the email."}, status=404)

            # Step 2: Fetch all emails in the same conversation
            thread_url = "https://graph.microsoft.com/v1.0/me/messages"
            params = {
                "$filter": f"conversationId eq '{conversation_id}'",
                "$select": "id,subject,bodyPreview,from,toRecipients,receivedDateTime"
            }

            thread_response = requests.get(thread_url, headers=headers, params=params)

            if thread_response.status_code != 200:
                return JsonResponse({"error": "Failed to retrieve conversation thread", "details": thread_response.text}, status=thread_response.status_code)

            thread_emails = thread_response.json().get("value", [])

            return JsonResponse({
                "selected_email": email_data,
                "thread_emails": thread_emails
            }, status=200)

        except Exception as e:
            return JsonResponse({"error": str(e)}, status=500)





# function returns takes the html and returns json response
def SubmitTextAPI_helper(user_text):
      
    cleaned_text = html_to_text(user_text)
    cleaned_text = clean_text(cleaned_text)

    structured_data = extract_key_value_pairs(cleaned_text)
    final_cpi = extract_final_agreed_cpi(cleaned_text)
    table_data_list = extract_table_data_from_html(user_text)
    vertical_table_data_list = extract_vertical_table_data(user_text)

    # backup function
    unstructured_data = extract_value_without_key(cleaned_text)
    fuzzy_data = extract_fuzzy_key_value_pairs(cleaned_text)

    # remove 'from', 'sent', 'subject' etc. key from dictionary, if present
    if 'from' in structured_data:
        del structured_data['from']
    
    if 'sent' in structured_data:
        del structured_data['sent']

    if 'to' in structured_data:
        del structured_data['to']
    
    if 'cc' in structured_data:
        del structured_data['cc']
    
    if 'subject' in structured_data:
        del structured_data['subject']
    
    table_data = table_data_list.copy()

    if vertical_table_data_list == [] and len(table_data_list) > 0:
        table_data = table_data_list.copy()

    if table_data_list == [] and  len(vertical_table_data_list) > 0:
        table_data = vertical_table_data_list.copy()

          
    if table_data:

        # ignore multiple rows, send first row
        final_table_data = table_data[0]

        # merge new keys from 'structured_data' to 'final_table_data'
        final_table_data = {**structured_data, **final_table_data}     


        return {
            "status": "success",
            "html_data" : final_table_data,
            "clear_data": cleaned_text
           
        }
    
    combined_data = {}

    combined_data = {**structured_data, **combined_data}    

    # for text data only 
    if combined_data == {}:
        # 'unstructured_data has 5 fields only - n,ir,loi,market,field_time'
        combined_data = {**unstructured_data, **combined_data}    

    if 'requested_cpi' not in combined_data and final_cpi:
        combined_data['requested_cpi'] = final_cpi

    # for text data - UNSTRUCTURED DATA populating STRUCTURED DATA  - start block
    if 'market' not in combined_data and 'market' in unstructured_data:
        combined_data['market'] = unstructured_data['market']
    
    if 'loi' not in combined_data and 'loi' in unstructured_data:
        combined_data['loi'] = unstructured_data['loi']

    if 'ir' not in combined_data and 'ir' in unstructured_data:
        combined_data['ir'] = unstructured_data['ir']
    
    # for text data - UNSTRUCTURED DATA populating STRUCTURED DATA  - end block
        
    if 'industries' not in combined_data and 'industries' in fuzzy_data:
        combined_data['industries'] = fuzzy_data['industries']

    if 'target_audience' not in combined_data and 'target_audience' in fuzzy_data:
        combined_data['target_audience'] = fuzzy_data['target_audience']
    
    # if 'n' not in combined_data and 'n' in fuzzy_data:
    #     combined_data['n'] = fuzzy_data['n']

    if 'loi' not in combined_data and 'loi' in fuzzy_data:
        combined_data['target_audience'] = fuzzy_data['target_audience']

    if 'ir' not in combined_data and 'ir' in fuzzy_data:
        combined_data['ir'] = fuzzy_data['ir']
    
    if 'methodology' not in combined_data and 'methodology' in fuzzy_data:
        combined_data['methodology'] = fuzzy_data['methodology']
    
    if 'quotas' not in combined_data and 'quotas' in fuzzy_data:
        combined_data['quotas'] = fuzzy_data['quotas']

    if 'field_time' not in combined_data and 'field_time' in fuzzy_data:
        combined_data['field_time'] = fuzzy_data['field_time']

    if 'feasibility' not in combined_data and 'feasibility' in fuzzy_data:
        combined_data['feasibility'] = fuzzy_data['feasibility']

    if 'survey_type' not in combined_data and 'survey_type' in fuzzy_data:
        combined_data['survey_type'] = fuzzy_data['survey_type']

    if 'survey_topic' not in combined_data and 'survey_topic' in fuzzy_data:
        combined_data['survey_topic'] = fuzzy_data['survey_topic']

    if 'devices' not in combined_data:
        combined_data['devices'] = extract_device_keywords(cleaned_text)            
  

    combined_data = filter_required_keys_only(combined_data)
   
    return {
        "status": "success",
        "html_data" : combined_data,
        "clear_data": cleaned_text
    }


