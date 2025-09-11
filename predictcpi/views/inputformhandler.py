from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.parsers import JSONParser


from django.shortcuts import render
from django.utils.decorators import method_decorator
from django.conf import settings
from django.views.decorators.csrf import csrf_exempt
from .emailreader import extract_key_value_pairs, extract_final_agreed_cpi, html_to_text, extract_table_data_from_html, extract_vertical_table_data , clean_text, extract_email_metadata, save_unique_mail_in_db, SubmitTextAPI_helper

import random
import os
import numpy as np
import pickle
import pandas as pd
import traceback
from rest_framework.test import APIRequestFactory
from .training import PredictCPI
import requests

@csrf_exempt
def input_form_view(request):
    return render(request, 'input_form.html')


API_KEY = "JFJM5jDOxm1kgDuOBY4oIAtK1VkMzKX1"
API_URL = "https://api.mistral.ai/v1/chat/completions"


headers = {
    "Authorization": f"Bearer {API_KEY}",
    "Content-Type": "application/json"
}

B2C_KEYWORDS = [
    "General Population",
    "Males and Females of any specific above criteria",
    "Primary Grocery Shoppers",
    "Household Decision Makers",
    "Those who have taken loan",
    "Entertainment Survey",
    "watch TV",
    "Movie",
    "Netflix",
    "streamers",
    "Travellers",
    "flight for business or leisure",
    "High net worth income",
    "Gamers",
    "online games",
    "Music enthusiasts",
    "Youtube",
    "Vehicle owners",
    "buy a vehicle",
    "Registered Voters",
    "Luxury Product Buyers",
    "Credit Card Users",
    "Reward Programs",
    "Smart Home Device",
    "Alexa",
    "Google Home",
    "Tech Enthusiasts",
    "New Gadgets",
    "Mobile App Users",
    "finance app",
    "health app",
    "fitness app",
    "Food Delivery App",
    "Uber Eats",
    "Parents of Young Children",
    "Pet Owners",
    "Pet Care Buyers",
    "Chronic Illness Patients",
    "Caregivers",
    "First-Time Parents",
    "pregnant women",
    "College Students",
    "University Students",
    "Gen-pop"
    "consumer",
    "Gen-pop (Consumer)",
    "Mobile phone users",
    "Parents of 18 YO",
    "Female Shoppers",
    "Male Shoppers",
    "Owners of PS5",
    "PS5 gamers",
    "Users of generative AI chatbot platforms",
    "Music streamers",
    "Spotify",
    "Apple Music",
    "Multiple language speakers",
    "Leisure activity",
    "Travellers",
    "Parents of kids",
    "Vehicle owners",
    "Intenders vehicle brand",
    "purchased vehicle brand within the next 2 years",
    "Banked individuals aged 18+",
    "House hold Decision makers",
    "House hold DMs for Insurance",
    "Homeowners",
    "Credit card holders",
    "Students",
    "Smokers",
    "Alcohol Drinkers",
    "Drinkers",
    "HH DMs for Baby food",
    "HH DMs for milk",
]


def classify_business(text: str) -> str:
    """
    Classify a company/business as B2B or B2C using Mistral API.
    Always return only 'B2B', 'B2C', or 'Unknown'.
    """
    # 🔹 Step 1: Quick check against predefined B2C list
    text_lower = text.lower()
    for keyword in B2C_KEYWORDS:
        if keyword.lower() in text_lower:
            return "B2C"  # direct return, no API call

    # 🔹 Step 2: If not matched, call API
    prompt = f"""
    You are an expert at classifying businesses.
    Given the description below, classify the business strictly as one of:
    - B2B (business-to-business)
    - B2C (business-to-consumer)

    Respond with only one word: 'B2B', 'B2C', or 'Unknown'.

    Description: {text}
    """

    payload = {
        "model": "mistral-large-latest",
        "messages": [
            {"role": "system", "content": "You are a helpful assistant that classifies businesses as B2B or B2C."},
            {"role": "user", "content": prompt}
        ],
        "temperature": 0.0,
    }

    response = requests.post(API_URL, headers=headers, json=payload)
    response.raise_for_status()
    result = response.json()
    output = result["choices"][0]["message"]["content"].strip()

    # 🔹 Step 3: Post-process to ensure valid output
    if output.upper().startswith("B2B"):
        return "B2B"
    elif output.upper().startswith("B2C"):
        return "B2C"
    else:
        return "B2B"



def classify_roles(text: str) -> dict:
    """
    Classify if a given text contains Director/VP level or C-level roles.
    Returns dict with keys: dir, clevel
    """
    text = text.lower()

    # Keywords for Director+ & VP+ Titles
    director_keywords = ["director", "vp", "vice president", "senior vice president","dir"]

    # Keywords for C-level Titles
    clevel_keywords = ["c-level", "ceo", "cfo", "coo", "cto", "cio", "cmo", "chief","clevel"]

    result = {
        "dir": "no",
        "clevel": "no"
    }

    # Check for Director+ & VP+
    if any(keyword in text for keyword in director_keywords):
        result["dir"] = "yes"

    # Check for C-level
    if any(keyword in text for keyword in clevel_keywords):
        result["clevel"] = "yes"

    return result






@method_decorator(csrf_exempt, name='dispatch')
class SubmitTextAPI(APIView):

    parser_classes = [JSONParser]

    def post(self, request):
        try:
            user_text = request.data.get('text', '')
            client_name = request.data.get('client_name', '')


            if not user_text:
                return Response({
                    "status": "fail",
                    "msg": "error1",
                    "message": "pls enter the text"
                }, status=400)


            extracted_data = SubmitTextAPI_helper(user_text)
            # print("!!!!!!!!!!",extracted_data)
            master_data=extracted_data["html_data"]

            clear_data=extracted_data["clear_data"]
            master_data["client_name"] = client_name

            audience_text = master_data.get("target_audience") or clear_data
            # business_type = classify_business(audience_text)
            # print("&&&&&&&",audience_text)
            # print("*****",business_type)
            roles = classify_roles(audience_text) 
            master_data["dir"] = roles["dir"] 
            master_data["clevel"] = roles["clevel"]

            business_type = request.data.get("business_type", "").strip()
            if not business_type:  
                business_type = classify_business(audience_text)
            master_data["business_type"]  = business_type

             # Predict CPI
            factory = APIRequestFactory()
            internal_request = factory.post('/predict-cpi/', master_data, format='json')
            response = PredictCPI.as_view()(internal_request)

        
            return Response({
                "status": "success",
                "structured_data": master_data,
                "business_type": business_type,
                "message": response.data,
            })

        except Exception as e:
            tb = traceback.format_exc()   # full traceback as string
            print(tb)                     # logs to console
            return Response({
                "status": "fail",
                "msg": "error2",
                "message": str(e),
                "traceback": tb   # optional: return it in API response
            }, status=400)
            

