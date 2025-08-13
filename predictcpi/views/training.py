
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
import pandas as pd
import joblib
import os
import re
import urllib.parse
from sqlalchemy import create_engine
import numpy as np

# # ✅ DB Credentials
# DB_USER = "root"
# DB_PASS = urllib.parse.quote_plus("Pankaj@123")
# DB_HOST = "ss"
# DB_PORT = "3306"
# DB_NAME = "mycrm_db"
# db_url = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
# engine = create_engine(db_url)

# # ✅ Load country mapping
# country_df = pd.read_sql("SELECT sortname, countries_name FROM tb_countries", engine)
# country_df['sortname'] = country_df['sortname'].str.upper()
# country_map = dict(zip(country_df['sortname'], country_df['countries_name']))
country_map = {}  # Default empty for production

# ✅ Only run this in local development
if not os.environ.get('WEBSITE_SITE_NAME'):  
    DB_USER = "root"
    DB_PASS = urllib.parse.quote_plus("Pankaj@123")
    DB_HOST = "localhost"
    DB_PORT = "3306"
    DB_NAME = "mycrm_db"

    db_url = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    engine = create_engine(db_url)

    # Load country mapping
    country_df = pd.read_sql("SELECT sortname, countries_name FROM tb_countries", engine)
    country_df['sortname'] = country_df['sortname'].str.upper()
    country_map = dict(zip(country_df['sortname'], country_df['countries_name']))

# ✅ Load models and features
with_cpi_model = joblib.load('ml/final_cpi_model_with_cpi.pkl')
with_cpi_features = joblib.load('ml/model_features_with_cpi.pkl')

without_cpi_model = joblib.load('ml/final_cpi_model_without_cpi.pkl')
without_cpi_features = joblib.load('ml/model_features_without_cpi.pkl')

# ✅ Helper functions
def clean_text(text):
    if not text:
        return ''
    return re.sub(r'\s+', ' ', re.sub(r'[^a-zA-Z0-9 ]', ' ', str(text))).lower().strip()

def extract_average(val):
    try:
        nums = [float(n) for n in re.findall(r'\d+\.?\d*', str(val))]
        return sum(nums) / len(nums) if nums else None
    except:
        return None

class PredictCPI(APIView):
    def post(self, request):
        try:
            market_code = request.data.get("market", "").upper()
            market = country_map.get(market_code, market_code)

            def get_text(field): return clean_text(request.data.get(field, ""))
            def get_avg(field): return extract_average(request.data.get(field))

            n = get_avg("n")
            ir = get_avg("ir")
            loi = get_avg("loi")
            field_time = get_avg("field_time")
            requested_cpi = get_avg("requested_cpi")
            number_of_open_ends = get_avg("number_of_open_ends")
            efficiency_score = ir / loi if ir and loi else None

            input_data = {
                'market': market,
                'n': n,
                'target_audience': get_text("target_audience"),
                'ir': ir,
                'loi': loi,
                'devices': get_text("devices"),
                'industries': get_text("industries"),
                'field_time': field_time,
                'methodology': get_text("methodology"),
                'feasibility': get_text("feasibility"),
                'company_size': get_text("company_size"),
                'languages': get_text("languages"),
                'quotas': get_text("quotas"),
                'departments': get_text("departments"),
                'number_of_open_ends': number_of_open_ends,
                'eligibility_criteria': get_text("eligibility_criteria"),
                'survey_topic': get_text("survey_topic"),
                'survey_type': get_text("survey_type"),
                'efficiency_score': efficiency_score,
            }

            use_requested_cpi = requested_cpi is not None
            if use_requested_cpi:
                input_data['requested_cpi'] = requested_cpi

            input_df = pd.DataFrame([input_data])
            input_encoded = pd.get_dummies(input_df)
            input_encoded.columns = input_encoded.columns.str.replace(r'[<>\[\]]', '_', regex=True)

            model = with_cpi_model if use_requested_cpi else without_cpi_model
            model_features = with_cpi_features if use_requested_cpi else without_cpi_features

            input_encoded = input_encoded.reindex(columns=model_features, fill_value=0)
            # predicted_log_cpi = model.predict(input_encoded)[0]
            # predicted_cpi = np.expm1(predicted_log_cpi)
            predicted_log_cpi = model.predict(input_encoded)[0]
            predicted_cpi = np.expm1(predicted_log_cpi)
            print("******",predicted_cpi)

            return Response({
                "status": "success",
                "predicted_final_cpi": "{:.2f}".format(predicted_cpi),
                "model_used": "with_requested_cpi" if use_requested_cpi else "without_requested_cpi"
            })

        except Exception as e:
            return Response({
                "status": "error",
                "message": str(e)
            }, status=status.HTTP_400_BAD_REQUEST)




