from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.parsers import JSONParser

from django.shortcuts import render
from django.utils.decorators import method_decorator
from django.views.decorators.csrf import csrf_exempt
from django.conf import settings
from django.utils.decorators import method_decorator
from django.views.decorators.csrf import csrf_exempt
from rest_framework.request import Request
from rest_framework.test import APIRequestFactory
from .training import PredictCPI
from .emailreader import extract_key_value_pairs, filter_required_keys_only, extract_final_agreed_cpi, html_to_text, extract_table_data_from_html, extract_vertical_table_data , extract_device_keywords, extract_value_without_key, extract_fuzzy_key_value_pairs, clean_text
import random

@csrf_exempt
def input_form_view(request):
    return render(request, 'input_form.html')


@method_decorator(csrf_exempt, name='dispatch')
class SubmitTextAPI(APIView):

    parser_classes = [JSONParser]

    def post(self, request):
        try:
            user_text = request.data.get('text', '')

            if not user_text:
                return Response({
                    "status": "fail",
                    "msg": "error1",
                    "message": "pls enter the text"
                }, status=400)

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

            # table_data = []
            table_data = table_data_list.copy()

            if vertical_table_data_list == [] and len(table_data_list) > 0:
                table_data = table_data_list.copy()

            if table_data_list == [] and  len(vertical_table_data_list) > 0:
                table_data = vertical_table_data_list.copy()
            
            combined_data = structured_data.copy()

            # combine text data and table data if there is only one table
            if len(table_data) == 1:
                temp_dict = table_data[0]
                combined_data.update(temp_dict)

            # take only first row of table data, exclude others
            if combined_data == {} and  len(table_data) > 0:
                combined_data = table_data[0]

            # for text data only 
            if combined_data == {}:

                if  'market' in  unstructured_data :
                    combined_data['market'] = unstructured_data['market']

                if  'n' in unstructured_data:
                    combined_data['n'] = unstructured_data['n']

                if  'loi' in unstructured_data:
                    combined_data['loi'] = unstructured_data['loi']

                if 'ir' in  unstructured_data:
                    combined_data['ir'] = unstructured_data['ir']
                
                if 'field_time' in  unstructured_data:
                    combined_data['field_time'] = unstructured_data['field_time']

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
                combined_data['target_audience'] = fuzzy_data['target_audience']
            
            if 'methodology' not in combined_data and 'methodology' in fuzzy_data:
                combined_data['target_audience'] = fuzzy_data['target_audience']
            
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

            clean_combined_data = filter_required_keys_only(combined_data)

            print("**********",combined_data)

            factory = APIRequestFactory()
            internal_request = factory.post('/predict-cpi/', clean_combined_data, format='json')
            response = PredictCPI.as_view()(internal_request)

            # return Response({
            #     "status": "success",
            #     "combined_data" : clean_combined_data,
            # })
            return Response({
                "status": "success",
                "structured_data": clean_combined_data,
                "message": response.data,
            })
        
        except Exception as e:
            return Response({
                "status": "fail",
                "msg": "error2",
                "message": str(e)
            }, status=400)
        