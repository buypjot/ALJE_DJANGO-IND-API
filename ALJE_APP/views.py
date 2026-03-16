

from django.views.decorators.http import require_GET,require_POST
from django.utils import timezone




from django.db import close_old_connections, connection


from itertools import groupby
from operator import itemgetter


from django.urls import reverse

import requests

from django.db.models import F

from django.db.models import (
    Sum, Q, Count, Case, When, IntegerField, BooleanField, OuterRef, Subquery, Value
)
import base64
from django.db.models.functions import Cast, Substr
from rest_framework.views import APIView
from rest_framework.response import Response
from .version import API_VERSION, RELEASE_DATE
from rest_framework.exceptions import NotFound
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
import json
from django.db import connection,transaction
from datetime import datetime ,timedelta
from rest_framework import status
import logging
from rest_framework.viewsets import ViewSet
from django.utils.dateparse import parse_date
from django.db.models import Q
from .models import Pickman_ScanModels, Truck_scanModels
from .serializers import Pickman_ScanModelsserializers
import traceback
from django.views.generic import View 
from django.utils.decorators import method_decorator
from django.db.models import Sum, OuterRef, Subquery, Value, IntegerField
from django.db.models.functions import Coalesce, Cast
import re
from django.shortcuts import render
import urllib.parse
from rest_framework.decorators import api_view
from rest_framework.decorators import action
from rest_framework import viewsets , status
from rest_framework.pagination import PageNumberPagination
from .serializers import *
from .models import REQNO_Models
from django.middleware.csrf import get_token
import boto3
from django.conf import settings
from rest_framework.parsers import MultiPartParser
import time
import imaplib
from email import encoders
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from django.core.mail import EmailMessage
from urllib.parse import unquote
from collections import defaultdict

def test_cors(request):
    # Accessing request method and relevant headers using request.META
    method = request.method
    origin = request.META.get('HTTP_ORIGIN', 'No Origin Header')

    return JsonResponse({
        'message': 'CORS is working!',
        'method': method,
        'origin': origin,
    })

class StandardResultsSetPagination(PageNumberPagination):
    """
    Custom pagination class to define default page size and maximum page size.
    """
    page_size = 20

    page_size_query_param = 'page_size'
    max_page_size = 100


class GenerateTokenForReqnoView(APIView):
    def get(self, request, *args, **kwargs):
        # Generate a CSRF token
        tocken = get_token(request)

        # Get the current year and month
        now = datetime.now()
        year_short = str(now.year)[-2:]  # '25' for 2025
        month = f"{now.month:02d}"       # '04' for April

        # New prefix format: DR<year><month> => 'DR2504'
        prefix = f"DR{year_short}{month}"

        # Filter existing REQ_IDs starting with the prefix
        similar_ids = REQNO_Models.objects.filter(REQ_ID__startswith=prefix).order_by('-id')

        if similar_ids.exists():
            last_req_id = similar_ids.first().REQ_ID
            # Match the number after the prefix
            match = re.match(rf"{prefix}(\d+)$", last_req_id)
            if match:
                last_number = int(match.group(1))
                next_number = last_number + 1
            else:
                next_number = 1
        else:
            next_number = 1

        # Final REQ_ID format: DR<year><month><next_number>
        next_REQ_ID = f"{prefix}{next_number:01d}"

        # Save to database
        REQNO_Models.objects.create(REQ_ID=next_REQ_ID, TOCKEN=tocken)

        return Response({
            "REQ_ID": next_REQ_ID,
            "tocken": tocken
        }, status=status.HTTP_200_OK)
    
class ReqnoView(APIView):
    def get(self, request, *args, **kwargs):
        # Get the current year and month
        now = datetime.now()
        year_short = str(now.year)[-2:]  # '25' for 2025
        month = f"{now.month:02d}"       # '04' for April

        # Prefix like REQNO_25_04_
        prefix = f"DR{year_short}{month}"

        # Default REQ_ID if no record exists
        default_req_id = f"{prefix}0"

        # Try to get the latest REQ_ID from the table
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT TOP 1 [id], [REQ_ID], [TOCKEN]
                FROM [BUYP].[dbo].[WHR_REQID_tbl]
                ORDER BY [id] DESC;
            """)
            result = cursor.fetchone()

        # Determine REQ_ID to return
        if result and result[1]:
            REQ_ID = result[1]
        else:
            REQ_ID = default_req_id

        return Response({'REQ_ID': REQ_ID})

class GenerateTokenForPickidView(APIView):    
    def get(self, request, *args, **kwargs):
        # Generate a CSRF token
        tocken = get_token(request)

        # Get current year and month
        now = datetime.now()
        year_short = str(now.year)[-2:]  # e.g., '25' for 2025
        month = f"{now.month:02d}"       # e.g., '05' for May

        # Prefix for PICK_ID, like 'PI2505'
        prefix = f"PK{year_short}{month}"

        # Query existing PICK_IDs starting with the prefix
        similar_ids = PICKID_Models.objects.filter(PICK_ID__startswith=prefix).order_by('-id')

        if similar_ids.exists():
            last_pick_id = similar_ids.first().PICK_ID
            match = re.match(rf"{prefix}(\d+)$", last_pick_id)
            if match:
                last_number = int(match.group(1))
                next_number = last_number + 1
            else:
                next_number = 1
        else:
            next_number = 1

        # Format next number with zero padding (e.g., 001, 002)
        next_number_str = f"{next_number:01d}"

        # Final PICK_ID like 'PI250501'
        next_pickid = f"{prefix}{next_number_str}"

        # Save to database
        PICKID_Models.objects.create(PICK_ID=next_pickid, TOCKEN=tocken)

        return Response({
            "PICK_ID": next_pickid,
            "TOCKEN": tocken
        }, status=status.HTTP_200_OK)

class GenerateTokenForQuickBillidView(APIView):
    def get(self, request, *args, **kwargs):
        # Generate a CSRF token
        token = get_token(request)

        # Current year and month
        now = datetime.now()
        year_short = str(now.year)[-2:]   # e.g., '25'
        month = f"{now.month:02d}"        # e.g., '08'

        # Prefix for QUICK_BILL_ID → "OKB2508"
        prefix = f"QKB{year_short}{month}"

        # Find last ID with same prefix
        similar_ids = QUICK_BILL_ID_Models.objects.filter(
            QUICK_BILL_ID__startswith=prefix
        ).order_by('-id')

        if similar_ids.exists():
            last_pick_id = similar_ids.first().QUICK_BILL_ID
            # Match trailing number after prefix
            match = re.match(rf"{prefix}(\d+)$", last_pick_id)
            if match:
                last_number = int(match.group(1))
                next_number = last_number + 1
            else:
                next_number = 1
        else:
            next_number = 1

        # ✅ No zero padding, just plain number
        next_pickid = f"{prefix}{next_number}"

        # Save to DB (ensure field names match your model)
        QUICK_BILL_ID_Models.objects.create(
            QUICK_BILL_ID=next_pickid,
            TOCKEN=token
        )

        return Response({
            "QUICK_BILL_ID": next_pickid,
            "TOKEN": token
        }, status=status.HTTP_200_OK)

class Pickid_View(APIView):
    def get(self, request, *args, **kwargs):
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT TOP 1 [id], [PICK_ID], [TOCKEN]
                FROM [BUYP].[dbo].[WHR_PICKID_tbl]
                ORDER BY [id] DESC;
            """)
            result = cursor.fetchone()
        
        if result and result[1]:
            PICK_ID = result[1]  # Existing PICK_ID
        else:
            # Generate new PICK_ID if table is empty
            now = datetime.now()
            year_short = str(now.year)[-2:]  # Last two digits of year
            month = f"{now.month:02d}"       # Zero-padded month
            prefix = f"PK{year_short}{month}"
            PICK_ID = f"{prefix}0"

        return Response({'PICK_ID': PICK_ID})

class GenerateTokenForDeliveryIdView(APIView):    
    def get(self, request, *args, **kwargs):
        # Generate a CSRF token
        tocken = get_token(request)

        # Get the current full year and month
        now = datetime.now()
        year_short = str(now.year)[-2:]  # e.g., '25' for 2025
        month = f"{now.month:02d}"      # '05' for May

        # Prefix like DL202505
        prefix = f"DL{year_short}{month}"

        # Filter and get latest matching DELIVERY_ID
        similar_ids = DELIVERYID_Models.objects.filter(DELIVERY_ID__startswith=prefix).order_by('-id')

        if similar_ids.exists():
            last_DELIVERY_ID = similar_ids.first().DELIVERY_ID
            match = re.match(rf"{prefix}(\d+)$", last_DELIVERY_ID)
            if match:
                last_number = int(match.group(1))
                next_number = last_number + 1
            else:
                next_number = 1
        else:
            next_number = 1

        # Construct the final DELIVERY_ID
        next_deliveryid = f"{prefix}{next_number:01d}"  # Format number with at least 2 digits

        # Save to the database
        DELIVERYID_Models.objects.create(DELIVERY_ID=next_deliveryid, TOCKEN=tocken)

        return Response({
            "DELIVERY_ID": next_deliveryid,
            "TOCKEN": tocken
        }, status=status.HTTP_200_OK)




@api_view(["POST", "GET"])
def create_deliverid_code(request):
    new_code = DELIVERYID_Models.objects.create()
   
    data = {
        "DELIVERY_ID": new_code.DELIVERY_ID,
        "TOCKEN": new_code.TOCKEN
    }
 
    if request.method == "POST":
        return Response(data, status=status.HTTP_201_CREATED)
   
    return Response(data, status=status.HTTP_200_OK)


from .models import ShipmentID_Models
 
@api_view(["POST", "GET"])
def create_shipment_id(request):
    new_shipment = ShipmentID_Models.objects.create()
   
    data = {
        "Shipment_Id": new_shipment.shipment_id,
        "Tocken": new_shipment.tocken
    }
 
    if request.method == "POST":
        return Response(data, status=status.HTTP_201_CREATED)  
    return Response(data, status=status.HTTP_200_OK)
 


 
class DeliveryID_view(APIView):
    """
    Generates unique DELIVERY_ID in the format: DLYYMMN
    Example: DL25101, DL25102, etc.
    Ensures the new ID is always greater than the last from both:
      - WHR_UNIQUE_DELIVERYID_tbl
      - WHR_TRUCK_SCAN_DETAILS
    """

    def _parse_numeric_value(self, value):
        """Parse DLYYMMN (or DLYYMMNNN) to numeric value for comparison"""
        try:
            if not value or len(value) < 6:
                return 0
            year = int(value[2:4])
            month = int(value[4:6])
            number = int(value[6:]) if len(value) > 6 else 0
            return (year * 1000000) + (month * 10000) + number
        except Exception:
            return 0

    def get(self, request, *args, **kwargs):
        while True:
            try:
                with transaction.atomic():
                    # Step 1: Get latest IDs from both tables
                    with connection.cursor() as cursor:
                        cursor.execute("""
                            SELECT TOP 1 [DELIVERY_ID]
                            FROM [BUYP].[dbo].[WHR_UNIQUE_DELIVERYID_tbl] WITH (UPDLOCK, HOLDLOCK)
                            ORDER BY id DESC;
                        """)
                        delivery_row = cursor.fetchone()
                        delivery_id = delivery_row[0] if delivery_row else None

                        cursor.execute("""
                            SELECT TOP 1 [DISPATCH_ID]
                            FROM [BUYP].[dbo].[WHR_TRUCK_SCAN_DETAILS] WITH (NOLOCK)
                            WHERE [DISPATCH_ID] IS NOT NULL
                            ORDER BY id DESC;
                        """)
                        dispatch_row = cursor.fetchone()
                        dispatch_id = dispatch_row[0] if dispatch_row else None

                    # Step 2: Determine the last ID to base next number on
                    delivery_val = self._parse_numeric_value(delivery_id)
                    dispatch_val = self._parse_numeric_value(dispatch_id)
                    last_id = dispatch_id if dispatch_val > delivery_val else (delivery_id or "")

                    # Step 3: Generate new DELIVERY_ID
                    now = datetime.now()
                    prefix = "DL"
                    cur_yy = now.strftime("%y")
                    cur_mm = now.strftime("%m")

                    if last_id:
                        try:
                            last_year = last_id[2:4]
                            last_month = last_id[4:6]
                            last_number = int(last_id[6:]) if len(last_id) > 6 else 0

                            if last_year == cur_yy and last_month == cur_mm:
                                next_number = last_number + 1
                            else:
                                next_number = 1
                        except Exception:
                            next_number = 1
                    else:
                        next_number = 1

                    # No zero-padding, just append the number
                    new_delivery_id = f"{prefix}{cur_yy}{cur_mm}{next_number}"

                    # Step 4: Insert only if not exists
                    with connection.cursor() as cursor:
                        cursor.execute("""
                            IF NOT EXISTS (
                                SELECT 1 FROM [BUYP].[dbo].[WHR_UNIQUE_DELIVERYID_tbl]
                                WHERE DELIVERY_ID = %s
                            )
                            INSERT INTO [BUYP].[dbo].[WHR_UNIQUE_DELIVERYID_tbl] (DELIVERY_ID, TOCKEN)
                            VALUES (%s, %s);
                        """, [new_delivery_id, new_delivery_id, next_number])

                    return Response({
                        'DELIVERY_ID': new_delivery_id,
                        'TOCKEN': next_number,
                        'STATUS': 'SUCCESS'
                    })

            except Exception as e:
                # Retry on lock/contention
                time.sleep(0.3)
                continue

# class DeliveryID_view(APIView):
#     def get(self, request, *args, **kwargs):
#         while True:  # keep retrying until success
#             try:
#                 with transaction.atomic():
#                     # Step 1: Lock table to ensure one request processes at a time
#                     with connection.cursor() as cursor:
#                         cursor.execute("""
#                                 SELECT TOP 1 [DELIVERY_ID]
#                                 FROM [BUYP].[dbo].[WHR_UNIQUE_DELIVERYID_tbl] WITH (UPDLOCK, HOLDLOCK)
#                                 ORDER BY [id] DESC;
#                         """)
#                         delivery_result = cursor.fetchone()

#                         cursor.execute("""
#                             SELECT TOP 1 [DISPATCH_ID]
#                             FROM [BUYP].[dbo].[WHR_TRUCK_SCAN_DETAILS] WITH (NOLOCK)
#                             WHERE [DISPATCH_ID] IS NOT NULL
#                             ORDER BY [id] DESC;
#                         """)
#                         dispatch_result = cursor.fetchone()

#                     # Step 2: Extract values
#                     delivery_id = delivery_result[0] if delivery_result and delivery_result[0] else None
#                     dispatch_id = dispatch_result[0] if dispatch_result and dispatch_result[0] else None

#                     # Helper to parse numeric comparison
#                     def parse_numeric_value(value):
#                         try:
#                             prefix = value[:2]
#                             year = int(value[2:4])
#                             month = int(value[4:6])
#                             number = int(value[6:])
#                             return (year * 1000000) + (month * 10000) + number
#                         except:
#                             return 0

#                     delivery_val = parse_numeric_value(delivery_id) if delivery_id else 0
#                     dispatch_val = parse_numeric_value(dispatch_id) if dispatch_id else 0
#                     last_id = dispatch_id if dispatch_val > delivery_val else (delivery_id or "")

#                     # Step 3: Generate new DELIVERY_ID
#                     if last_id:
#                         try:
#                             prefix = last_id[:2]
#                             year = last_id[2:4]
#                             month = last_id[4:6]
#                             number_part = last_id[6:]
#                             new_number = int(number_part) + 1
#                             new_delivery_id = f"{prefix}{year}{month}{new_number:03d}"
#                         except Exception:
#                             now = datetime.now()
#                             prefix = "DL"
#                             year = str(now.year)[-2:]
#                             month = f"{now.month:02d}"
#                             new_delivery_id = f"{prefix}{year}{month}001"
#                     else:
#                         now = datetime.now()
#                         prefix = "DL"
#                         year = str(now.year)[-2:]
#                         month = f"{now.month:02d}"
#                         new_delivery_id = f"{prefix}{year}{month}001"

#                     # Step 4: Extract numeric token
#                     try:
#                         tocken = int(new_delivery_id[6:])
#                     except Exception:
#                         tocken = 0

#                     # Step 5: Insert the new record safely
#                     with connection.cursor() as cursor:
#                         cursor.execute(f"""
#                             IF NOT EXISTS (
#                                 SELECT 1 FROM [BUYP].[dbo].[WHR_UNIQUE_DELIVERYID_tbl]
#                                 WHERE DELIVERY_ID = '{new_delivery_id}'
#                             )
#                             BEGIN
#                                 INSERT INTO [BUYP].[dbo].[WHR_UNIQUE_DELIVERYID_tbl] (DELIVERY_ID, TOCKEN)
#                                 VALUES ('{new_delivery_id}', {tocken});
#                             END
#                         """)

#                     # ✅ Step 6: If successful, return and break loop
#                     return Response({
#                         'DELIVERY_ID': new_delivery_id,
#                         'TOCKEN': tocken,
#                         'STATUS': 'SUCCESS'
#                     })

#             except Exception as e:
#                 # Wait a bit, then retry automatically until success
#                 time.sleep(0.3)
#                 continue
            
class GenerateTokenForReturnIdView(APIView):    
    def get(self, request, *args, **kwargs):
        # Generate a CSRF token
        token = get_token(request)

        # Get the current year and month
        now = datetime.now()
        year_short = str(now.year)[-2:]  # e.g., '25' for 2025
        month = f"{now.month:02d}"    # Two-digit month, e.g., '04'

        # Prefix like RD202504
        prefix = f"RD{year_short}{month}"

        # Filter and get latest matching RETURN_IDs
        similar_ids = RETURNID_Models.objects.filter(RETURN_ID__startswith=prefix).order_by('-id')

        if similar_ids.exists():
            last_RETURN_ID = similar_ids.first().RETURN_ID
            match = re.match(rf"{prefix}(\d+)$", last_RETURN_ID)
            if match:
                last_number = int(match.group(1))
                next_number = last_number + 1
            else:
                next_number = 1
        else:
            next_number = 1

        # Construct the final RETURN_ID with padded number (e.g., RD202504001)
        next_return_id = f"{prefix}{next_number:01d}"

        # Save to the database
        RETURNID_Models.objects.create(RETURN_ID=next_return_id, TOCKEN=token)

        return Response({
            "RETURN_ID": next_return_id,
            "TOCKEN": token
        }, status=status.HTTP_200_OK)
    
class ReturnID_view(APIView):
    def get(self, request, *args, **kwargs):
        # Execute raw SQL query to get the latest RETURN_ID
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT TOP 1 [id], [RETURN_ID], [TOCKEN]
                FROM [BUYP].[dbo].[WHR_RETURNID_tbl]
                ORDER BY [id] DESC;
            """)
            result = cursor.fetchone()

        if result and result[1]:  # Check if RETURN_ID exists
            RETURN_ID = result[1]
        else:
            # If no result, generate default RETURN_ID
            now = datetime.now()
            year_short = str(now.year)[-2:]  # '25' for 2025
            month = f"{now.month:02d}"       # '04' for April
            RETURN_ID = f"RD{year_short}{month}0"

        return Response({'RETURN_ID': RETURN_ID})
    
# class ShipmentID_view(APIView):

#     def get(self, request, *args, **kwargs):
#         # Execute raw SQL query to get the latest SHIPMENT_ID
#         with connection.cursor() as cursor:
#             cursor.execute("""
#                  SELECT TOP 1 [id], [SHIPMENT_ID], [Tocken]
#                 FROM [BUYP].[dbo].[WHR_SHIPMENT_ID]
#                 ORDER BY [id] DESC;
#             """)
#             result = cursor.fetchone()

#         if result and result[1]:  # Check if SHIPMENT_ID exists
#             SHIPMENT_ID = result[1]
#         else:
#             # If no result, generate default SHIPMENT_ID
#             now = datetime.now()
#             year_short = str(now.year)[-2:]  # '25' for 2025
#             month = f"{now.month:02d}"       # '04' for April
#             SHIPMENT_ID = f"INO{year_short}{month}0"

#         return Response({'Shipment_Id': SHIPMENT_ID})


class ShipmentID_view(APIView):
    """
    Generates unique Shipment_Id in format: INOYYMMN
    Example: INO25101, INO25102
    Ensures that the new ID is always greater than the last from both:
      - WHR_UNIQUE_SHIPMENT_ID
      - WHR_SHIMENT_DISPATCH
    """

    def _parse_numeric_value(self, value):
        """Parse INOYYMMN (or INOYYMMNNN) to numeric value for comparison"""
        try:
            if not value or len(value) < 7:
                return 0
            year = int(value[3:5])
            month = int(value[5:7])
            number = int(value[7:]) if len(value) > 7 else 0
            return (year * 1000000) + (month * 10000) + number
        except Exception:
            return 0

    def get(self, request, *args, **kwargs):
        while True:
            try:
                with transaction.atomic():
                    # Step 1: Get latest IDs
                    with connection.cursor() as cursor:
                        cursor.execute("""
                            SELECT TOP 1 [Shipment_Id]
                            FROM [BUYP].[dbo].[WHR_UNIQUE_SHIPMENT_ID] WITH (UPDLOCK, HOLDLOCK)
                            ORDER BY id DESC;
                        """)
                        delivery_row = cursor.fetchone()
                        delivery_id = delivery_row[0] if delivery_row else None

                        cursor.execute("""
                            SELECT TOP 1 [SHIPMENT_ID]
                            FROM [BUYP].[dbo].[WHR_SHIMENT_DISPATCH] WITH (NOLOCK)
                            WHERE [SHIPMENT_ID] IS NOT NULL
                            ORDER BY id DESC;
                        """)
                        dispatch_row = cursor.fetchone()
                        dispatch_id = dispatch_row[0] if dispatch_row else None

                    # Step 2: Determine the last ID to base next number on
                    delivery_val = self._parse_numeric_value(delivery_id)
                    dispatch_val = self._parse_numeric_value(dispatch_id)
                    last_id = dispatch_id if dispatch_val > delivery_val else (delivery_id or "")

                    # Step 3: Generate new Shipment ID
                    now = datetime.now()
                    prefix = "INO"
                    cur_yy = now.strftime("%y")
                    cur_mm = now.strftime("%m")

                    if last_id:
                        try:
                            last_year = last_id[3:5]
                            last_month = last_id[5:7]
                            last_number = int(last_id[7:]) if len(last_id) > 7 else 0

                            if last_year == cur_yy and last_month == cur_mm:
                                next_number = last_number + 1
                            else:
                                next_number = 1
                        except Exception:
                            next_number = 1
                    else:
                        next_number = 1

                    # Remove fixed-width formatting; just append the number
                    new_delivery_id = f"{prefix}{cur_yy}{cur_mm}{next_number}"

                    # Step 4: Insert only if not exists
                    with connection.cursor() as cursor:
                        cursor.execute("""
                            IF NOT EXISTS (
                                SELECT 1 FROM [BUYP].[dbo].[WHR_UNIQUE_SHIPMENT_ID]
                                WHERE Shipment_Id = %s
                            )
                            INSERT INTO [BUYP].[dbo].[WHR_UNIQUE_SHIPMENT_ID] (Shipment_Id, Tocken)
                            VALUES (%s, %s);
                        """, [new_delivery_id, new_delivery_id, next_number])

                    return Response({
                        'Shipment_Id': new_delivery_id,
                        'TOCKEN': next_number,
                        'STATUS': 'SUCCESS'
                    })

            except Exception as e:
                # Retry on exception (lock/contention)
                time.sleep(0.3)
                continue

class GenerateTokenForShipmentIdView(APIView):   
    def get(self, request, *args, **kwargs):
        # Generate a CSRF token
        token = get_token(request)

        # Get the current year and month
        now = datetime.now()
        year_short = str(now.year)[-2:]  # e.g., '25' for 2025
        month = f"{now.month:02d}"    # Two-digit month, e.g., '04'

        # Prefix like RD202504
        prefix = f"INO{year_short}{month}"

        # Filter and get latest matching RETURN_IDs
        similar_ids = ShipmentID_Models.objects.filter(Shipment_Id__startswith=prefix).order_by('-id')

        if similar_ids.exists():
            last_Shipment_Id = similar_ids.first().Shipment_Id
            match = re.match(rf"{prefix}(\d+)$", last_Shipment_Id)
            if match:
                last_number = int(match.group(1))
                next_number = last_number + 1
            else:
                next_number = 1
        else:
            next_number = 1

        # Construct the final RETURN_ID with padded number (e.g., RD202504001)
        next_Shipment_Id = f"{prefix}{next_number:01d}"

        # Save to the database
        ShipmentID_Models.objects.create(Shipment_Id=next_Shipment_Id, Tocken=token)

        return Response({
            "Shipment_Id": next_Shipment_Id,
            "Tocken": token
        }, status=status.HTTP_200_OK)
 
class GenerateTokenForInvoiceReturnIdView(APIView):    
    def get(self, request, *args, **kwargs):
        # Generate a CSRF token
        token = get_token(request)

        # Get the current year and month
        now = datetime.now()
        year_short = str(now.year)[-2:]  # e.g., '25' for 2025
        month = f"{now.month:02d}"    # Two-digit month, e.g., '04'

        # Prefix like RD202504
        prefix = f"INVR{year_short}{month}"

        # Filter and get latest matching INVOICE_RETURN_ID
        similar_ids = INVOICE_RETURN_ID_Models.objects.filter(INVOICE_RETURN_ID__startswith=prefix).order_by('-id')

        if similar_ids.exists():
            last_INVOICE_RETURN_ID = similar_ids.first().INVOICE_RETURN_ID
            match = re.match(rf"{prefix}(\d+)$", last_INVOICE_RETURN_ID)
            if match:
                last_number = int(match.group(1))
                next_number = last_number + 1
            else:
                next_number = 1
        else:
            next_number = 1

        # Construct the final INVOICE_RETURN_ID with padded number (e.g., RD202504001)
        next_invoice_return_id = f"{prefix}{next_number:01d}"

        # Save to the database
        INVOICE_RETURN_ID_Models.objects.create(INVOICE_RETURN_ID=next_invoice_return_id, TOCKEN=token)

        return Response({
            "INVOICE_RETURN_ID": next_invoice_return_id,
            "TOCKEN": token
        }, status=status.HTTP_200_OK)
    
class Invoice_ReturnID_view(APIView):
    def get(self, request, *args, **kwargs):
        now = datetime.now()
        year_short = str(now.year)[-2:]  # '25' for 2025
        month = f"{now.month:02d}"      # '06' for June

        prefix = f"INVR{year_short}{month}"  # e.g., 'INVR2506'

        # Default serial number
        new_serial = 1

        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT TOP 1 [INVOICE_RETURN_ID] 
                FROM [BUYP].[dbo].[INVOICE_REUTRN_ID_TBL]
                WHERE [INVOICE_RETURN_ID] LIKE %s
                ORDER BY [id] DESC;
            """, [f"{prefix}%"])
            
            result = cursor.fetchone()

            if result and result[0]:
                last_invoice_id = result[0]  # e.g., 'INVR250603'
                try:
                    # Extract and increment the serial part (last 3 digits)
                    last_serial = int(last_invoice_id[-3:])
                    new_serial = last_serial + 1
                except:
                    new_serial = 1  # fallback to 1 if parse error

        new_invoice_id = f"{prefix}{new_serial:03d}"  # e.g., 'INVR250604'
        return Response({'INVOICE_RETURN_ID': new_invoice_id})

class User_member_detailsView(viewsets.ModelViewSet):
    queryset = User_member_detailsModels.objects.all()
    serializer_class = User_member_detailserializers
    pagination_class = StandardResultsSetPagination

class User_member_UniqueIdView(viewsets.ModelViewSet):
    serializer_class = User_member_detailserializers
    pagination_class = StandardResultsSetPagination

    def get_queryset(self):
        # Get the role from the URL parameters
        role = self.kwargs.get('role', None)
        if role:
            # Filter by the provided role and order by ID descending
            return User_member_detailsModels.objects.filter(role=role).order_by('-id')
        return User_member_detailsModels.objects.none()  # Return empty queryset if no role provided

    def list(self, request, *args, **kwargs):
        queryset = self.get_queryset()
        # Get the last entry for the specified role
        last_entry = queryset.first()  # Get the first entry after ordering by descending ID
        if last_entry:
            return Response({"unique_id": last_entry.unique_id})
        return Response({"message": f"No user found with role '{kwargs.get('role')}'."}, status=404)



class GetEmployeeDetailsView(APIView):
    def get(self, request):
        emp_username = request.query_params.get('username', None)

        if not emp_username:
            return Response({"error": "username parameter is required"}, status=400)

        with connection.cursor() as cursor:
            query = """
                SELECT 
                    id,
                    PHYSICAL_WAREHOUSE,
                    ORG_ID,
                    ORG_NAME,
                    EMPLOYEE_ID,
                    EMP_NAME,
                    EMP_MAIL,
                    EMP_ROLE,
                    EMP_USERNAME,
                    CAST(EMP_PASSWORD AS VARCHAR(MAX)) AS EMP_PASSWORD,  -- Decrypt hex to text
                    CREATION_DATE,
                    CREATED_BY,
                    CREATED_IP,
                    CREATED_MAC,
                    LAST_UPDATE_DATE,
                    LAST_UPDATED_BY,
                    LAST_UPDATE_IP,
                    FLAG,
                    EMP_ACCESS_CONTROL,
                    CAST(ENCRYPTED_PASSWORD AS VARCHAR(MAX)) AS ENCRYPTED_PASSWORD
                FROM [BUYP].[dbo].[WHR_USER_MANAGEMENT]
                WHERE EMP_USERNAME = %s
            """
            cursor.execute(query, [emp_username])
            row = cursor.fetchone()

            if not row:
                return Response({"error": "User not found"}, status=404)

            columns = [col[0] for col in cursor.description]
            data = dict(zip(columns, row))

        return Response(data)

@csrf_exempt
def update_user_details(request, user_id):
    if request.method == 'PUT':
        try:
            data = json.loads(request.body)
 
            # Extract all required fields
            get_warehouse = data.get("PHYSICAL_WAREHOUSE")
            org_id = data.get("ORG_ID")
            get_org_name = data.get("ORG_NAME")
            employee_id = data.get("EMPLOYEE_ID")
            employee_name = data.get("EMP_NAME")
            employee_mail = data.get("EMP_MAIL")
            employee_role = data.get("EMP_ROLE")
            employee_username = data.get("EMP_USERNAME")
            encoded_password = data.get("EMP_PASSWORD")
            last_update_date = data.get("LAST_UPDATE_DATE", datetime.now().isoformat())
            last_updated_by = data.get("LAST_UPDATED_BY", "null")
            last_update_ip = data.get("LAST_UPDATE_IP", "null")
            flag = data.get("FLAG", "Y")
 

            # Check required fields
            required_fields = [org_id, employee_id, employee_name, employee_username, encoded_password]
            if any(field is None for field in required_fields):
                return JsonResponse({"error": "Missing required fields"}, status=400)

            # Decode base64 password
            try:
                decoded_password = base64.b64decode(encoded_password)
            except Exception as e:
                return JsonResponse({"error": f"Base64 decoding failed: {str(e)}"}, status=400)

            # Fetch warehouse & org_name from ALJE_PHYSICAL_WHR
            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT TOP 1 WAREHOUSE_NAME, REGION_NAME
                    FROM ALJE_PHYSICAL_WHR
                    WHERE ORGANIZATION_ID = %s
                """, [org_id])
                whr_row = cursor.fetchone()

            if not whr_row:
                return JsonResponse({"error": f"No warehouse/org found for ORG_ID {org_id}"}, status=404)

            warehouse, org_name = whr_row

            # Execute SQL update in WHR_USER_MANAGEMENT
            with connection.cursor() as cursor:
                cursor.execute("""
                    UPDATE WHR_USER_MANAGEMENT
                    SET PHYSICAL_WAREHOUSE = %s,
                        ORG_ID = %s,
                        ORG_NAME = %s,
                        EMPLOYEE_ID = %s,
                        EMP_NAME = %s,
                        EMP_MAIL = %s,
                        EMP_ROLE = %s,
                        EMP_USERNAME = %s,
                        EMP_PASSWORD = %s,
                        LAST_UPDATE_DATE = %s,
                        LAST_UPDATED_BY = %s,
                        LAST_UPDATE_IP = %s,
                        FLAG = %s
                    WHERE ID = %s
                """, [
                    warehouse,
                    org_id,
                    org_name,
                    employee_id,
                    employee_name,
                    employee_mail,
                    employee_role,
                    employee_username,
                    decoded_password,
                    last_update_date,
                    last_updated_by,
                    last_update_ip,
                    flag,
                    user_id
                ])

            return JsonResponse({"message": "User updated successfully"}, status=200)

        except Exception as e:
            return JsonResponse({"error": str(e)}, status=500)

    return JsonResponse({"error": "Only PUT method is allowed"}, status=405)

@csrf_exempt
def create_user_details(request):
    if request.method == 'POST':
        try:
            data = json.loads(request.body)

            # Extract required fields
            warehouse = data.get("PHYSICAL_WAREHOUSE")
            org_id = data.get("ORG_ID")
            org_name = data.get("ORG_NAME")
            employee_id = data.get("EMPLOYEE_ID")
            employee_name = data.get("EMP_NAME")
            employee_mail = data.get("EMP_MAIL")
            employee_role = data.get("EMP_ROLE")
            employee_username = data.get("EMP_USERNAME")
            encoded_password = data.get("EMP_PASSWORD")
            creation_date = data.get("CREATION_DATE", datetime.now().isoformat())
            created_by = data.get("CREATED_BY", "null")
            created_ip = data.get("CREATED_IP", "null")
            created_mac = data.get("CREATED_MAC", "null")
            last_update_date = data.get("LAST_UPDATE_DATE", datetime.now().isoformat())
            last_updated_by = data.get("LAST_UPDATED_BY", "null")
            last_update_ip = data.get("LAST_UPDATE_IP", "null")
            flag = data.get("FLAG", "Y")
            emp_access_control = data.get("EMP_ACCESS_CONTROL", "")

            # Validate required fields
            required_fields = [warehouse, org_id, org_name, employee_id, employee_name,
                               employee_username, encoded_password]
            if any(field is None for field in required_fields):
                return JsonResponse({"error": "Missing required fields"}, status=400)

            # Encode password to bytes (for VARBINARY)
            try:
                password_bytes = base64.b64decode(encoded_password)
            except Exception as e:
                return JsonResponse({"error": f"Password base64 decode error: {str(e)}"}, status=400)

            # Insert data into the table
            with connection.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO WHR_USER_MANAGEMENT (
                        PHYSICAL_WAREHOUSE, ORG_ID, ORG_NAME, EMPLOYEE_ID, EMP_NAME,
                        EMP_MAIL, EMP_ROLE, EMP_USERNAME, EMP_PASSWORD,
                        CREATION_DATE, CREATED_BY, CREATED_IP, CREATED_MAC,
                        LAST_UPDATE_DATE, LAST_UPDATED_BY, LAST_UPDATE_IP, FLAG,
                        EMP_ACCESS_CONTROL
                    )
                    VALUES (%s, %s, %s, %s, %s,
                            %s, %s, %s, %s,
                            %s, %s, %s, %s,
                            %s, %s, %s, %s,
                            %s)
                """, [
                    warehouse, org_id, org_name, employee_id, employee_name,
                    employee_mail, employee_role, employee_username, password_bytes,
                    creation_date, created_by, created_ip, created_mac,
                    last_update_date, last_updated_by, last_update_ip, flag,
                    emp_access_control
                ])

            return JsonResponse({"message": "User created successfully"}, status=201)

        except Exception as e:
            return JsonResponse({"error": str(e)}, status=500)

    return JsonResponse({"error": "Only POST method is allowed"}, status=405)

@csrf_exempt
def update_user_password(request, user_id):
    if request.method == 'PUT':
        try:
            data = json.loads(request.body)

            # Get base64 password string from request
            encoded_password = data.get("EMP_PASSWORD", "")
            if not encoded_password:
                return JsonResponse({"error": "EMP_PASSWORD is required"}, status=400)

            # Decode base64 to binary
            try:
                decoded_password = base64.b64decode(encoded_password)
            except Exception as e:
                return JsonResponse({"error": f"Base64 decoding failed: {str(e)}"}, status=400)

            # Execute raw SQL update
            with connection.cursor() as cursor:
                cursor.execute("""
                    UPDATE WHR_USER_MANAGEMENT
                    SET EMP_PASSWORD = %s
                    WHERE ID = %s
                """, [decoded_password, user_id])

            return JsonResponse({"message": "Password updated successfully"}, status=200)

        except Exception as e:
            return JsonResponse({"error": str(e)}, status=500)
    else:
        return JsonResponse({"error": "Only PUT method is allowed"}, status=405)

class Physical_WarehouseView(viewsets.ModelViewSet):
    queryset = Physical_WarehouseModels.objects.all()
    serializer_class = Physical_Warehouseserializers
    pagination_class = StandardResultsSetPagination
  
class UndeliveredDataView(viewsets.ViewSet):
    """
    A ViewSet for listing undelivered data using a custom SQL query.
    """
    pagination_class = StandardResultsSetPagination

    def list(self, request):
        # Get the salesman_no parameter from the request
        salesman_no = request.query_params.get('salesman_no', None)

        # Prepare the SQL query
        query = '''
            SELECT * FROM BUYP.BUYP.XXALJE_UNDELIVERED_DATA_BUYP1
        '''

        # Add a WHERE clause if salesman_no is provided
        if salesman_no:
            query += ' WHERE SALESMAN_NO = %s'
        
        # Execute raw SQL query
        with connection.cursor() as cursor:
            if salesman_no:
                cursor.execute(query, [salesman_no])  # Use parameterized query for safety
            else:
                cursor.execute(query)
                
            rows = cursor.fetchall()

        # Prepare the data for serialization
        data = [
            {
                'undel_id': row[0],
                'to_warehouse': row[1],
                'org_id': row[2],
                'org_name': row[3],
                'salesrep_id': row[4],
                'salesman_no': row[5],
                'salesman_name': row[6],
                'customer_id': row[7],
                'customer_number': row[8],
                'customer_name': row[9],
                'sales_channel': row[10],
                'customer_site_id': row[11],
                'cus_location': row[12],
                'customer_trx_id': row[13],
                'customer_trx_line_id': row[14],
                'invoice_date': row[15],
                'invoice_number': row[16],
                'line_number': row[17],
                'inventory_item_id': row[18],
                'quantity': row[19],
                'dispatch_qty': row[20],
                'amount': row[21],
                'item_cost': row[22],
                'flag': row[23],
                'reference1': row[24],
                'reference2': row[25],
                'attribute1': row[26],
                'attribute2': row[27],
                'attribute3': row[28],
                'attribute4': row[29],
                'attribute5': row[30],
                'freeze_status': row[31],
                'last_update_date': row[32],
                'last_updated_by': row[33],
                'creation_date': row[34],
                'created_by': row[35],
                'last_update_login': row[36],
                'warehouse_id': row[37],
                'warehouse_name': row[38],
                'legacy_ref': row[39],
                'inv_row_id': row[40]
            } for row in rows
        ]

        # Paginate the data
        paginator = StandardResultsSetPagination()
        paginated_data = paginator.paginate_queryset(data, request)

        # Serialize the paginated data
        serializer = UndeliveredDataSerializer(paginated_data, many=True)

        # Return the paginated response
        return paginator.get_paginated_response(serializer.data)

class SalesmandetailsView(viewsets.ViewSet):
    """
    A ViewSet for listing distinct salesman data.
    """
    pagination_class = StandardResultsSetPagination

    def list(self, request):
        # Prepare the SQL query to get distinct salesman data
        query = '''
            SELECT DISTINCT SALESMAN_NO, SALESREP_ID, SALESMAN_NAME
            FROM BUYP.XXALJE_UNDELIVERED_DATA_BUYP1
        '''

        # Execute raw SQL query
        with connection.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()

        # Prepare the data for serialization
        data = [
            {
                'salesman_no': row[0],
                'salesrep_id': row[1],
                'salesman_name': row[2],
            } for row in rows
        ]

        # Paginate the data
        paginator = StandardResultsSetPagination()
        paginated_data = paginator.paginate_queryset(data, request)

        # Serialize the paginated data
        serializer = SalesmanDataSerializer(paginated_data, many=True)  # Use correct serializer

        # Return the paginated response
        return paginator.get_paginated_response(serializer.data)
    
class loginsalesmanwarehousedetailsView(viewsets.ViewSet):
    """
    A ViewSet for listing undelivered data using a custom SQL query.
    """
    pagination_class = StandardResultsSetPagination

    def list(self, request):
        # Get the salesman_no parameter from the request
        salesman_no = request.query_params.get('salesman_no', None)

        # Prepare the base SQL query, selecting only the necessary fields
        query = '''
            SELECT DISTINCT 
                SALESREP_ID,
                SALESMAN_NO,
                SALESMAN_NAME,
                SALES_CHANNEL,
                TO_WAREHOUSE, 
                ORG_ID,
                ORG_NAME,
                CUSTOMER_ID, 
                CUSTOMER_NUMBER, 
                CUSTOMER_NAME ,CUSTOMER_SITE_ID,SALES_CHANNEL
            FROM BUYP.BUYP.XXALJE_UNDELIVERED_DATA_BUYP1
        '''

        # Add a WHERE clause if salesman_no is provided
        if salesman_no:
            query += ' WHERE SALESMAN_NO = %s'

        # Execute the raw SQL query
        with connection.cursor() as cursor:
            if salesman_no:
                cursor.execute(query, [salesman_no])  # Use parameterized query for safety
            else:
                cursor.execute(query)
                
            rows = cursor.fetchall()

        # Prepare the data for serialization
        data = [
            { 
                'salesrep_id': row[0],
                'salesman_no': row[1],
                'salesman_name': row[2],                
                'salesman_channel': row[3],
                'to_warehouse': row[4],
                'org_id': row[5],
                'org_name': row[6],

                'customer_id': row[7],
                'customer_number': row[8],
                'customer_name': row[9],                
                'customer_site_id': row[10],              
                'customer_site_channel': row[11],
            } for row in rows
        ]

        # Paginate the data
        paginator = StandardResultsSetPagination()
        paginated_data = paginator.paginate_queryset(data, request)

        # Serialize the paginated data
        serializer = loginsalesmanwarehousedetailsSerializer(paginated_data, many=True)

        # Return the paginated response
        return paginator.get_paginated_response(serializer.data)

# class SalesInvoiceDetailsView(APIView):
#     """
#     A View for listing invoice details filtered by ORG_ID, SALESMAN_NO, CUSTOMER_NUMBER, and CUSTOMER_SITE_ID.
#     Only invoices where buyp_total_quantity != dispatch_total_quantity are shown.
#     """
#     pagination_class = StandardResultsSetPagination

#     def get(self, request, salesmanno, cusnumber, cussiteid):
#         query = '''
#             SELECT 
#                 u.SALESMAN_NO, 
#                 u.CUSTOMER_NUMBER, 
#                 u.INVOICE_NUMBER, 
#                 u.CUSTOMER_SITE_ID,
#                 u.CUSTOMER_TRX_ID,

#                 m.ORA_INVOICE_NO,
#                 m.ORA_INVOICE_DATE,
#                 m.RP_CUSTOMER_NAME,
#                 m.RP_MOBILE_NO,
#                 m.RP_INVOICE_NO,

#                 SUM(u.DISPATCH_QTY) AS DISPATCH_QTY,
#                 SUM(u.QUANTITY) - ISNULL(SUM(u.RETURN_QTY), 0) AS BUYP_TOTAL_QUANTITY,
#                 SUM(ISNULL(u.DISPATCH_QTY, 0) + ISNULL(d.TRUCK_SCAN_QTY, 0)) AS DISPATCH_TOTAL_QUANTITY

#             FROM 
#                 BUYP.[XXALJE_UNDELIVERED_DATA_BUYP1] u

#             LEFT JOIN 
#                 [WHR_CREATE_DISPATCH] d
#                 ON u.SALESMAN_NO = d.SALESMAN_NO
#                 AND u.CUSTOMER_NUMBER = d.CUSTOMER_NUMBER
#                 AND u.INVOICE_NUMBER = d.INVOICE_NUMBER
#                 AND u.CUSTOMER_SITE_ID = d.CUSTOMER_SITE_ID

#             LEFT JOIN 
#                 [WHR_RETURN_DISPATCH] r
#                 ON u.SALESMAN_NO = r.SALESMAN_NO
#                 AND u.CUSTOMER_NUMBER = r.CUSTOMER_NUMBER
#                 AND u.INVOICE_NUMBER = r.INVOICE_NO
#                 AND u.CUSTOMER_SITE_ID = r.CUSTOMER_SITE_ID

#             LEFT JOIN 
#                 [BUYP].[dbo].[XXALJEBYP_RETAIL_SALES_MST] m
#                 ON u.CUSTOMER_TRX_ID = m.CUSTOMER_TRX_ID

#             WHERE 
#                 u.SALESMAN_NO = %s
#                 AND u.CUSTOMER_NUMBER = %s
#                 AND u.CUSTOMER_SITE_ID = %s
#                 AND (u.QUANTITY - ISNULL(u.RETURN_QTY, 0)) >= u.DISPATCH_QTY
#                 AND (u.QUANTITY - ISNULL(u.RETURN_QTY, 0)) != 0

#             GROUP BY 
#                 u.SALESMAN_NO, 
#                 u.CUSTOMER_NUMBER, 
#                 u.INVOICE_NUMBER, 
#                 u.CUSTOMER_SITE_ID,
#                 u.CUSTOMER_TRX_ID,
#                 m.ORA_INVOICE_NO,
#                 m.ORA_INVOICE_DATE,
#                 m.RP_CUSTOMER_NAME,
#                 m.RP_MOBILE_NO,
#                 m.RP_INVOICE_NO

#             HAVING 
#                 (SUM(u.QUANTITY) - ISNULL(SUM(u.RETURN_QTY), 0)) > 
#                 SUM(ISNULL(u.DISPATCH_QTY, 0) + ISNULL(d.TRUCK_SCAN_QTY, 0))
#         '''

#         try:
#             with connection.cursor() as cursor:
#                 cursor.execute(query, [salesmanno, cusnumber, cussiteid])
#                 rows = cursor.fetchall()

#             # Map correct indices
#             filtered_data = []
#             for row in rows:
#                 # Indexes based on SELECT fields
#                 buyp_total_quantity = row[11]  # BUYP_TOTAL_QUANTITY
#                 dispatch_total_quantity = row[12]  # DISPATCH_TOTAL_QUANTITY

#                 if buyp_total_quantity != dispatch_total_quantity:
#                     filtered_data.append({
#                         'salesman_no': row[0],
#                         'customer_number': row[1],
#                         'invoice_number': row[2],
#                         'customer_site_id': row[3],
#                         'customer_trx_id': int(row[4]),

#                         'ora_invoice_no': row[5],
#                         'ora_invoice_date': row[6],
#                         'rp_customer_name': row[7],
#                         'rp_mobile_no': row[8],
#                         'rp_invoice_no': row[9],

#                         'buyp_total_quantity': float(row[10]) if row[10] is not None else 0.0,
#                         'dispatch_total_quantity': float(buyp_total_quantity) if buyp_total_quantity is not None else 0.0,
#                         # 'dispatch_total_quantity': float(dispatch_total_quantity) if dispatch_total_quantity is not None else 0.0,
#                     })

#             # Paginate
#             paginator = self.pagination_class()
#             paginated_data = paginator.paginate_queryset(filtered_data, request, view=self)

#             return paginator.get_paginated_response(paginated_data)

#         except Exception as e:
#             return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class SalesInvoiceDetailsView(APIView):
    """
    A View for listing invoice details filtered by SALESMAN_NO, CUSTOMER_NUMBER, and CUSTOMER_SITE_ID.
    Only invoices where buyp_total_quantity != dispatch_total_quantity are shown.
    """
    pagination_class = StandardResultsSetPagination

    def get(self, request, salesmanno, cusnumber, cussiteid):
        query = '''
            ;WITH u_agg AS (
                SELECT
                    SALESMAN_NO,
                    CUSTOMER_NUMBER,
                    INVOICE_NUMBER,
                    CUSTOMER_SITE_ID,
                    CUSTOMER_TRX_ID,
                    SUM(ISNULL(DISPATCH_QTY,0))    AS SUM_DISPATCH_QTY,
                    SUM(ISNULL(QUANTITY,0))        AS SUM_QUANTITY,
                    SUM(ISNULL(RETURN_QTY,0))      AS SUM_RETURN_QTY
                FROM BUYP.[XXALJE_UNDELIVERED_DATA_BUYP1] u
                WHERE
                    u.SALESMAN_NO = %s
                    AND u.CUSTOMER_NUMBER = %s
                    AND u.CUSTOMER_SITE_ID = %s
                    AND (u.QUANTITY - ISNULL(u.RETURN_QTY, 0)) >= u.DISPATCH_QTY
                    AND (u.QUANTITY - ISNULL(u.RETURN_QTY, 0)) <> 0
                GROUP BY
                    SALESMAN_NO, CUSTOMER_NUMBER, INVOICE_NUMBER, CUSTOMER_SITE_ID, CUSTOMER_TRX_ID
            ),
            d_agg AS (
                SELECT
                    SALESMAN_NO,
                    CUSTOMER_NUMBER,
                    INVOICE_NUMBER,
                    CUSTOMER_SITE_ID,
                    SUM(ISNULL(TRUCK_SCAN_QTY,0)) AS SUM_TRUCK_SCAN_QTY
                FROM [WHR_CREATE_DISPATCH]
                GROUP BY SALESMAN_NO, CUSTOMER_NUMBER, INVOICE_NUMBER, CUSTOMER_SITE_ID
            )
            SELECT
                u.SALESMAN_NO,
                u.CUSTOMER_NUMBER,
                u.INVOICE_NUMBER,
                u.CUSTOMER_SITE_ID,
                u.CUSTOMER_TRX_ID,
                m.ORA_INVOICE_NO,
                m.ORA_INVOICE_DATE,
                m.RP_CUSTOMER_NAME,
                m.RP_MOBILE_NO,
                m.RP_INVOICE_NO,

                u.SUM_DISPATCH_QTY         AS DISPATCH_QTY,
                (u.SUM_QUANTITY - ISNULL(u.SUM_RETURN_QTY,0)) AS BUYP_TOTAL_QUANTITY,
                (u.SUM_DISPATCH_QTY + ISNULL(d.SUM_TRUCK_SCAN_QTY,0)) AS DISPATCH_TOTAL_QUANTITY

            FROM u_agg u
            LEFT JOIN d_agg d
                ON u.SALESMAN_NO = d.SALESMAN_NO
            AND u.CUSTOMER_NUMBER = d.CUSTOMER_NUMBER
            AND u.INVOICE_NUMBER = d.INVOICE_NUMBER
            AND u.CUSTOMER_SITE_ID = d.CUSTOMER_SITE_ID
            LEFT JOIN BUYP.dbo.XXALJEBYP_RETAIL_SALES_MST m
                ON u.CUSTOMER_TRX_ID = m.CUSTOMER_TRX_ID
            WHERE
                (u.SUM_QUANTITY - ISNULL(u.SUM_RETURN_QTY,0))
                >
                (u.SUM_DISPATCH_QTY + ISNULL(d.SUM_TRUCK_SCAN_QTY,0));
            '''


        try:
            with connection.cursor() as cursor:
                cursor.execute(query, [salesmanno, cusnumber, cussiteid])
                rows = cursor.fetchall()

            filtered_data = []
            for row in rows:
                buyp_total_quantity = row[11]   # BUYP_TOTAL_QUANTITY
                dispatch_total_quantity = row[12]  # DISPATCH_TOTAL_QUANTITY

                filtered_data.append({
                    'salesman_no': row[0],
                    'customer_number': row[1],
                    'invoice_number': row[2],
                    'customer_site_id': row[3],
                    'customer_trx_id': int(row[4]) if row[4] else None,

                    'ora_invoice_no': row[5],
                    'ora_invoice_date': row[6],
                    'rp_customer_name': row[7],
                    'rp_mobile_no': row[8],
                    'rp_invoice_no': row[9],

                    'dispatch_qty': float(row[10]) if row[10] is not None else 0.0,
                    'buyp_total_quantity': float(buyp_total_quantity) if buyp_total_quantity is not None else 0.0,
                    'dispatch_total_quantity': float(dispatch_total_quantity) if dispatch_total_quantity is not None else 0.0,
                })

            paginator = self.pagination_class()
            paginated_data = paginator.paginate_queryset(filtered_data, request, view=self)
            return paginator.get_paginated_response(paginated_data)

        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)



# class InvoiceDetailsView(viewsets.ViewSet):
#     """
#     A ViewSet for listing invoice details filtered by salesman_no, customer_number, and invoice_number,
#     excluding rows where FLAG_STATUS = 'IC', and showing quantity as QUANTITY - RETURN_QTY,
#     excluding rows where the resulting quantity is less than or equal to 0.
#     """
#     pagination_class = StandardResultsSetPagination

#     def list(self, request):
#         salesman_no = request.query_params.get('salesman_no')
#         customer_number = request.query_params.get('customer_number')
#         invoice_number = request.query_params.get('invoice_number')

#         # Dynamically building WHERE clause for the CTE
#         where_clauses = ["(A.FLAG_STATUS IS NULL OR A.FLAG_STATUS = '' OR A.FLAG_STATUS != 'IC')",
#                          "(A.QUANTITY - ISNULL(A.RETURN_QTY, 0)) > 0"]
#         params = []

#         if salesman_no:
#             where_clauses.append("A.SALESMAN_NO = %s")
#             params.append(salesman_no)
#         if customer_number:
#             where_clauses.append("A.CUSTOMER_NUMBER = %s")
#             params.append(customer_number)
#         if invoice_number:
#             where_clauses.append("A.INVOICE_NUMBER = %s")
#             params.append(invoice_number)

#         where_sql = "WHERE " + " AND ".join(where_clauses)

#         query = f'''       
#             WITH UndeliveredData AS (
#                 SELECT DISTINCT 
#                     A.UNDEL_ID, 
#                     A.INVOICE_NUMBER, 
#                     A.CUSTOMER_TRX_ID, 
#                     A.CUSTOMER_TRX_LINE_ID, 
#                     A.LINE_NUMBER, 
#                     CAST(A.INVENTORY_ITEM_ID AS VARCHAR) AS INVENTORY_ITEM_ID, 
#                     (A.QUANTITY - ISNULL(A.RETURN_QTY, 0)) AS CALCULATED_QTY,

#                     (
#                         ISNULL(A.DISPATCH_QTY, 0) + 
#                         ISNULL((
#                             SELECT SUM(D.TRUCK_SCAN_QTY)
#                             FROM [BUYP].[dbo].[WHR_CREATE_DISPATCH] AS D
#                             WHERE D.SALESMAN_NO = A.SALESMAN_NO
#                             AND D.CUSTOMER_NUMBER = A.CUSTOMER_NUMBER
#                             AND D.INVOICE_NUMBER = A.INVOICE_NUMBER
#                             AND B.ITEM_CODE = CAST(D.INVENTORY_ITEM_ID AS VARCHAR)
#                             AND B.DESCRIPTION = D.ITEM_DESCRIPTION
#                         ), 0)
#                     ) AS DISPATCH_QTY,

#                     A.AMOUNT, 
#                     A.ITEM_COST, 
#                     A.FLAG_STATUS,
#                     B.DESCRIPTION, 
#                     B.ITEM_CODE
#                 FROM [BUYP].[XXALJE_UNDELIVERED_DATA_BUYP1] AS A
#                 LEFT JOIN [BUYP].[ALJE_ITEM_CATEGORIES_CPD_V] AS B
#                     ON CAST(A.INVENTORY_ITEM_ID AS VARCHAR) = CAST(B.INVENTORY_ITEM_ID AS VARCHAR)
#                 {where_sql}
#             )
#             SELECT *
#             FROM UndeliveredData
#             WHERE CALCULATED_QTY <> DISPATCH_QTY;
            
#         '''

#         try:
#             with connection.cursor() as cursor:
#                 cursor.execute(query, params)
#                 rows = cursor.fetchall()

#             data = [
#                 {
#                     'undel_id': row[0],
#                     'invoice_number': row[1],
#                     'customer_trx_id': row[2],
#                     'customer_trx_line_id': row[3],
#                     'line_number': row[4],
#                     'inventory_item_id': row[5],
#                     'quantity': row[6],
#                     'dispatched_qty': row[7],
#                     'description': row[11],
#                     'item_code': row[12],
#                 }
#                 for row in rows
#             ]

#             paginator = StandardResultsSetPagination()
#             paginated_data = paginator.paginate_queryset(data, request)
#             serializer = invoicedetailsSerializer(paginated_data, many=True)

#             return paginator.get_paginated_response(serializer.data)

#         except Exception as e:
#             return Response({'error': str(e)}, status=400)


class InvoiceDetailsView(viewsets.ViewSet):
    """
    ViewSet to list invoice details filtered by salesman_no, customer_number, and invoice_number.
    If the invoice is blocked (exists in XXALJE_BUYP_BLOCKED_INVOICES), returns a message instead of data.
    """
    pagination_class = StandardResultsSetPagination

    def list(self, request):
        salesman_no = request.query_params.get('salesman_no')
        customer_number = request.query_params.get('customer_number')
        invoice_number = request.query_params.get('invoice_number')

        # Validate required params
        if not all([salesman_no, customer_number, invoice_number]):
            return Response(
                {"error": "salesman_no, customer_number, and invoice_number are required parameters."},
                status=400
            )

        try:
            with connection.cursor() as cursor:
                # Step 1: Check if invoice is blocked
                check_query = """
                    SELECT 1
                    FROM [BUYP].[dbo].[XXALJE_BUYP_BLOCKED_INVOICES]
                    WHERE CUSTOMER_NUMBER = %s AND TRX_NUMBER = %s
                """
                cursor.execute(check_query, [customer_number, invoice_number])
                blocked = cursor.fetchone()

                if blocked:
                    # Invoice is blocked — return simple message
                    return Response({
                        "Message": "This invoice is blocked",
                        "Customer_Number": customer_number,
                        "Invoice_Number": invoice_number
                    })

                # Step 2: If not blocked, run the main query
                main_query = """
                    WITH UndeliveredData AS (
                SELECT DISTINCT 
                    A.UNDEL_ID, 
                    A.INVOICE_NUMBER, 
                    A.CUSTOMER_TRX_ID, 
                    A.CUSTOMER_TRX_LINE_ID, 
                    A.LINE_NUMBER, 
                    A.INVENTORY_ITEM_ID, 
                   (A.QUANTITY - ISNULL(A.RETURN_QTY, 0)) AS CALCULATED_QTY,

                    (
                        ISNULL(A.DISPATCH_QTY, 0) + 
                        ISNULL((
                            SELECT SUM(D.TRUCK_SCAN_QTY)
                            FROM [BUYP].[dbo].[WHR_CREATE_DISPATCH] AS D
                            WHERE D.SALESMAN_NO = A.SALESMAN_NO
                            AND D.CUSTOMER_NUMBER = A.CUSTOMER_NUMBER
                            AND D.INVOICE_NUMBER = A.INVOICE_NUMBER
                            AND B.ITEM_CODE = CAST(D.INVENTORY_ITEM_ID AS VARCHAR)
                            AND B.DESCRIPTION = D.ITEM_DESCRIPTION
                        ), 0)
                    ) AS DISPATCH_QTY,

                    A.AMOUNT, 
                    A.ITEM_COST, 
                    A.FLAG_STATUS,
                    B.DESCRIPTION, 
                    B.ITEM_CODE
                FROM [BUYP].[XXALJE_UNDELIVERED_DATA_BUYP1] AS A
                LEFT JOIN [BUYP].[ALJE_ITEM_CATEGORIES_CPD_V] AS B
                    ON A.INVENTORY_ITEM_ID = B.INVENTORY_ITEM_ID 

                        WHERE A.SALESMAN_NO = %s
                          AND A.CUSTOMER_NUMBER = %s
                          AND A.INVOICE_NUMBER = %s
                    )
                    SELECT *
                    FROM UndeliveredData
                    WHERE CALCULATED_QTY <> DISPATCH_QTY;
                """

                cursor.execute(main_query, [salesman_no, customer_number, invoice_number])
                rows = cursor.fetchall()

            # Handle case when no rows found
            if not rows:
                return Response({"Message": "No undelivered data found for this invoice."})

            # Convert query results into dictionary
            data = [
                {
                    'undel_id': row[0],
                    'invoice_number': row[1],
                    'customer_trx_id': row[2],
                    'customer_trx_line_id': row[3],
                    'line_number': row[4],
                    'inventory_item_id': row[5],
                    'quantity': row[6],
                    'dispatched_qty': row[7],
                    'amount': row[8],
                    'item_cost': row[9],
                    'flag_status': row[10],
                    'description': row[11],
                    'item_code': row[12],
                }
                for row in rows
            ]

            # Apply pagination
            paginator = StandardResultsSetPagination()
            paginated_data = paginator.paginate_queryset(data, request)

            return paginator.get_paginated_response(paginated_data)

        except Exception as e:
            return Response({'error': str(e)}, status=500)

class Salesman_ListView(viewsets.ModelViewSet):
    queryset = Salesman_List.objects.all()
    serializer_class = Salesman_Listserializers
    pagination_class = StandardResultsSetPagination

class EmployeeView(viewsets.ViewSet):
    pagination_class = StandardResultsSetPagination  # Use your pagination class


    def list(self, request):
        # Raw SQL query to fetch employee data
        query = """
            SELECT
                [PERSON_ID],
                [EMAIL_ADDRESS],
                [EMPLOYEE_NUMBER],
                [FULL_NAME],
                [EFFECTIVE_START],
                [EFFECTIVE_END_DATE],
                [INTERNAL_LOCATION],
                [MAILSTOP]
            FROM [dbo].[ALJE_EMPLOYEE_BUYP]
        """
       
        with connection.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()  # Fetch all the results
           
            # Get column names
            columns = [column[0] for column in cursor.description]
       
        # Convert the result to a list of dictionaries
        result_dicts = [dict(zip(columns, row)) for row in result]
       
        # Apply pagination manually
        paginator = self.pagination_class()
        paginated_result = paginator.paginate_queryset(result_dicts, request)
       
        # Return paginated response
        if paginated_result is not None:
            return paginator.get_paginated_response(paginated_result)
       
        return Response(result_dicts)
   
    @action(detail=False, methods=['get'])
    def get_employee(self, request):
        # Get employee_number from query parameters
        employee_number = request.query_params.get('employee_number', None)
       
        if not employee_number:
            return Response({'error': 'Employee number is required'}, status=status.HTTP_400_BAD_REQUEST)


        # Raw SQL query to fetch employee details
        query = """
            SELECT TOP (1)
                [PERSON_ID],
                [EMAIL_ADDRESS],
                [EMPLOYEE_NUMBER],
                [FULL_NAME]
            FROM [dbo].[ALJE_EMPLOYEE_BUYP]
            WHERE [EMPLOYEE_NUMBER] = %s
        """
       
        with connection.cursor() as cursor:
            cursor.execute(query, [employee_number])
            result = cursor.fetchone()
           
            if result:
                columns = ['PERSON_ID', 'EMAIL_ADDRESS', 'EMPLOYEE_NUMBER', 'FULL_NAME']
                result_dict = dict(zip(columns, result))
                return Response(result_dict)
            else:
                return Response({'error': 'Employee not found'}, status=status.HTTP_404_NOT_FOUND)
    
class SalesmanValidEmployee(APIView):
    """
    View to fetch ORG_ID for a given SALESMAN_NO from the MSSQL database.
    """

    def get(self, request, employeeno):
        try:
            # Define the raw SQL query
            query = """
                SELECT DISTINCT u.ORG_ID
                FROM [BUYP].[XXALJE_UNDELIVERED_DATA_BUYP1] u
                LEFT JOIN ALJE_EMPLOYEE_BUYP e
                ON e.EMPLOYEE_NUMBER = u.SALESMAN_NO
                WHERE u.SALESMAN_NO = %s
                ORDER BY u.ORG_ID;
            """

            # Execute the query with the provided employeeno
            with connection.cursor() as cursor:
                cursor.execute(query, [employeeno])
                rows = cursor.fetchall()

            # Process the results
            org_ids = [row[0] for row in rows]

            if not org_ids:
                return Response(
                    {"message": "No ORG_ID found for the given SALESMAN_NO."},
                    status=status.HTTP_404_NOT_FOUND
                )

            # Return the list of ORG_IDs
            return Response({"org_ids": org_ids}, status=status.HTTP_200_OK)

        except Exception as e:
            return Response(
                {"error": str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
  
# class CustomerNamelistView(APIView):
#     """
#     A View for listing unique customer numbers and names filtered by SALESMAN_NO
#     where QUANTITY > (DISPATCH_QTY + RETURN_QTY).
#     """
#     pagination_class = StandardResultsSetPagination

#     def get(self, request, salesmanno):
#         # query = '''
#         #     SELECT
#         #         A.CUSTOMER_NUMBER,
#         #         MAX(A.CUSTOMER_NAME) AS UNDEL_CUSTOMER_NAME,
#         #         MAX(ISNULL(C.CUSTOMER_NAME, A.CUSTOMER_NAME)) AS FINAL_CUSTOMER_NAME
#         #     FROM
#         #         [BUYP].[XXALJE_UNDELIVERED_DATA_BUYP1] A
#         #     LEFT JOIN
#         #         [BUYP].[ALJE_CUSTOMERS] C
#         #         ON A.CUSTOMER_NUMBER = C.CUSTOMER_NUMBER
#         #     WHERE
#         #         A.SALESMAN_NO = %s
#         #         AND ISNULL(A.QUANTITY, 0) > (ISNULL(A.DISPATCH_QTY, 0) + ISNULL(A.RETURN_QTY, 0))
#         #     GROUP BY
#         #         A.CUSTOMER_NUMBER;
#         # '''

#         query = '''
#             ;WITH A_agg AS (
#                 SELECT
#                     CUSTOMER_NUMBER,
#                     MAX(CUSTOMER_NAME)        AS UNDEL_CUSTOMER_NAME,
#                     SUM(QUANTITY)             AS quantity,
#                     SUM(DISPATCH_QTY)         AS dispatch_qty,
#                     SUM(RETURN_QTY)           AS return_qty
#                 FROM [BUYP].[XXALJE_UNDELIVERED_DATA_BUYP1]
#                 WHERE SALESMAN_NO = %s
#                 GROUP BY CUSTOMER_NUMBER
#             ),
#             D_agg AS (
#                 SELECT
#                     CUSTOMER_NUMBER,
#                     SUM(TRUCK_SCAN_QTY) AS truck_qty
#                 FROM WHR_CREATE_DISPATCH WHERE SALESMAN_NO = %s
#                 GROUP BY CUSTOMER_NUMBER
#             )
#             SELECT
#                 a.CUSTOMER_NUMBER,
#                 a.UNDEL_CUSTOMER_NAME,
#                 ISNULL(c.CUSTOMER_NAME, a.UNDEL_CUSTOMER_NAME) AS FINAL_CUSTOMER_NAME,
#                 ISNULL(a.quantity,0)      AS quantity,
#                 ISNULL(a.dispatch_qty,0)  AS dispatch_qty,
#                 ISNULL(a.return_qty,0)    AS return_qty,
#                 ISNULL(d.truck_qty,0)     AS truck_qty
#             FROM A_agg a
#             LEFT JOIN [BUYP].[ALJE_CUSTOMERS] c
#                 ON a.CUSTOMER_NUMBER = c.CUSTOMER_NUMBER
#             LEFT JOIN D_agg d
#                 ON a.CUSTOMER_NUMBER = d.CUSTOMER_NUMBER
#             WHERE ISNULL(a.quantity,0) >
#                 (ISNULL(a.dispatch_qty,0) + ISNULL(a.return_qty,0) + ISNULL(d.truck_qty,0));
#         '''

#         with connection.cursor() as cursor:
#             cursor.execute(query, [salesmanno],[salesmanno])   # ✅ works now
#             rows = cursor.fetchall()

#         # Prepare the data for serialization
#         data = [
#             {
#                 'customer_number': row[0],
#                 'customer_name': row[2]  # FINAL_CUSTOMER_NAME
#             }
#             for row in rows
#         ]

#         # Paginate the data
#         paginator = self.pagination_class()
#         paginated_data = paginator.paginate_queryset(data, request, view=self)

#         return paginator.get_paginated_response(paginated_data)

class CustomerNamelistView(APIView):
    """
    A View for listing unique customer numbers and names filtered by SALESMAN_NO
    where QUANTITY > (DISPATCH_QTY + RETURN_QTY).
    """
    pagination_class = StandardResultsSetPagination

    def get(self, request, salesmanno):
        query = '''
            ;WITH A_agg AS (
                SELECT                   
                  CUSTOMER_NUMBER,  INVOICE_NUMBER ,
                    MAX(CUSTOMER_NAME)        AS UNDEL_CUSTOMER_NAME,
                    SUM(QUANTITY)             AS quantity,
                    SUM(DISPATCH_QTY)         AS dispatch_qty,
                    SUM(RETURN_QTY)           AS return_qty
                FROM [BUYP].[XXALJE_UNDELIVERED_DATA_BUYP1]
                WHERE SALESMAN_NO = %s
                GROUP BY CUSTOMER_NUMBER ,INVOICE_NUMBER
            ),
            D_agg AS (
                SELECT
                    CUSTOMER_NUMBER, INVOICE_NUMBER ,

                    SUM(TRUCK_SCAN_QTY) AS truck_qty
                FROM WHR_CREATE_DISPATCH WHERE SALESMAN_NO = %s
                GROUP BY CUSTOMER_NUMBER, INVOICE_NUMBER 

            )
            SELECT
                a.CUSTOMER_NUMBER,a.INVOICE_NUMBER ,

                a.UNDEL_CUSTOMER_NAME,
                ISNULL(c.CUSTOMER_NAME, a.UNDEL_CUSTOMER_NAME) AS FINAL_CUSTOMER_NAME,
                ISNULL(a.quantity,0)      AS quantity,
                ISNULL(a.dispatch_qty,0)  AS dispatch_qty,
                ISNULL(a.return_qty,0)    AS return_qty,
                ISNULL(d.truck_qty,0)     AS truck_qty
            FROM A_agg a
            LEFT JOIN [BUYP].[ALJE_CUSTOMERS] c
                ON a.CUSTOMER_NUMBER = c.CUSTOMER_NUMBER
            LEFT JOIN D_agg d
                ON a.CUSTOMER_NUMBER = d.CUSTOMER_NUMBER and a.INVOICE_NUMBER  = d.INVOICE_NUMBER 
            WHERE ISNULL(a.quantity,0) >
                (ISNULL(a.dispatch_qty,0) + ISNULL(a.return_qty,0) + ISNULL(d.truck_qty,0));
        '''

        with connection.cursor() as cursor:
            cursor.execute(query, [salesmanno, salesmanno])
            rows = cursor.fetchall()

        # Deduplicate results
        unique_data = {}
        for row in rows:
            cust_num = row[0]
            if cust_num not in unique_data:
                unique_data[cust_num] = {
                    'customer_number': cust_num,
                    'customer_name': row[2]  # FINAL_CUSTOMER_NAME
                }

        data = list(unique_data.values())

        # Paginate the data
        paginator = self.pagination_class()
        paginated_data = paginator.paginate_queryset(data, request, view=self)

        return paginator.get_paginated_response(paginated_data)


  
class Invocie_Return_CustomerNamelistView(APIView):
    """
    A View for listing unique customer numbers and names filtered by SALESMAN_NO
    where QUANTITY > (DISPATCH_QTY + RETURN_QTY).
    """
    pagination_class = StandardResultsSetPagination

    def get(self, request, warehousename):
        query = '''
            SELECT
                A.CUSTOMER_NUMBER,
                MAX(A.CUSTOMER_NAME) AS UNDEL_CUSTOMER_NAME,
                MAX(ISNULL(C.CUSTOMER_NAME, A.CUSTOMER_NAME)) AS FINAL_CUSTOMER_NAME
            FROM
                [BUYP].[XXALJE_UNDELIVERED_DATA_BUYP1] A
            LEFT JOIN
                [BUYP].[ALJE_CUSTOMERS] C
                ON A.CUSTOMER_NUMBER = C.CUSTOMER_NUMBER
            WHERE
                A.TO_WAREHOUSE = %s
                AND ISNULL(A.QUANTITY, 0) > (ISNULL(A.DISPATCH_QTY, 0) + ISNULL(A.RETURN_QTY, 0))
            GROUP BY
                A.CUSTOMER_NUMBER;
        '''

        # Execute the query with parameterized inputs
        with connection.cursor() as cursor:
            cursor.execute(query, [warehousename])
            rows = cursor.fetchall()

        # Prepare the data for serialization
        data = [
            {
                'customer_number': row[0],
                'customer_name': row[2]  # FINAL_CUSTOMER_NAME
            } for row in rows
        ]

        # Paginate the data
        paginator = self.pagination_class()
        paginated_data = paginator.paginate_queryset(data, request, view=self)

        return paginator.get_paginated_response(paginated_data)



# class CustomerNamelistView(APIView):
#     """
#     A View for listing customer names and numbers filtered by SALESMAN_NO
#     using a custom SQL query.
#     """
#     pagination_class = StandardResultsSetPagination
 
#     def get(self, request, salesmanno):
       
#         # query = '''
#         #     SELECT
#         #         CUSTOMER_NUMBER,
#         #         CUSTOMER_NAME,
#         #         SUM(ISNULL(QUANTITY, 0)) AS TOTAL_QUANTITY,
#         #         SUM(ISNULL(DISPATCH_QTY, 0)) AS TOTAL_DISPATCH_QTY,
#         #         SUM(ISNULL(RETURN_QTY, 0)) AS TOTAL_RETURN_QTY,
#         #         SUM(ISNULL(DISPATCH_QTY, 0)) + SUM(ISNULL(RETURN_QTY, 0)) AS TOTAL_DISPATCH_RETURN_QTY
#         #     FROM
#         #         [BUYP].[XXALJE_UNDELIVERED_DATA_BUYP1]
#         #     WHERE
#         #         SALESMAN_NO = %s
#         #     GROUP BY
#         #         CUSTOMER_NUMBER,
#         #         CUSTOMER_NAME
#         #     HAVING
#         #         SUM(ISNULL(QUANTITY, 0)) > SUM(ISNULL(DISPATCH_QTY, 0)) + SUM(ISNULL(RETURN_QTY, 0));
#         # '''
 
#         query = '''
#                 SELECT
#                     A.CUSTOMER_NUMBER,
#                     A.CUSTOMER_NAME AS UNDEL_CUSTOMER_NAME,
#                     ISNULL(C.CUSTOMER_NAME, A.CUSTOMER_NAME) AS FINAL_CUSTOMER_NAME,
#                     SUM(ISNULL(A.QUANTITY, 0)) AS TOTAL_QUANTITY,
#                     SUM(ISNULL(A.DISPATCH_QTY, 0)) AS TOTAL_DISPATCH_QTY,
#                     SUM(ISNULL(A.RETURN_QTY, 0)) AS TOTAL_RETURN_QTY,
#                     SUM(ISNULL(A.DISPATCH_QTY, 0)) + SUM(ISNULL(A.RETURN_QTY, 0)) AS TOTAL_DISPATCH_RETURN_QTY
#                 FROM
#                     [BUYP].[XXALJE_UNDELIVERED_DATA_BUYP1] A
#                 LEFT JOIN
#                     [BUYP].[ALJE_CUSTOMERS] C
#                     ON A.CUSTOMER_NUMBER = C.CUSTOMER_NUMBER
#                 WHERE
#                     A.SALESMAN_NO = %s
#                 GROUP BY
#                     A.CUSTOMER_NUMBER,
#                     A.CUSTOMER_NAME,
#                     C.CUSTOMER_NAME
#                 HAVING
#                     SUM(ISNULL(A.QUANTITY, 0)) >
#                     (SUM(ISNULL(A.DISPATCH_QTY, 0)) + SUM(ISNULL(A.RETURN_QTY, 0)));
#  '''
 
#         # Execute the query with parameterized inputs
#         with connection.cursor() as cursor:
#             cursor.execute(query, [salesmanno])
#             rows = cursor.fetchall()
 
#         # Prepare the data for serialization (only return customer_number and name)
#         data = [
#             {
#                 'customer_number': row[0],
#                 'customer_name': row[2]
#             } for row in rows
#         ]
 
#         # Paginate the data
#         paginator = self.pagination_class()
#         paginated_data = paginator.paginate_queryset(data, request, view=self)
 
#         return paginator.get_paginated_response(paginated_data)

class CustomerSiteIDListView(APIView):
    """
    A view to return party_site_name and customer_site_id
    for a given salesman number and customer number.
    """
    pagination_class = StandardResultsSetPagination

    def get(self, request, salesmanno, custno):
        query = '''
                SELECT 
                    ISNULL(MAX(C.PARTY_SITE_NAME), '') AS PARTY_SITE_NAME,
                    U.CUSTOMER_SITE_ID
                FROM 
                    [BUYP].[BUYP].[XXALJE_UNDELIVERED_DATA_BUYP1] U
                LEFT JOIN 
                    [BUYP].[BUYP].[ALJE_CUSTOMERS] C
                    ON C.CUSTOMER_NUMBER = U.CUSTOMER_NUMBER
                WHERE 
                    U.CUSTOMER_NUMBER =%s AND 
                    U.SALESMAN_NO = %s
                GROUP BY 
                    U.CUSTOMER_SITE_ID;

        '''

        try:
            with connection.cursor() as cursor:
                cursor.execute(query, [custno, salesmanno])
                rows = cursor.fetchall()

            # Only return the required fields
            data = [
                {
                    'party_site_name': row[0],
                    'customer_site_id': row[1],
                    'site_use_id': row[1],
                }
                for row in rows
            ]

            # Apply pagination
            paginator = self.pagination_class()
            paginated_data = paginator.paginate_queryset(data, request)
            return paginator.get_paginated_response(paginated_data)

        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_400_BAD_REQUEST)


class Invoice_Return_CustomerSiteIDListView(APIView):
    """
    A view to return party_site_name and customer_site_id
    for a given salesman number and customer number.
    """
    pagination_class = StandardResultsSetPagination

    def get(self, request, warehousename, custno):
        query = '''
                SELECT 
                    ISNULL(MAX(C.PARTY_SITE_NAME), '') AS PARTY_SITE_NAME,
                    U.CUSTOMER_SITE_ID
                FROM 
                    [BUYP].[BUYP].[XXALJE_UNDELIVERED_DATA_BUYP1] U
                LEFT JOIN 
                    [BUYP].[BUYP].[ALJE_CUSTOMERS] C
                    ON C.CUSTOMER_NUMBER = U.CUSTOMER_NUMBER
                WHERE 
                    U.CUSTOMER_NUMBER =%s AND 
                    U.TO_WAREHOUSE = %s
                GROUP BY 
                    U.CUSTOMER_SITE_ID;

        '''

        try:
            with connection.cursor() as cursor:
                cursor.execute(query, [custno, warehousename])
                rows = cursor.fetchall()

            # Only return the required fields
            data = [
                {
                    'party_site_name': row[0],
                    'customer_site_id': row[1],
                    'site_use_id': row[1],
                }
                for row in rows
            ]

            # Apply pagination
            paginator = self.pagination_class()
            paginated_data = paginator.paginate_queryset(data, request)
            return paginator.get_paginated_response(paginated_data)

        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_400_BAD_REQUEST)


class Create_DispatchView(viewsets.ModelViewSet):
    queryset = WHRCreateDispatch.objects.all()
    serializer_class = create_Dispatchserializers
    pagination_class = StandardResultsSetPagination

class OnProgress_DispatchView(viewsets.ModelViewSet):
    queryset = WHRCreateDispatch.objects.all().order_by('-id')  # Order by id descending
    serializer_class = create_Dispatchserializers
    pagination_class = StandardResultsSetPagination

@csrf_exempt  # Allow POST without CSRF token for API calls like Flutter
def update_createdispatch_qty(request):
    if request.method == "POST":
        try:
            # Load the data from the request body
            data = json.loads(request.body)
            dispatch_id = data.get('id')
            qty_to_subtract = data.get('qty')

            # Ensure both ID and qty are provided
            if not dispatch_id or qty_to_subtract is None:
                return JsonResponse({"status": "error", "message": "ID and QTY are required."}, status=400)

            # Fetch the current truck_scan_qty from the database
            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT TRUCK_SCAN_QTY
                    FROM [BUYP].[dbo].[WHR_CREATE_DISPATCH]
                    WHERE id = %s
                """, [dispatch_id])
                result = cursor.fetchone()

            # If no record is found, return an error
            if not result:
                return JsonResponse({"status": "error", "message": "Record not found for the given ID."}, status=404)

            current_qty = result[0]

            # Calculate the new quantity by subtracting the provided qty
            new_qty = current_qty - qty_to_subtract

            # Update the table with the new quantity
            with connection.cursor() as cursor:
                cursor.execute("""
                    UPDATE [BUYP].[dbo].[WHR_CREATE_DISPATCH]
                    SET TRUCK_SCAN_QTY = %s
                    WHERE id = %s
                """, [new_qty, dispatch_id])

            # Return a success response
            return JsonResponse({"status": "success", "message": f"Quantity updated successfully. New qty: {new_qty}"})

        except Exception as e:
            # Return error response if something goes wrong
            return JsonResponse({"status": "error", "message": str(e)}, status=500)

    # If the request is not POST, return method not allowed
    return JsonResponse({"status": "error", "message": "Invalid request method."}, status=405)

# class UpdateCreateDispatchRequestView(APIView):
#     def post(self, request, *args, **kwargs):
#         # Extract data from request
#         reqno = request.data.get("reqno")
#         cusno = request.data.get("cusno")
#         cussite = request.data.get("cussite")
        
#         invoiceno = request.data.get("invoiceno")
#         itemcode = request.data.get("itemcode")
#         qty = request.data.get("qty")  # DISPATCHED_QTY
#         balance_qty = request.data.get("balanceqty")  # BALANCE_QTY
#         dispatched_by_manager = request.data.get("dispatched_by_manager", 0)
#         truck_scan_qty = request.data.get("truck_scan_qty", 0)

#         # Validate required fields
#         if not all([reqno, cusno, cussite, itemcode]):
#             return Response({"error": "Missing required parameters."}, status=status.HTTP_400_BAD_REQUEST)

#         with connection.cursor() as cursor:
#             # Check if the dispatch entry exists
#             cursor.execute("""
#                 SELECT ID FROM WHR_CREATE_DISPATCH
#                 WHERE REQ_ID = %s AND CUSTOMER_NUMBER = %s AND CUSTOMER_SITE_ID = %s AND INVOICE_NUMBER = %s AND INVENTORY_ITEM_ID = %s
#             """, [reqno, cusno, cussite, invoiceno, itemcode])
#             row = cursor.fetchone()

#             if not row:
#                 return Response({"error": "Dispatch object not found."}, status=status.HTTP_404_NOT_FOUND)

#             dispatch_id = row[0]

#             # Update the record
#             cursor.execute("""
#                 UPDATE WHR_CREATE_DISPATCH
#                 SET DISPATCHED_QTY = %s,
#                     BALANCE_QTY = %s,
#                     DISPATCHED_BY_MANAGER = %s,
#                     TRUCK_SCAN_QTY = %s,
#                     LAST_UPDATE_DATE = GETDATE()
#                 WHERE ID = %s
#             """, [qty, balance_qty, dispatched_by_manager, truck_scan_qty, dispatch_id])

#         return Response({"message": "Dispatch updated successfully."}, status=status.HTTP_200_OK)


class UpdateCreateDispatchRequestView(APIView):
    def post(self, request, *args, **kwargs):
        # Extract data from request
        reqno = request.data.get("reqno")
        cusno = request.data.get("cusno")
        cussite = request.data.get("cussite")
        invoiceno = request.data.get("invoiceno")
        itemcode = request.data.get("itemcode")
        qty = request.data.get("qty")  # DISPATCHED_QTY
        balance_qty = request.data.get("balanceqty")  # BALANCE_QTY
        dispatched_by_manager = request.data.get("dispatched_by_manager", 0)
        truck_scan_qty = request.data.get("truck_scan_qty", 0)
        deliveryaddress = request.data.get("deliveryaddress", "")
        others = request.data.get("others", "")

        # Validate required fields
        if not all([reqno, cusno, cussite, invoiceno, itemcode]):
            return Response({"error": "Missing required parameters."}, status=status.HTTP_400_BAD_REQUEST)

        try:
            with connection.cursor() as cursor:
                # Check if the dispatch entry exists
                cursor.execute("""
                    SELECT ID FROM WHR_CREATE_DISPATCH
                    WHERE REQ_ID = %s AND CUSTOMER_NUMBER = %s AND CUSTOMER_SITE_ID = %s AND INVOICE_NUMBER = %s AND INVENTORY_ITEM_ID = %s
                """, [reqno, cusno, cussite, invoiceno, itemcode])
                row = cursor.fetchone()

                if not row:
                    return Response({"error": "Dispatch object not found."}, status=status.HTTP_404_NOT_FOUND)

                dispatch_id = row[0]

                # Update the record
                cursor.execute("""
                    UPDATE WHR_CREATE_DISPATCH
                    SET DISPATCHED_QTY = %s,
                        BALANCE_QTY = %s,
                        DISPATCHED_BY_MANAGER = %s,
                        TRUCK_SCAN_QTY = %s,
                        DELIVERYADDRESS = %s,
                        REMARKS = %s,
                        LAST_UPDATE_DATE = GETDATE()
                    WHERE ID = %s
                """, [
                    qty,
                    balance_qty,
                    dispatched_by_manager,
                    truck_scan_qty,
                    deliveryaddress,
                    others,
                    dispatch_id
                ])

            return Response({"message": "Dispatch updated successfully."}, status=status.HTTP_200_OK)

        except Exception as e:
            return Response({"error": f"Failed to update dispatch. Details: {str(e)}"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

class GetFlagRcountcreatedispatchView(viewsets.ViewSet):
    """
    Custom view to filter data based on reqid, customerno, customersite, and flag='R',
    and return the count of the filtered records.
    """

    def list(self, request, reqid, cusno, cussite):
        # Filter queryset based on the parameters from the URL
        queryset = WHRCreateDispatch.objects.filter(
            REQ_ID=reqid,
            CUSTOMER_NUMBER=cusno,
            CUSTOMER_SITE_ID=cussite,
            FLAG='R'
        )

        # Count the filtered records
        count = queryset.count()

        return Response({"count": count})
    
class Create_DispatchReqnoView(APIView):
    def get(self, request, *args, **kwargs):
        # Get the largest REQ_ID, assuming REQ_ID is a numeric field.
        dispatch_with_highest_reqno = WHRCreateDispatch.objects.all().order_by('-REQ_ID').first()
        
        # Extract REQ_ID if such an object exists
        if dispatch_with_highest_reqno:
            REQ_ID = dispatch_with_highest_reqno.REQ_ID
        else:
            REQ_ID = "0"  # Default value if no records are found
        
        return Response({'REQ_ID': REQ_ID})

class CommericialDispatch(viewsets.ModelViewSet):
    queryset = WHRCreateDispatch.objects.all()
    serializer_class = create_Dispatchserializers

    def get_queryset(self):
     """
     Get the queryset based on the 'commericialno' parameter from the URL.
     If 'commericialno' is provided, it filters by it and aggregates the `DISPATCHED_QTY`.
     """
     commericialno = self.kwargs.get('commericialno')
     if commericialno:
        filtered_queryset = WHRCreateDispatch.objects.filter(COMMERCIAL_NO=commericialno)
        if not filtered_queryset.exists():
            raise NotFound(f"No records found for commercialNo = {commericialno}")

        # Aggregate the `DISPATCHED_QTY` by SUM for unique combinations
        aggregated_queryset = filtered_queryset.values(
            "REQ_ID", "INVOICE_DATE", "COMMERCIAL_NO", "COMMERCIAL_NAME",
            "SALESMAN_NO", "SALESMAN_NAME", "CUSTOMER_NUMBER",
            "CUSTOMER_NAME", "CUSTOMER_SITE_ID"
        ).annotate(
            total_dispatched_qty=Sum('DISPATCHED_QTY')
        ).order_by()  # <== Important fix: Remove ordering on ID or any other field not in GROUP BY

        return aggregated_queryset

     return WHRCreateDispatch.objects.none()

    def list(self, request, *args, **kwargs):
        """
        Custom response to include the additional fields and aggregated `DISPATCHED_QTY`.
        """
        queryset = self.get_queryset()
        page = self.paginate_queryset(queryset)
        if page is not None:
            custom_data = [
                {
                    "REQ_ID": record["REQ_ID"],
                    "INVOICE_DATE": record["INVOICE_DATE"],                    
                    "COMMERCIAL_NO": record["COMMERCIAL_NO"],
                    "COMMERCIAL_NAME": record["COMMERCIAL_NAME"],
                    "SALESMAN_NO": record["SALESMAN_NO"],
                    "SALESMAN_NAME": record["SALESMAN_NAME"],
                    "CUSTOMER_NUMBER": record["CUSTOMER_NUMBER"],
                    "CUSTOMER_NAME": record["CUSTOMER_NAME"],
                    "CUSTOMER_SITE_ID": record["CUSTOMER_SITE_ID"],
                    "DISPATCHED_QTY": record["total_dispatched_qty"],  # Show aggregated dispatched quantity
                }
                for record in page
            ]
            return self.get_paginated_response(custom_data)

        custom_data = [
            {
                "REQ_ID": record["REQ_ID"],
                "INVOICE_DATE": record["INVOICE_DATE"],                
                "COMMERCIAL_NO": record["COMMERCIAL_NO"],
                "COMMERCIAL_NAME": record["COMMERCIAL_NAME"],
                "SALESMAN_NO": record["SALESMAN_NO"],
                "SALESMAN_NAME": record["SALESMAN_NAME"],
                "CUSTOMER_NUMBER": record["CUSTOMER_NUMBER"],
                "CUSTOMER_NAME": record["CUSTOMER_NAME"],
                "CUSTOMER_SITE_ID": record["CUSTOMER_SITE_ID"],
                "DISPATCHED_QTY": record["total_dispatched_qty"],  # Show aggregated dispatched quantity
            }
            for record in queryset
        ]
        return Response(custom_data)
 

 
class TopProductsViewSet(viewsets.ViewSet):
    def list(self, request):
        # Get filter parameters
        filterstatus = request.query_params.get("filterstatus")
        employee_id = request.query_params.get("employee_id")
        warehouse_name = request.query_params.get("warehouse_name")

        # Start with all objects
        queryset = WHRCreateDispatch.objects.all()

        # Apply filters based on filterstatus
        if filterstatus == "salesman" and employee_id:
            queryset = queryset.filter(SALESMAN_NO=employee_id)
        elif filterstatus == "whrsuperuser" and warehouse_name:
            queryset = queryset.filter(PHYSICAL_WAREHOUSE=warehouse_name)

        # Aggregate top 5 products by DISPATCHED_QTY
        top_products = (
            queryset.values("INVENTORY_ITEM_ID")
            .annotate(total_dispatched_qty=Sum("DISPATCHED_QTY"))
            .order_by("-total_dispatched_qty")[:5]
        )

        return Response(top_products)


class Filtered_balanceDispatchView(viewsets.ModelViewSet):
    queryset = WHRCreateDispatch.objects.all()
    serializer_class = create_Dispatchserializers

    @action(detail=False, methods=['get'])
    def filter_dispatch(self, request):
        # Get the salesman number and invoice number from query parameters
        salesman_no = self.request.query_params.get('SALESMAN_NO', None)
        invoice_number = self.request.query_params.get('INVOICE_NUMBER', None)

        # Ensure both parameters are provided
        if salesman_no and invoice_number:
            # Filter the dispatch data based on salesman number and invoice number
            dispatch_queryset = WHRCreateDispatch.objects.filter(
                SALESMAN_NO=salesman_no,
                INVOICE_NUMBER=invoice_number
            ).values('INVENTORY_ITEM_ID', 'TRUCK_SCAN_QTY')

            # Check if any dispatch records exist
            if not dispatch_queryset.exists():
                return Response({'detail': 'No records found in WHRCreateDispatch'}, status=404)

            # Prepare a result list
            result = []

            # Iterate through the dispatch data to compare with return dispatch data
            for dispatch in dispatch_queryset:
                inventory_item_id = dispatch['INVENTORY_ITEM_ID']
                dispatched_qty = float(dispatch['TRUCK_SCAN_QTY'])  # Convert to float for arithmetic operations

                # Fetch the corresponding return dispatch data and sum the TRUCK_SEND_QTY
                truck_send_qty = WHRReturnDispatch.objects.filter(
                    INVOICE_NO=invoice_number,
                    ITEM_CODE=inventory_item_id
                ).aggregate(total_truck_send_qty=Sum('TRUCK_SEND_QTY'))['total_truck_send_qty'] or 0

                # Calculate the difference between dispatched and truck send quantity
                balance_qty = dispatched_qty - float(truck_send_qty)

                # Append the result to the list
                result.append({
                    'INVENTORY_ITEM_ID': inventory_item_id,
                    'DISPATCHED_QTY': dispatched_qty,
                    'TRUCK_SEND_QTY': truck_send_qty,
                    'BALANCE_QTY': balance_qty
                })

            # Return the result
            return Response(result)

        # Return an error if parameters are missing
        return Response({'detail': 'Please provide both SALESMAN_NO and INVOICE_NUMBER in the URL'}, status=400)
 
class Dispatch_requestView(viewsets.ModelViewSet):
    queryset = WHRDispatchRequest.objects.all()
    serializer_class = Dispatch_requestserializers
    pagination_class = StandardResultsSetPagination

class FilteredDispatchRequestView(viewsets.ModelViewSet):
    queryset = WHRDispatchRequest.objects.all()
    serializer_class = Dispatch_requestserializers

    def get_queryset(self):
        queryset = super().get_queryset()
        reqid = self.kwargs.get('reqid')
        cusno = self.kwargs.get('cusno')
        cussite = self.kwargs.get('cussite')
        itemcode = self.kwargs.get('itemcode')

        if reqid:
            queryset = queryset.filter(REQ_ID=reqid)
        if cusno:
            queryset = queryset.filter(CUSTOMER_NUMBER=cusno)
        if cussite:
            queryset = queryset.filter(CUSTOMER_SITE_ID=cussite)
        if itemcode:
            queryset = queryset.filter(INVENTORY_ITEM_ID=itemcode)

        return queryset
    
class GetIdCreateDispatchView(viewsets.ModelViewSet):
    queryset = WHRCreateDispatch.objects.all()
    serializer_class = create_Dispatchserializers

    def get_queryset(self):
        queryset = super().get_queryset()
        reqid = self.kwargs.get('reqid')
        cusno = self.kwargs.get('cusno')
        cussite = self.kwargs.get('cussite')        
        invoiceno = self.kwargs.get('invoiceno')
        itemcode = self.kwargs.get('itemcode')

        if reqid:
            queryset = queryset.filter(REQ_ID=reqid)
        if cusno:
            queryset = queryset.filter(CUSTOMER_NUMBER=cusno)
        if cussite:
            queryset = queryset.filter(CUSTOMER_SITE_ID=cussite)
        if invoiceno:
            queryset = queryset.filter(INVOICE_NUMBER=invoiceno)
        if itemcode:
            queryset = queryset.filter(INVENTORY_ITEM_ID=itemcode)

        return queryset    
  
class FilteredCreateDispatchView(viewsets.ModelViewSet):
    queryset = WHRCreateDispatch.objects.all()
    serializer_class = create_Dispatchserializers

    def get_queryset(self):
        queryset = super().get_queryset()
        reqid = self.kwargs.get('reqid')
        cusno = self.kwargs.get('cusno')
        cussite = self.kwargs.get('cussite')

        if reqid:
            queryset = queryset.filter(REQ_ID=reqid)
        if cusno:
            queryset = queryset.filter(CUSTOMER_NUMBER=cusno)
        if cussite:
            queryset = queryset.filter(CUSTOMER_SITE_ID=cussite)
        

        return queryset

class Filtered_dispatchRequestView(viewsets.ModelViewSet):
    serializer_class = FilteredDispatchRequestSerializer
    pagination_class = StandardResultsSetPagination

    def get_queryset(self):
        req_id = self.kwargs.get('REQ_ID', None)  # Get 'REQ_ID' from URL
        warehouse_name = self.kwargs.get('warehouse_name', None)  # Get warehouse name from URL

        if req_id is not None and warehouse_name is not None:
            return WHRCreateDispatch.objects.filter(
                REQ_ID=req_id,
                PHYSICAL_WAREHOUSE__iexact=warehouse_name  # Case-insensitive match
            ).exclude(FLAG='D')
        return WHRCreateDispatch.objects.none()

    def list(self, request, *args, **kwargs):
        queryset = self.get_queryset()

        # Grouping by REQ_ID
        result = {}
        for record in queryset:
            if record.REQ_ID not in result:
                result[record.REQ_ID] = {
                    "REQ_ID": record.REQ_ID,
                    "TO_WAREHOUSE": record.PHYSICAL_WAREHOUSE,
                    "ORG_ID": record.ORG_ID,
                    "ORG_NAME": record.ORG_NAME,
                    "SALESMAN_NO": record.SALESMAN_NO,
                    "SALESMAN_NAME": record.SALESMAN_NAME,
                    "CUSTOMER_NUMBER": record.CUSTOMER_NUMBER,
                    "CUSTOMER_NAME": record.CUSTOMER_NAME,
                    "CUSTOMER_SITE_ID": record.CUSTOMER_SITE_ID,
                    "INVOICE_DATE": record.INVOICE_DATE,
                    "INVOICE_NUMBER": record.INVOICE_NUMBER,
                    "DELIVERY_DATE": record.DELIVERY_DATE,
                    "DELIVERYADDRESS": record.DELIVERYADDRESS,
                    "TABLE_DETAILS": []
                }

            detail = {
                "ID": record.id,
                "UNDEL_ID": record.UNDEL_ID,
                "INVOICE_NUMBER": record.INVOICE_NUMBER,
                "CUSTOMER_TRX_ID": record.CUSTOMER_TRX_ID,
                "CUSTOMER_TRX_LINE_ID": record.CUSTOMER_TRX_LINE_ID,
                "LINE_NUMBER": record.LINE_NUMBER,
                "INVENTORY_ITEM_ID": record.INVENTORY_ITEM_ID,
                "ITEM_DESCRIPTION": record.ITEM_DESCRIPTION,
                "TOT_QUANTITY": record.TOT_QUANTITY,
                "DISPATCHED_QTY": record.DISPATCHED_QTY,
                "DISPATCHED_BY_MANAGER": record.DISPATCHED_BY_MANAGER,
                "BALANCE_QTY": record.BALANCE_QTY,
            }

            result[record.REQ_ID]["TABLE_DETAILS"].append(detail)

        return Response(list(result.values()))
    
class Filtered_ReturndispatchView(viewsets.ModelViewSet):
    serializer_class = FilteredReturnDispatchSerializer
    pagination_class = StandardResultsSetPagination

    def get_queryset(self):
        return_dis_id = self.kwargs.get('RETURN_DIS_ID', None)
        warehouse_name = self.kwargs.get('warehouse_name', None)

        if return_dis_id and warehouse_name:
            return WHRReturnDispatch.objects.filter(
                RETURN_DIS_ID=return_dis_id,
                ORG_NAME__iexact=warehouse_name
            ).exclude(FLAG='D')

        return WHRReturnDispatch.objects.none()

    def list(self, request, *args, **kwargs):
        queryset = self.get_queryset()

        # Group by RETURN_DIS_ID for the top level
        result = {}
        for record in queryset:
            rd_id = record.RETURN_DIS_ID
            if rd_id not in result:
                result[rd_id] = {
                    "RETURN_DIS_ID": record.RETURN_DIS_ID,
                    "DISPATCH_ID": record.DISPATCH_ID,
                    "REQ_ID": record.REQ_ID,
                    "TO_WAREHOUSE": record.PHYSICAL_WAREHOUSE,
                    "ORG_ID": record.ORG_ID,
                    "ORG_NAME": record.ORG_NAME,
                    "SALESMAN_NO": record.SALESMAN_NO,
                    "SALESMAN_NAME": record.SALESMAN_NAME,
                    "MANAGER_NO": record.MANAGER_NO,
                    "MANAGER_NAME": record.MANAGER_NAME,
                    
                    "INVOICE_DATE": record.DATE,
                    "DELIVERY_DATE": record.DATE,
                    "CUSTOMER_NUMBER": record.CUSTOMER_NUMBER,
                    "CUSTOMER_NAME": record.CUSTOMER_NAME,
                    "CUSTOMER_SITE_ID": record.CUSTOMER_SITE_ID,
                    "RETURN_REASON": record.RETURN_REASON,
                    "TABLE_DETAILS": {}
                }

            # Composite key for grouping
            detail_key = (
                record.INVOICE_NO,
                str(record.CUSTOMER_TRX_ID),
                str(record.CUSTOMER_TRX_LINE_ID),
                record.LINE_NUMBER,
                record.ITEM_CODE,
                record.ITEM_DETAILS
            )

            # Access TABLE_DETAILS dict
            table_details_dict = result[rd_id]["TABLE_DETAILS"]

            if detail_key not in table_details_dict:
                table_details_dict[detail_key] = {
                 
                    "UNDEL_ID": record.UNDEL_ID or "",
                    "INVOICE_NUMBER": record.INVOICE_NO,
                    "CUSTOMER_TRX_ID": str(record.CUSTOMER_TRX_ID),
                    "CUSTOMER_TRX_LINE_ID": str(record.CUSTOMER_TRX_LINE_ID),
                    "LINE_NUMBER": record.LINE_NUMBER,
                    "INVENTORY_ITEM_ID": record.ITEM_CODE,
                    "ITEM_DESCRIPTION": record.ITEM_DETAILS,
                    "TOT_QUANTITY": int(record.DISREQ_QTY or 0),
                    "DISPATCHED_QTY": int(record.DISREQ_QTY or 0),
                    "DISPATCHED_BY_MANAGER": int(record.TRUCK_SEND_QTY or 0)
                }
            else:
                # Add to existing TRUCK_SEND_QTY
                table_details_dict[detail_key]["DISPATCHED_BY_MANAGER"] += int(record.TRUCK_SEND_QTY or 0)

        # Convert TABLE_DETAILS from dict to list
        for item in result.values():
            item["TABLE_DETAILS"] = list(item["TABLE_DETAILS"].values())

        return Response(list(result.values()))

class Filtered_ReturndispatchView(viewsets.ModelViewSet):
    serializer_class = FilteredReturnDispatchSerializer
    pagination_class = StandardResultsSetPagination

    def get_queryset(self):
        return_dis_id = self.kwargs.get('RETURN_DIS_ID', None)
        warehouse_name = self.kwargs.get('warehouse_name', None)

        if return_dis_id and warehouse_name:
            return WHRReturnDispatch.objects.filter(
                RETURN_DIS_ID=return_dis_id,
                ORG_NAME__iexact=warehouse_name
            ).exclude(FLAG='D')

        return WHRReturnDispatch.objects.none()

    def list(self, request, *args, **kwargs):
        queryset = self.get_queryset()

        # Group by RETURN_DIS_ID for the top level
        result = {}
        for record in queryset:
            rd_id = record.RETURN_DIS_ID
            if rd_id not in result:
                result[rd_id] = {
                    "RETURN_DIS_ID": record.RETURN_DIS_ID,
                    "DISPATCH_ID": record.DISPATCH_ID,
                    "REQ_ID": record.REQ_ID,
                    "TO_WAREHOUSE": record.PHYSICAL_WAREHOUSE,
                    "ORG_ID": record.ORG_ID,
                    "ORG_NAME": record.ORG_NAME,
                    "SALESMAN_NO": record.SALESMAN_NO,
                    "SALESMAN_NAME": record.SALESMAN_NAME,
                    "MANAGER_NO": record.MANAGER_NO,
                    "MANAGER_NAME": record.MANAGER_NAME,
                    
                    "INVOICE_DATE": record.DATE,
                    "DELIVERY_DATE": record.DATE,
                    "CUSTOMER_NUMBER": record.CUSTOMER_NUMBER,
                    "CUSTOMER_NAME": record.CUSTOMER_NAME,
                    "CUSTOMER_SITE_ID": record.CUSTOMER_SITE_ID,
                    "RETURN_REASON": record.RETURN_REASON,
                    "TABLE_DETAILS": {}
                }

            # Composite key for grouping
            detail_key = (
                record.INVOICE_NO,
                str(record.CUSTOMER_TRX_ID),
                str(record.CUSTOMER_TRX_LINE_ID),
                record.LINE_NUMBER,
                record.ITEM_CODE,
                record.ITEM_DETAILS
            )

            # Access TABLE_DETAILS dict
            table_details_dict = result[rd_id]["TABLE_DETAILS"]

            if detail_key not in table_details_dict:
                table_details_dict[detail_key] = {
                 
                    "UNDEL_ID": record.UNDEL_ID or "",
                    "INVOICE_NUMBER": record.INVOICE_NO,
                    "CUSTOMER_TRX_ID": str(record.CUSTOMER_TRX_ID),
                    "CUSTOMER_TRX_LINE_ID": str(record.CUSTOMER_TRX_LINE_ID),
                    "LINE_NUMBER": record.LINE_NUMBER,
                    "INVENTORY_ITEM_ID": record.ITEM_CODE,
                    "ITEM_DESCRIPTION": record.ITEM_DETAILS,
                    "TOT_QUANTITY": int(record.DISREQ_QTY or 0),
                    "DISPATCHED_QTY": int(record.DISREQ_QTY or 0),
                    "DISPATCHED_BY_MANAGER": int(record.TRUCK_SEND_QTY or 0)
                }
            else:
                # Add to existing TRUCK_SEND_QTY
                table_details_dict[detail_key]["DISPATCHED_BY_MANAGER"] += int(record.TRUCK_SEND_QTY or 0)

        # Convert TABLE_DETAILS from dict to list
        for item in result.values():
            item["TABLE_DETAILS"] = list(item["TABLE_DETAILS"].values())

        return Response(list(result.values()))
    
class Filtered_InterORGReportView(viewsets.ModelViewSet):
    serializer_class = FilteredInterOrgReporterializer
    pagination_class = StandardResultsSetPagination

    def get_queryset(self):
        shipment_id = self.kwargs.get('shipment_id', None)

        if shipment_id :
            return ShimentDispatchModels.objects.filter(
                shipment_id=shipment_id
            )

        return ShimentDispatchModels.objects.none()

    def list(self, request, *args, **kwargs):
        queryset = self.get_queryset()

        # Group by RETURN_DIS_ID for the top level
        result = {}
        for record in queryset:
            rd_id = record.shipment_id
            if rd_id not in result:
                result[rd_id] = {
                    "shipment_id": record.shipment_id,
                    "salesmanno": record.salesmanno,
                    "salesmanname": record.salesmanname,
                    "date": record.date,
                    "transporter_name": record.transporter_name,
                    "driver_name": record.driver_name,
                    "driver_mobileno": record.driver_mobileno,
                    "vehicle_no": record.vehicle_no,
                    "truck_dimension": record.truck_dimension,
                    "loading_charges": record.loading_charges,
                    
                    "transport_charges": record.transport_charges,
                    "misc_charges": record.misc_charges,
                    "deliveryaddress": record.deliveryaddress,
                    "shipment_header_id": record.shipment_header_id,
                    "shipment_line_id": record.shipment_line_id,
                    "shipment_num": record.shipment_num,
                    
                    "receipt_num": record.receipt_num,
                    "organization_id": record.organization_id,
                    "organization_name": record.organization_name,
                    "organization_code": record.organization_code,
                    "to_orgn_id": record.to_orgn_id ,
                    "to_orgn_code": record.to_orgn_code,
                    "to_orgn_name": record.to_orgn_name,
                    "remarks": record.remarks,
                    "TABLE_DETAILS": {}
                }

            # Composite key for grouping
            detail_key = (
                record.line_num,
                str(record.item_id),
                str(record.description),
                record.quantity_shipped,
                record.quantity_received,
                record.quantity_progress
            )

            # Access TABLE_DETAILS dict
            table_details_dict = result[rd_id]["TABLE_DETAILS"]

            if detail_key not in table_details_dict:
                table_details_dict[detail_key] = {
                 
                    "shipment_line_id": str(record.shipment_line_id),
                    "line_num": record.line_num or "",
                    "item_id": record.item_id,
                    "description": str(record.description),
                    "quantity_shipped": str(record.quantity_shipped),
                    "quantity_received": record.quantity_received,
                    "quantity_progress": record.quantity_progress,
                 
                }
            
        # Convert TABLE_DETAILS from dict to list
        for item in result.values():
            item["TABLE_DETAILS"] = list(item["TABLE_DETAILS"].values())

        return Response(list(result.values()))

@csrf_exempt
def update_dispatch_request(request):
    if request.method == 'POST':
        try:
            data = json.loads(request.body.decode('utf-8'))

            # Extract values from body
            REQ_ID = data.get('REQ_ID')
            CUSTOMER_NUMBER = data.get('CUSTOMER_NUMBER')
            CUSTOMER_SITE_ID = data.get('CUSTOMER_SITE_ID')
            INVOICE_NUMBER = data.get('INVOICE_NUMBER')
            INVENTORY_ITEM_ID = data.get('INVENTORY_ITEM_ID')

            qty = int(data.get('qty', 0))
            pick_id = data.get('PICK_ID', '')
            assign_pickman = data.get('ASSIGN_PICKMAN', '')
            manager_no = data.get('MANAGER_NO', '')
            manager_name = data.get('MANAGER_NAME', '')
            date = data.get('DATE', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

            if not all([REQ_ID, CUSTOMER_NUMBER, CUSTOMER_SITE_ID, INVOICE_NUMBER, INVENTORY_ITEM_ID]):
                return JsonResponse({'error': 'Missing required identifiers.'}, status=400)

            with connection.cursor() as cursor:
                # Step 1: Update WHR_DISPATCH_REQUEST
                cursor.execute("""
                    SELECT SCANNED_QTY
                    FROM BUYP.dbo.WHR_DISPATCH_REQUEST
                    WHERE REQ_ID = %s AND CUSTOMER_NUMBER = %s AND CUSTOMER_SITE_ID = %s
                      AND INVOICE_NUMBER = %s AND INVENTORY_ITEM_ID = %s
                      AND FLAG != 'D'
                """, [REQ_ID, CUSTOMER_NUMBER, CUSTOMER_SITE_ID, INVOICE_NUMBER, INVENTORY_ITEM_ID])
                
                row = cursor.fetchone()
                if not row:
                    return JsonResponse({'error': 'No matching row found in WHR_DISPATCH_REQUEST'}, status=404)

                existing_qty = int(row[0] or 0)
                new_qty = max(existing_qty - qty, 0)

                cursor.execute("""
                    UPDATE BUYP.dbo.WHR_DISPATCH_REQUEST
                    SET SCANNED_QTY = %s,
                        PICK_ID = %s,
                        ASSIGN_PICKMAN = %s,
                        MANAGER_NO = %s,
                        MANAGER_NAME = %s,
                        DATE = %s,
                        STATUS = 'pending',
                        LAST_UPDATE_DATE = GETDATE(),
                        LAST_UPDATED_BY = %s,
                        LAST_UPDATE_IP = ''
                    WHERE REQ_ID = %s AND CUSTOMER_NUMBER = %s AND CUSTOMER_SITE_ID = %s
                      AND INVOICE_NUMBER = %s AND INVENTORY_ITEM_ID = %s
                      AND FLAG != 'D'
                """, [
                    new_qty, pick_id, assign_pickman, manager_no, manager_name,
                    date, manager_name,
                    REQ_ID, CUSTOMER_NUMBER, CUSTOMER_SITE_ID, INVOICE_NUMBER, INVENTORY_ITEM_ID
                ])

                # Step 2: Update WHR_CREATE_DISPATCH
                cursor.execute("""
                    SELECT DISPATCHED_BY_MANAGER
                    FROM BUYP.dbo.WHR_CREATE_DISPATCH
                    WHERE REQ_ID = %s AND CUSTOMER_NUMBER = %s AND CUSTOMER_SITE_ID = %s
                      AND INVOICE_NUMBER = %s AND INVENTORY_ITEM_ID = %s
                      AND FLAG != 'D'
                """, [REQ_ID, CUSTOMER_NUMBER, CUSTOMER_SITE_ID, INVOICE_NUMBER, INVENTORY_ITEM_ID])

                dispatch_row = cursor.fetchone()
                if not dispatch_row:
                    return JsonResponse({'error': 'No matching row found in WHR_CREATE_DISPATCH'}, status=404)

                current_dispatched = int(dispatch_row[0] or 0)
                new_dispatched = max(current_dispatched - qty, 0)

                cursor.execute("""
                    UPDATE BUYP.dbo.WHR_CREATE_DISPATCH
                    SET DISPATCHED_BY_MANAGER = %s,
                        LAST_UPDATE_DATE = GETDATE(),
                        LAST_UPDATED_BY = %s
                    WHERE REQ_ID = %s AND CUSTOMER_NUMBER = %s AND CUSTOMER_SITE_ID = %s
                      AND INVOICE_NUMBER = %s AND INVENTORY_ITEM_ID = %s
                      AND FLAG != 'D'
                """, [
                    new_dispatched, manager_name,
                    REQ_ID, CUSTOMER_NUMBER, CUSTOMER_SITE_ID, INVOICE_NUMBER, INVENTORY_ITEM_ID
                ])

            return JsonResponse({
                'success': True,
                'message': 'Dispatch updated successfully',
                'incoming_qty': qty,
                'existing_SCANNED_QTY': existing_qty,
                'new_SCANNED_QTY': new_qty,
                'existing_DISPATCHED_BY_MANAGER': current_dispatched,
                'new_DISPATCHED_BY_MANAGER': new_dispatched
            })

        except Exception as e:
            return JsonResponse({'error': str(e)}, status=500)

    return JsonResponse({'error': 'Only POST method is allowed'}, status=405)


class InsertPickedManAssignData(APIView):
    def post(self, request):
        data = request.data
        rows = data.get("rows", [])
        row_count = data.get("row_count", 0)

        if not rows or not isinstance(rows, list):
            return Response({"error": "Rows must be a list of data maps."}, status=status.HTTP_400_BAD_REQUEST)

        # The order of columns
        columns = [
            "PICK_ID", "REQ_ID", "DATE", "ASSIGN_PICKMAN", "PHYSICAL_WAREHOUSE",
            "ORG_ID", "ORG_NAME", "SALESMAN_NO", "SALESMAN_NAME", "MANAGER_NO",
            "MANAGER_NAME", "PICKMAN_NO", "PICKMAN_NAME", "CUSTOMER_NUMBER",
            "CUSTOMER_NAME", "CUSTOMER_SITE_ID", "INVOICE_DATE", "INVOICE_NUMBER",
            "LINE_NUMBER", "CUSTOMER_TRX_ID", "CUSTOMER_TRX_LINE_ID", "INVENTORY_ITEM_ID",
            "ITEM_DESCRIPTION", "TOT_QUANTITY", "DISPATCHED_QTY", "BALANCE_QTY",
            "PICKED_QTY", "PRODUCT_CODE", "SERIAL_NO", "CREATION_DATE", "CREATED_BY",
            "CREATED_IP", "CREATED_MAC", "LAST_UPDATE_DATE", "LAST_UPDATED_BY",
            "LAST_UPDATE_IP", "FLAG", "UNDEL_ID"
        ]

        insert_sql = f"""
            INSERT INTO [BUYP].[dbo].[WHR_PICKED_MAN] (
                {', '.join(columns)}
            ) VALUES (
                {', '.join(['%s'] * len(columns))}
            )
        """

        inserted_count = 0
        with connection.cursor() as cursor:
            for idx, row in enumerate(rows):
                if not isinstance(row, dict):
                    return Response({
                        "error": f"Row {idx + 1} is not a valid mapping (dict)"
                    }, status=status.HTTP_400_BAD_REQUEST)

                try:
                    values = [row.get(col, None) for col in columns]
                    cursor.execute(insert_sql, values)
                    inserted_count += 1
                except Exception as e:
                    return Response({
                        "error": f"Insert failed on row {inserted_count + 1}: {str(e)}",
                        "failed_row_data": row
                    }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        return Response({
            "message": "Rows inserted successfully",
            "expected_row_count": row_count,
            "actual_inserted": inserted_count
        }, status=status.HTTP_201_CREATED)



# If UpdateDispatchQtyView is in the same module or another module, import it.
# Adjust the import path below as needed.
# from .views import UpdateDispatchQtyView
# OR if in another app:
# from myapp.views import UpdateDispatchQtyView

class NewInsertPickedManAssignData(APIView):
    def post(self, request):
        try:
            data = request.data
            rows = data.get("rows", [])

            try:
                rowcount_requested = int(data.get("row_count", 0))
            except (TypeError, ValueError):
                return Response({"error": "Invalid row_count; must be integer"}, status=400)

            if not rows or rowcount_requested <= 0:
                return Response({"error": "No rows or valid row_count provided"}, status=400)

            row = rows[0]
            if not isinstance(row, dict):
                return Response({"error": "Invalid row format; expected dict"}, status=400)

            # -------- Parse ISO datetime --------
            def parse_dt(val):
                if not val:
                    return None
                try:
                    if isinstance(val, datetime):
                        return val
                    s = str(val).replace("Z", "")
                    if "." in s:
                        s = s.split(".")[0]
                    return datetime.fromisoformat(s)
                except Exception:
                    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
                        try:
                            return datetime.strptime(s, fmt)
                        except:
                            pass
                    logger.warning("Unable to parse datetime: %s", val)
                    return None

            row["DATE"] = parse_dt(row.get("DATE"))
            row["INVOICE_DATE"] = parse_dt(row.get("INVOICE_DATE"))
            row["CREATION_DATE"] = parse_dt(row.get("CREATION_DATE"))
            row["LAST_UPDATE_DATE"] = parse_dt(row.get("LAST_UPDATE_DATE"))

            # -------- Required Fields --------
            required = ["PICK_ID", "REQ_ID", "INVOICE_NUMBER", "UNDEL_ID", "INVENTORY_ITEM_ID"]
            if not all(row.get(field) for field in required):
                return Response({"error": f"Missing required fields. Required: {required}"}, status=400)

            # -------- Assigned qty & Already picked --------
            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT ISNULL(PICKED_QTY, 0)
                    FROM WHR_DISPATCH_REQUEST
                    WHERE REQ_ID=%s AND PICK_ID=%s AND INVOICE_NUMBER=%s AND UNDEL_ID=%s AND INVENTORY_ITEM_ID=%s
                """, [
                    row["REQ_ID"], row["PICK_ID"], row["INVOICE_NUMBER"],
                    row["UNDEL_ID"], row["INVENTORY_ITEM_ID"]
                ])
                fetch = cursor.fetchone()
                assigned_count = fetch[0] if fetch else 0

                cursor.execute("""
                    SELECT COUNT(*) 
                    FROM WHR_PICKED_MAN
                    WHERE REQ_ID=%s AND PICK_ID=%s AND INVOICE_NUMBER=%s AND UNDEL_ID=%s AND INVENTORY_ITEM_ID=%s
                """, [
                    row["REQ_ID"], row["PICK_ID"], row["INVOICE_NUMBER"],
                    row["UNDEL_ID"], row["INVENTORY_ITEM_ID"]
                ])
                fetch2 = cursor.fetchone()
                already_picked = fetch2[0] if fetch2 else 0

            allowed_to_insert = max(0, int(assigned_count) - int(already_picked))
            final_to_insert = min(rowcount_requested, allowed_to_insert)

            logger.debug(
                "assigned=%s picked=%s allowed=%s requested=%s final=%s",
                assigned_count, already_picked, allowed_to_insert,
                rowcount_requested, final_to_insert
            )

            if final_to_insert <= 0:
                return Response({
                    "message": "Skip - Assigned count reached",
                    "assigned_count": assigned_count,
                    "already_picked": already_picked,
                    "rowcount_requested": rowcount_requested,
                    "allowed_to_insert": allowed_to_insert,
                    "final_inserted": 0
                }, status=200)

            # -------- INSERT SQL --------
            insert_sql = """
                INSERT INTO WHR_PICKED_MAN (
                    PICK_ID, REQ_ID, [DATE], ASSIGN_PICKMAN, PHYSICAL_WAREHOUSE,
                    ORG_ID, ORG_NAME, SALESMAN_NO, SALESMAN_NAME, MANAGER_NO,
                    MANAGER_NAME, PICKMAN_NO, PICKMAN_NAME, CUSTOMER_NUMBER,
                    CUSTOMER_NAME, CUSTOMER_SITE_ID, INVOICE_DATE, INVOICE_NUMBER,
                    LINE_NUMBER, CUSTOMER_TRX_ID, CUSTOMER_TRX_LINE_ID,
                    INVENTORY_ITEM_ID, ITEM_DESCRIPTION, TOT_QUANTITY,
                    DISPATCHED_QTY, BALANCE_QTY, PICKED_QTY, PRODUCT_CODE,
                    SERIAL_NO, CREATION_DATE, CREATED_BY, CREATED_IP,
                    CREATED_MAC, LAST_UPDATE_DATE, LAST_UPDATED_BY,
                    LAST_UPDATE_IP, FLAG, UNDEL_ID
                ) VALUES (
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,%s,%s,%s
                )
            """

            params = [
                row.get("PICK_ID"), row.get("REQ_ID"), row.get("DATE"),
                row.get("ASSIGN_PICKMAN"), row.get("PHYSICAL_WAREHOUSE"),
                row.get("ORG_ID"), row.get("ORG_NAME"), row.get("SALESMAN_NO"),
                row.get("SALESMAN_NAME"), row.get("MANAGER_NO"),
                row.get("MANAGER_NAME"), row.get("PICKMAN_NO"), row.get("PICKMAN_NAME"),
                row.get("CUSTOMER_NUMBER"), row.get("CUSTOMER_NAME"), row.get("CUSTOMER_SITE_ID"),
                row.get("INVOICE_DATE"), row.get("INVOICE_NUMBER"), row.get("LINE_NUMBER"),
                row.get("CUSTOMER_TRX_ID"), row.get("CUSTOMER_TRX_LINE_ID"),
                row.get("INVENTORY_ITEM_ID"), row.get("ITEM_DESCRIPTION"),
                row.get("TOT_QUANTITY"), row.get("DISPATCHED_QTY"), row.get("BALANCE_QTY"),
                1, row.get("PRODUCT_CODE"), row.get("SERIAL_NO"), row.get("CREATION_DATE"),
                row.get("CREATED_BY"), row.get("CREATED_IP"), row.get("CREATED_MAC"),
                row.get("LAST_UPDATE_DATE"), row.get("LAST_UPDATED_BY"),
                row.get("LAST_UPDATE_IP"), row.get("FLAG"), row.get("UNDEL_ID")
            ]

            # -------- INSERT Rows + On Commit Trigger --------
            with transaction.atomic():
                with connection.cursor() as cursor:
                    for _ in range(final_to_insert):
                        cursor.execute(insert_sql, params)

                # Call update ONLY after successful insert
                def run_update():
                    try:
                        UpdateDispatchQtyView.update_qty(
                            row["REQ_ID"], row["PICK_ID"], row["UNDEL_ID"],
                            row["INVOICE_NUMBER"], row["INVENTORY_ITEM_ID"]
                        )
                        logger.info("UpdateDispatchQtyView.update_qty ran successfully")
                    except Exception as e:
                        logger.exception("Error running update_qty: %s", e)

                transaction.on_commit(run_update)

            return Response({
                "message": f"{final_to_insert} rows inserted successfully",
                "assigned_count": assigned_count,
                "already_picked": already_picked,
                "rowcount_requested": rowcount_requested,
                "allowed_to_insert": allowed_to_insert,
                "final_inserted": final_to_insert
            }, status=201)

        except Exception as e:
            logger.exception("Error in post: %s", e)
            return Response({"error": str(e)}, status=400)

# class NewInsertPickedManAssignData(APIView):
#     def post(self, request):
#         try:
#             data = request.data
#             rows = data.get("rows", [])
#             try:
#                 rowcount_requested = int(data.get("row_count", 0))
#             except (TypeError, ValueError):
#                 return Response({"error": "Invalid row_count; must be integer"}, status=400)

#             if not rows or rowcount_requested <= 0:
#                 return Response({"error": "No rows or valid row_count provided"}, status=400)

#             row = rows[0]
#             if not isinstance(row, dict):
#                 return Response({"error": "Invalid row format; expected a dict"}, status=400)

#             # --- Parse ISO datetimes safely (naive is fine for SQL Server via pyodbc) ---
#             def parse_dt(val):
#                 if not val:
#                     return None
#                 if isinstance(val, datetime):
#                     return val
#                 s = str(val)
#                 # strip trailing Z / microseconds for fromisoformat()
#                 s = s.replace("Z", "")
#                 if "." in s:
#                     s = s.split(".")[0]
#                 try:
#                     return datetime.fromisoformat(s)
#                 except Exception:
#                     # Try common fallback formats
#                     for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
#                         try:
#                             return datetime.strptime(s, fmt)
#                         except Exception:
#                             continue
#                     # if cannot parse, return None and log
#                     logger.warning("Unable to parse datetime: %s", val)
#                     return None

#             row["DATE"] = parse_dt(row.get("DATE"))
#             row["INVOICE_DATE"] = parse_dt(row.get("INVOICE_DATE"))
#             row["CREATION_DATE"] = parse_dt(row.get("CREATION_DATE"))
#             row["LAST_UPDATE_DATE"] = parse_dt(row.get("LAST_UPDATE_DATE"))

#             # --- Required fields for the count checks ---
#             required = ["PICK_ID", "REQ_ID", "INVOICE_NUMBER", "UNDEL_ID", "INVENTORY_ITEM_ID"]
#             if not all(row.get(field) for field in required):
#                 return Response({"error": f"Missing required fields. Required: {required}"}, status=400)

#             # --- Get assigned and already picked counts ---
#             with connection.cursor() as cursor:
#                 cursor.execute(
#                     """
#                     SELECT ISNULL(PICKED_QTY, 0)
#                     FROM WHR_DISPATCH_REQUEST
#                     WHERE REQ_ID=%s AND PICK_ID=%s AND INVOICE_NUMBER=%s AND UNDEL_ID=%s AND INVENTORY_ITEM_ID=%s 
#                     """,
#                     [row["REQ_ID"], row["PICK_ID"], row["INVOICE_NUMBER"], row["UNDEL_ID"], row["INVENTORY_ITEM_ID"]],
#                 )
#                 fetch = cursor.fetchone()
#                 assigned_count = (fetch[0] if fetch and fetch[0] is not None else 0)

#                 cursor.execute(
#                     """
#                     SELECT COUNT(*)
#                     FROM WHR_PICKED_MAN
#                     WHERE REQ_ID=%s AND PICK_ID=%s AND INVOICE_NUMBER=%s AND UNDEL_ID=%s AND INVENTORY_ITEM_ID=%s
#                     """,
#                     [row["REQ_ID"], row["PICK_ID"], row["INVOICE_NUMBER"], row["UNDEL_ID"], row["INVENTORY_ITEM_ID"]],
#                 )
#                 fetch2 = cursor.fetchone()
#                 already_picked = (fetch2[0] if fetch2 and fetch2[0] is not None else 0)

#             # --- Core rule ---
#             allowed_to_insert = max(0, int(assigned_count) - int(already_picked))
#             final_to_insert = min(rowcount_requested, allowed_to_insert)

#             # quick debug logs
#             logger.debug("assigned_count=%s already_picked=%s allowed_to_insert=%s requested=%s final=%s",
#                          assigned_count, already_picked, allowed_to_insert, rowcount_requested, final_to_insert)

#             if final_to_insert <= 0:
#                 return Response({
#                     "message": "Skip - Assigned count reached",
#                     "assigned_count": assigned_count,
#                     "already_picked": already_picked,
#                     "rowcount_requested": rowcount_requested,
#                     "allowed_to_insert": allowed_to_insert,
#                     "final_inserted": 0
#                 }, status=200)

#             # --- Build INSERT with exactly 38 placeholders (one per column) ---
#             insert_sql = """
#                 INSERT INTO WHR_PICKED_MAN (
#                     PICK_ID, REQ_ID, [DATE], ASSIGN_PICKMAN, PHYSICAL_WAREHOUSE,
#                     ORG_ID, ORG_NAME, SALESMAN_NO, SALESMAN_NAME, MANAGER_NO,
#                     MANAGER_NAME, PICKMAN_NO, PICKMAN_NAME, CUSTOMER_NUMBER,
#                     CUSTOMER_NAME, CUSTOMER_SITE_ID, INVOICE_DATE, INVOICE_NUMBER,
#                     LINE_NUMBER, CUSTOMER_TRX_ID, CUSTOMER_TRX_LINE_ID,
#                     INVENTORY_ITEM_ID, ITEM_DESCRIPTION, TOT_QUANTITY,
#                     DISPATCHED_QTY, BALANCE_QTY, PICKED_QTY, PRODUCT_CODE,
#                     SERIAL_NO, CREATION_DATE, CREATED_BY, CREATED_IP,
#                     CREATED_MAC, LAST_UPDATE_DATE, LAST_UPDATED_BY,
#                     LAST_UPDATE_IP, FLAG, UNDEL_ID
#                 ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
#             """

#             params = [
#                 row.get("PICK_ID"), row.get("REQ_ID"), row.get("DATE"), row.get("ASSIGN_PICKMAN"), row.get("PHYSICAL_WAREHOUSE"),
#                 row.get("ORG_ID"), row.get("ORG_NAME"), row.get("SALESMAN_NO"), row.get("SALESMAN_NAME"), row.get("MANAGER_NO"),
#                 row.get("MANAGER_NAME"), row.get("PICKMAN_NO"), row.get("PICKMAN_NAME"), row.get("CUSTOMER_NUMBER"), row.get("CUSTOMER_NAME"),
#                 row.get("CUSTOMER_SITE_ID"), row.get("INVOICE_DATE"), row.get("INVOICE_NUMBER"), row.get("LINE_NUMBER"), row.get("CUSTOMER_TRX_ID"),
#                 row.get("CUSTOMER_TRX_LINE_ID"), row.get("INVENTORY_ITEM_ID"), row.get("ITEM_DESCRIPTION"), row.get("TOT_QUANTITY"), row.get("DISPATCHED_QTY"),
#                 row.get("BALANCE_QTY"), 1, row.get("PRODUCT_CODE"), row.get("SERIAL_NO"), row.get("CREATION_DATE"),
#                 row.get("CREATED_BY"), row.get("CREATED_IP"), row.get("CREATED_MAC"), row.get("LAST_UPDATE_DATE"), row.get("LAST_UPDATED_BY"),
#                 row.get("LAST_UPDATE_IP"), row.get("FLAG"), row.get("UNDEL_ID")
#             ]

#             # --- Insert final_to_insert rows only, and schedule update after commit ---
#             with transaction.atomic():
#                 with connection.cursor() as cursor:
#                     for _ in range(final_to_insert):
#                         cursor.execute(insert_sql, params)

#                 # Schedule the update call to run only after transaction commits.
#                 # This prevents the update routine from seeing uncommitted data.
#                 def call_update_after_commit():
#                     update_result = None
#                     try:
#                         # Try calling as a classmethod / staticmethod first
#                         try:
#                             update_result = UpdateDispatchQtyView.update_qty(
#                                 row.get("REQ_ID"),
#                                 row.get("PICK_ID"),
#                                 row.get("UNDEL_ID"),
#                                 row.get("INVOICE_NUMBER"),
#                                 row.get("INVENTORY_ITEM_ID")
#                             )
#                             logger.debug("Called UpdateDispatchQtyView.update_qty as class/static method; result=%s", update_result)
#                             return
#                         except TypeError:
#                             # update_qty probably expects 'self' or is an instance method; fallthrough to instance approach
#                             logger.debug("UpdateDispatchQtyView.update_qty not callable as class/staticmethod; will instantiate.")

#                         # Try instantiating the view and calling the method
#                         try:
#                             view_instance = UpdateDispatchQtyView()
#                             # If update_qty is defined to accept the instance and same params
#                             update_result = view_instance.update_qty(
#                                 row.get("REQ_ID"),
#                                 row.get("PICK_ID"),
#                                 row.get("UNDEL_ID"),
#                                 row.get("INVOICE_NUMBER"),
#                                 row.get("INVENTORY_ITEM_ID")
#                             )
#                             logger.debug("Called UpdateDispatchQtyView.update_qty on instance; result=%s", update_result)
#                             return
#                         except Exception as e:
#                             logger.exception("Instance call to UpdateDispatchQtyView.update_qty failed: %s", e)

#                         # As a last resort, if update_qty is implemented as a plain function in module, call it from module namespace:
#                         try:
#                             # import module where UpdateDispatchQtyView lives and see if there's a helper function
#                             # e.g., UpdateDispatchQtyView._update_qty_helper or similar - adapt if you have a helper
#                             if hasattr(UpdateDispatchQtyView, "_update_qty"):
#                                 update_result = getattr(UpdateDispatchQtyView, "_update_qty")(
#                                     row.get("REQ_ID"),
#                                     row.get("PICK_ID"),
#                                     row.get("UNDEL_ID"),
#                                     row.get("INVOICE_NUMBER"),
#                                     row.get("INVENTORY_ITEM_ID")
#                                 )
#                                 logger.debug("Called UpdateDispatchQtyView._update_qty fallback; result=%s", update_result)
#                                 return
#                         except Exception as e:
#                             logger.exception("Fallback update call failed: %s", e)

#                         logger.error("Unable to call UpdateDispatchQtyView.update_qty by any known method.")
#                     except Exception as exc:
#                         logger.exception("Unexpected error while attempting to call UpdateDispatchQtyView.update_qty: %s", exc)

#                 transaction.on_commit(call_update_after_commit)

#             # If caller wants sync confirmation: we at least scheduled the update call after commit.
#             return Response({
#                 "message": f"{final_to_insert} rows inserted successfully (update scheduled after commit)",
#                 "assigned_count": assigned_count,
#                 "already_picked": already_picked,
#                 "rowcount_requested": rowcount_requested,
#                 "allowed_to_insert": allowed_to_insert,
#                 "final_inserted": final_to_insert
#             }, status=201)

#         except Exception as e:
#             logger.exception("Error in NewInsertPickedManAssignData.post: %s", e)
#             return Response({"error": str(e)}, status=400)

class InsertSaved_Truck_scan_AssignData(APIView):
    def post(self, request):
        data = request.data
        rows = data.get("rows", [])
        row_count = data.get("row_count", 0)  # Number of rows expected (could be 1)
        total_send_qty = data.get("total_send_qty", 1)  # Number of times to insert each row

        if not rows or not isinstance(rows, list):
            return Response({"error": "Rows must be a list of data maps."}, status=status.HTTP_400_BAD_REQUEST)

        columns = [
            "dispatch_id", "req_no", "pick_id", "salesman_no", "salesman_name",
            "manager_no", "manager_name", "pickman_no", "pickman_name",
            "Customer_no", "Customer_name", "Customer_Site", "invoice_no",
            "Customer_trx_id", "Customer_trx_line_id", "line_no", "Item_code",
            "Item_detailas", "DisReq_Qty", "Send_qty", "Product_code",
            "Serial_No", "Udel_id", "SCAN_STATUS"
        ]

        insert_sql = f"""
            INSERT INTO [BUYP].[dbo].[WHR_SAVE_TRUCK_DETAILS_TBL] (
                {', '.join(columns)}
            ) VALUES (
                {', '.join(['%s'] * len(columns))}
            )
        """

        inserted_count = 0
        total_qty_inserted = 0

        with connection.cursor() as cursor:
            for idx, row in enumerate(rows):
                if not isinstance(row, dict):
                    return Response({
                        "error": f"Row {idx + 1} is not a valid mapping (dict)"
                    }, status=status.HTTP_400_BAD_REQUEST)

                # Insert the row total_send_qty times
                for i in range(int(total_send_qty)):
                    try:
                        # Insert with Send_qty = 1 per row (you can keep original if you want)
                        # Here, we insert 1 unit per row, total_send_qty times
                        values = []
                        for col in columns:
                            if col == "Send_qty":
                                values.append(1)  # single qty per insert
                            else:
                                values.append(row.get(col, None))

                        cursor.execute(insert_sql, values)
                        inserted_count += 1
                        total_qty_inserted += 1

                    except Exception as e:
                        return Response({
                            "error": f"Insert failed on row {idx + 1} iteration {i + 1}: {str(e)}",
                            "failed_row_data": row
                        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        return Response({
            "message": f"Rows inserted successfully with replication",
            "expected_row_count": row_count,
            "actual_inserted": inserted_count,
            "total_qty_inserted": total_qty_inserted
        }, status=status.HTTP_201_CREATED)

@csrf_exempt
def update_reassign_status(request, return_dispatch_id):
    if request.method == 'POST':
        try:
            data = json.loads(request.body.decode('utf-8'))
            new_status = data.get('RE_ASSIGN_STATUS')

            if new_status is None:
                return JsonResponse({'status': 'error', 'message': 'Missing RE_ASSIGN_STATUS in request body'}, status=400)

            with connection.cursor() as cursor:
                update_query = """
                    UPDATE BUYP.dbo.WHR_RETURN_DISPATCH
                    SET RE_ASSIGN_STATUS = %s
                    WHERE RETURN_DIS_ID = %s
                """
                cursor.execute(update_query, [new_status, return_dispatch_id])

            return JsonResponse({'status': 'success', 'message': f'RE_ASSIGN_STATUS updated for RETURN_DIS_ID {return_dispatch_id}'})

        except Exception as e:
            return JsonResponse({'status': 'error', 'message': str(e)}, status=500)

    return JsonResponse({'status': 'error', 'message': 'Only POST method allowed'}, status=405)

class HighestPickIDView(APIView):
    def get(self, request, *args, **kwargs):
        highest_pick = WHRDispatchRequest.objects.filter(PICK_ID__isnull=False).order_by('-id').first()        
        if highest_pick:
            pick_id = highest_pick.PICK_ID
        else:
            pick_id = "0"        
        return Response({'PICK_ID': pick_id})
    
class ViewPickiddetailsView(viewsets.ModelViewSet):
    serializer_class = FilteredpickformSerializer
    pagination_class = StandardResultsSetPagination

    def get_queryset(self):
        pick_id = self.kwargs.get('PICK_ID', None)  
        if pick_id is not None:
            return WHRDispatchRequest.objects.filter(PICK_ID=pick_id)  # Filter by 'PICK_ID'
        return WHRDispatchRequest.objects.none()  # Return empty if no PICK_ID is provided

    def list(self, request, *args, **kwargs):
        queryset = self.get_queryset()

        # Grouping the results
        result = {}
        for record in queryset:
            if record.PICK_ID not in result:  # Use 'PICK_ID' instead of 'pick_id'
                result[record.PICK_ID] = {  # Use 'PICK_ID' instead of 'pick_id'
                    "PICK_ID": record.PICK_ID,
                    "ASS_PICKMAN": record.ASSIGN_PICKMAN,
                    "REQ_ID": record.REQ_ID,
                    "TO_WAREHOUSE": record.PHYSICAL_WAREHOUSE,
                    "ORG_ID": record.ORG_ID,
                    "ORG_NAME": record.ORG_NAME,
                    "SALESMAN_NO": record.SALESMAN_NO,
                    "SALESMAN_NAME": record.SALESMAN_NAME,                    
                    "MANAGER_NO": record.MANAGER_NO,
                    "MANAGER_NAME": record.MANAGER_NAME,
                    "CUSTOMER_NUMBER": record.CUSTOMER_NUMBER,
                    "CUSTOMER_NAME": record.CUSTOMER_NAME,
                    "CUSTOMER_SITE_ID": record.CUSTOMER_SITE_ID,
                    "INVOICE_DATE": record.INVOICE_DATE,
                    "INVOICE_NUMBER": record.INVOICE_NUMBER,
                    "CREATION_DATE":record.CREATION_DATE,
                    "TABLE_DETAILS": []
                }

            # Add detail to TABLE_DETAILS
            detail = {
                "ID": record.id,
                "INVOICE_NUMBER": record.INVOICE_NUMBER,
                "CUSTOMER_TRX_ID": record.CUSTOMER_TRX_ID,
                "CUSTOMER_TRX_LINE_ID": record.CUSTOMER_TRX_LINE_ID,               
                "LINE_NUMBER": record.LINE_NUMBER,
                "INVENTORY_ITEM_ID": record.INVENTORY_ITEM_ID,                
                "ITEM_DESCRIPTION": record.ITEM_DESCRIPTION,
                "TOT_QUANTITY": record.TOT_QUANTITY,
                "DISPATCHED_QTY": record.DISPATCHED_QTY,
                "PICKED_QTY": record.PICKED_QTY,
                "BALANCE_QTY": record.BALANCE_QTY,           
                "SCANNED_QTY": record.SCANNED_QTY,
                "STATUS": record.STATUS
            }
            result[record.PICK_ID]["TABLE_DETAILS"].append(detail)  # Use 'PICK_ID' instead of 'pick_id'

        # Serialize and return the grouped results
        return Response(list(result.values()))

# class Filtered_PickscanView(viewsets.ModelViewSet):
#     serializer_class = FilteredpickformSerializer
#     pagination_class = StandardResultsSetPagination
#     http_method_names = ['get']  # Restrict to GET requests only

#     def get_queryset(self):
#         pick_id = self.kwargs.get("PICK_ID", None)
#         req_id = self.kwargs.get("REQ_ID", None)

#         print(f"Received PICK_ID: {pick_id}")
#         print(f"Received REQ_ID: {req_id}")

#         if pick_id and req_id:
#             queryset = WHRDispatchRequest.objects.filter(PICK_ID=pick_id, REQ_ID=req_id)
#             print(f"Filtered QuerySet: {queryset}")
#             return queryset
        
#         return WHRDispatchRequest.objects.none()

#     def list(self, request, *args, **kwargs):
#         queryset = self.get_queryset()
#         result = {}

#         print("All Data in WHRDispatchRequest:")
#         for record in WHRDispatchRequest.objects.all():
#             print(record)

#         for record in queryset:
#             if record.PICK_ID not in result:
#                 result[record.PICK_ID] = {
#                     "PICK_ID": record.PICK_ID,
#                     "ASS_PICKMAN": record.ASSIGN_PICKMAN,
#                     "REQ_ID": record.REQ_ID,
#                     "TO_WAREHOUSE": record.PHYSICAL_WAREHOUSE,
#                     "ORG_ID": record.ORG_ID,
#                     "ORG_NAME": record.ORG_NAME,
#                     "SALESMAN_NO": record.SALESMAN_NO,
#                     "SALESMAN_NAME": record.SALESMAN_NAME,
#                     "MANAGER_NO": record.MANAGER_NO,
#                     "MANAGER_NAME": record.MANAGER_NAME,
#                     "CUSTOMER_NUMBER": record.CUSTOMER_NUMBER,
#                     "CUSTOMER_NAME": record.CUSTOMER_NAME,
#                     "CUSTOMER_SITE_ID": record.CUSTOMER_SITE_ID,
#                     "INVOICE_DATE": record.INVOICE_DATE,
#                     "INVOICE_NUMBER": record.INVOICE_NUMBER,
#                     "TABLE_DETAILS": []
#                 }

#             # Add detail to TABLE_DETAILS
#             detail = {
#                 "ID": record.id,
                
#                 "UNDEL_ID": record.UNDEL_ID,
#                 "INVOICE_NUMBER": record.INVOICE_NUMBER,
#                 "CUSTOMER_TRX_ID": record.CUSTOMER_TRX_ID,
#                 "CUSTOMER_TRX_LINE_ID": record.CUSTOMER_TRX_LINE_ID,
#                 "LINE_NUMBER": record.LINE_NUMBER,
#                 "INVENTORY_ITEM_ID": record.INVENTORY_ITEM_ID,
#                 "ITEM_DESCRIPTION": record.ITEM_DESCRIPTION,
#                 "TOT_QUANTITY": record.TOT_QUANTITY,
#                 "DISPATCHED_QTY": record.DISPATCHED_QTY,
#                 "PICKED_QTY": record.PICKED_QTY,
#                 "BALANCE_QTY": record.BALANCE_QTY,
#                 "SCANNED_QTY": record.SCANNED_QTY,
#                 "STATUS": record.STATUS
#             }
#             result[record.PICK_ID]["TABLE_DETAILS"].append(detail)

#         print("Final JSON Response:")
#         print(result)
        
#         return Response(list(result.values()))




class Filtered_PickscanView(viewsets.ViewSet):   # no need for ModelViewSet
    http_method_names = ['get']  # GET only

    def list(self, request, *args, **kwargs):
        # 👉 DRF passes kwargs through `self.kwargs`
        pick_id = self.kwargs.get("PICK_ID")
        req_id = self.kwargs.get("REQ_ID")

        if not pick_id or not req_id:
            return Response([], status=200)

        # ✅ Query only required fields (faster than full model fetch)
        queryset = (
            WHRDispatchRequest.objects.filter(PICK_ID=pick_id, REQ_ID=req_id)
            .values(
                "id", "PICK_ID", "REQ_ID", "ASSIGN_PICKMAN", "PHYSICAL_WAREHOUSE",
                "ORG_ID", "ORG_NAME", "SALESMAN_NO", "SALESMAN_NAME",
                "MANAGER_NO", "MANAGER_NAME", "CUSTOMER_NUMBER", "CUSTOMER_NAME",
                "CUSTOMER_SITE_ID", "INVOICE_DATE", "INVOICE_NUMBER",
                "UNDEL_ID", "CUSTOMER_TRX_ID", "CUSTOMER_TRX_LINE_ID",
                "LINE_NUMBER", "INVENTORY_ITEM_ID", "ITEM_DESCRIPTION",
                "TOT_QUANTITY", "DISPATCHED_QTY", "PICKED_QTY", "BALANCE_QTY",
                "SCANNED_QTY", "STATUS"
            )
        )

        if not queryset.exists():
            return Response([], status=200)

        # ✅ Group by PICK_ID
        queryset = sorted(queryset, key=itemgetter("PICK_ID"))
        result = []
        for pick_id, records in groupby(queryset, key=itemgetter("PICK_ID")):
            records = list(records)
            header = records[0]

            result.append({
                "PICK_ID": header["PICK_ID"],
                "ASS_PICKMAN": header["ASSIGN_PICKMAN"],
                "REQ_ID": header["REQ_ID"],
                "TO_WAREHOUSE": header["PHYSICAL_WAREHOUSE"],
                "ORG_ID": header["ORG_ID"],
                "ORG_NAME": header["ORG_NAME"],
                "SALESMAN_NO": header["SALESMAN_NO"],
                "SALESMAN_NAME": header["SALESMAN_NAME"],
                "MANAGER_NO": header["MANAGER_NO"],
                "MANAGER_NAME": header["MANAGER_NAME"],
                "CUSTOMER_NUMBER": header["CUSTOMER_NUMBER"],
                "CUSTOMER_NAME": header["CUSTOMER_NAME"],
                "CUSTOMER_SITE_ID": header["CUSTOMER_SITE_ID"],
                "INVOICE_DATE": header["INVOICE_DATE"],
                "INVOICE_NUMBER": header["INVOICE_NUMBER"],
                "TABLE_DETAILS": [
                    {
                        "ID": r["id"],
                        "UNDEL_ID": r["UNDEL_ID"],
                        "INVOICE_NUMBER": r["INVOICE_NUMBER"],
                        "CUSTOMER_TRX_ID": r["CUSTOMER_TRX_ID"],
                        "CUSTOMER_TRX_LINE_ID": r["CUSTOMER_TRX_LINE_ID"],
                        "LINE_NUMBER": r["LINE_NUMBER"],
                        "INVENTORY_ITEM_ID": r["INVENTORY_ITEM_ID"],
                        "ITEM_DESCRIPTION": r["ITEM_DESCRIPTION"],
                        "TOT_QUANTITY": r["TOT_QUANTITY"],
                        "DISPATCHED_QTY": r["DISPATCHED_QTY"],
                        "PICKED_QTY": r["PICKED_QTY"],
                        "BALANCE_QTY": r["BALANCE_QTY"],
                        "SCANNED_QTY": r["SCANNED_QTY"],
                        "STATUS": r["STATUS"],
                    }
                    for r in records
                ],
            })

        return Response(result, status=200)

class filteredProductcodeGetView(viewsets.ViewSet):
    """
    A viewset that retrieves products based on itemcode, handling SERIAL_STATUS and PRODUCT_BARCODE rules.
    """

    def list(self, request, itemcode=None , DESCRIPTION=None):
        # Filter by ITEM_CODE
        queryset = ProductcodeGetModels.objects.filter(ITEM_CODE=itemcode, DESCRIPTION=DESCRIPTION)

        # If nothing found
        if not queryset.exists():
            return Response({'message': 'No products found for the given itemcode.'}, status=404)

        # Check for bypass conditions
        bypass_items = queryset.filter(
            (
                (Q(PRODUCT_BARCODE='00') & Q(SERIAL_STATUS='N')) |
                (Q(PRODUCT_BARCODE='0') & Q(SERIAL_STATUS='N')) |
                (Q(PRODUCT_BARCODE__isnull=True) & Q(SERIAL_STATUS__isnull=True))
            )
        )

        if bypass_items.exists():
            return Response({'message': 'Bypass: Scanning not required for this item.'}, status=200)

        # Items with SERIAL_STATUS = 'Y'
        enabled_products = queryset.filter(SERIAL_STATUS__in=['Y', 'N'])
        if not enabled_products.exists():
            return Response({'message': 'This item code cannot be scanned because SERIAL_STATUS is not "Y".'}, status=403)

        # Serialize and return
        serializer = ProductcodeGetserializers(enabled_products, many=True)
        return Response(serializer.data)
    
class Pickman_scanView(viewsets.ModelViewSet):
    queryset = Pickman_ScanModels.objects.all()
    serializer_class = Pickman_ScanModelsserializers
    pagination_class = StandardResultsSetPagination
 
class Scanned_PickmanView(viewsets.ModelViewSet):
    serializer_class = Pickman_ScanModelsserializers

    def get_queryset(self):
        queryset = Pickman_ScanModels.objects.all()
        
        # Retrieve query parameters
        pick_id = self.request.query_params.get('PICK_ID')
        req_id = self.request.query_params.get('REQ_ID')
        invoice_number = self.request.query_params.get('INVOICE_NUMBER')
        inventory_item_id = self.request.query_params.get('INVENTORY_ITEM_ID')
        
        # Apply filters if parameters are provided
        if pick_id:
            queryset = queryset.filter(PICK_ID=pick_id)
        if req_id:
            queryset = queryset.filter(REQ_ID=req_id)
        if invoice_number:
            queryset = queryset.filter(INVOICE_NUMBER=invoice_number)
        if inventory_item_id:
            queryset = queryset.filter(INVENTORY_ITEM_ID=inventory_item_id)
        
        return queryset

class Pickman_Productcode(viewsets.ModelViewSet):
    serializer_class = Pickman_ScanModelsserializers

    def get_queryset(self):
        # Get URL parameters from kwargs
        product_code = self.kwargs.get('productcode', None)
        serial_no = self.kwargs.get('serialno', None)
        req_no = self.kwargs.get('reqno', None)

        # Base queryset
        queryset = Pickman_ScanModels.objects.all()

        # Apply filters based on availability
        if product_code and serial_no and req_no:
            queryset = queryset.filter(
                PRODUCT_CODE=product_code,
                SERIAL_NO=serial_no,
                REQ_ID=req_no
            ).exclude(FLAG__in=['R', 'SR'])

        return queryset

    def list(self, request, *args, **kwargs):
        queryset = self.get_queryset()
        serializer = self.get_serializer(queryset, many=True)
        return Response(serializer.data, status=status.HTTP_200_OK)

class Truck_scanView(viewsets.ModelViewSet):
    queryset = Truck_scanModels.objects.all()
    serializer_class = Truck_scanserializers
    pagination_class = StandardResultsSetPagination

    def get_queryset(self):
        """
        Optionally restricts the returned queryset by filtering against
        'REQ_NO' and 'PICK_ID' query parameters in the URL.
        """
        queryset = super().get_queryset()

        req_no = self.request.query_params.get('REQ_ID', None)
        pick_id = self.request.query_params.get('PICK_ID', None)

        if req_no is not None:
            queryset = queryset.filter(REQ_ID=req_no)

        if pick_id is not None:
            queryset = queryset.filter(PICK_ID=pick_id)

        return queryset

@csrf_exempt
def update_truck_flag(request,id):
    try:
        with connection.cursor() as cursor:
            cursor.execute("UPDATE [BUYP].[dbo].[WHR_TRUCK_SCAN_DETAILS] SET FLAG = 'R' WHERE id = %s", [id])
        return JsonResponse({'status': 'success', 'message': f'FLAG updated to R for ID {id}'})
    except Exception as e:
        return JsonResponse({'status': 'error', 'message': str(e)})

@csrf_exempt
def update_pickman_flag(request, reqno, pickid, invoiceno, totalProductCodeCount, productcode, serialno):
    try:
        total_to_update = int(totalProductCodeCount)

        with connection.cursor() as cursor:
            query = f"""
                WITH cte AS (
                    SELECT TOP ({total_to_update})
                        *
                    FROM [BUYP].[dbo].[WHR_PICKED_MAN]
                    WHERE REQ_ID = %s
                      AND PICK_ID = %s
                      AND INVOICE_NUMBER = %s
                      AND PRODUCT_CODE = %s
                      AND SERIAL_NO = %s
                      AND FLAG != 'R'
                )
                UPDATE cte SET FLAG = 'R';
            """

            cursor.execute(query, [reqno, pickid, invoiceno, productcode, serialno])

        return JsonResponse({
            'status': 'success',
            'message': f'FLAG updated to R for up to {total_to_update} rows.'
        })

    except Exception as e:
        return JsonResponse({'status': 'error', 'message': str(e)})

class Truck_scan_DispatchNoView(APIView):
    def get(self, request, *args, **kwargs):
        highest_dispatchno = Truck_scanModels.objects.filter(DISPATCH_ID__isnull=False).order_by('-id').first()        
        if highest_dispatchno:
            DISPATCH_ID = highest_dispatchno.DISPATCH_ID
        else:
            DISPATCH_ID = "0"        
        return Response({'DISPATCH_ID': DISPATCH_ID})

class ToGetGenerateDispatchView(viewsets.ModelViewSet):
    queryset = ToGetGenerateDispatch.objects.all()
    serializer_class = ToGetGenerateDispatchserializers
    pagination_class = StandardResultsSetPagination

    def get_queryset(self):
        """
        Filter queryset based on URL parameters.
        """
        queryset = super().get_queryset()

        # Extract URL parameters
        req_no = self.kwargs.get('req_no')
        # pick_id = self.kwargs.get('pick_id')
        Customer_no = self.kwargs.get('Customer_no')
        Customer_name = self.kwargs.get('Customer_name')
        Customer_Site = self.kwargs.get('Customer_Site')
      
        # Apply filters based on the provided parameters
        if req_no:
            queryset = queryset.filter(req_no=req_no)
        # if pick_id:
        #     queryset = queryset.filter(pick_id=pick_id)
        if Customer_no:
            queryset = queryset.filter(Customer_no=Customer_no)
        if Customer_name:
            queryset = queryset.filter(Customer_name=Customer_name)
        if Customer_Site:
            queryset = queryset.filter(Customer_Site=Customer_Site)
        if Customer_Site:
            queryset = queryset.filter(Customer_Site=Customer_Site)
     
        return queryset

    def retrieve(self, request, *args, **kwargs):
        """
        Custom retrieve method to handle filtered data.
        """
        instance = self.get_queryset()
        serializer = self.get_serializer(instance, many=True)
        return Response(serializer.data)

@method_decorator(csrf_exempt, name='dispatch')
class UpdateScanStatusView(View):
    def post(self, request, *args, **kwargs):
        return self.handle_request(request, *args, **kwargs)
        
    def get(self, request, *args, **kwargs):
        return self.handle_request(request, *args, **kwargs)
    
    def handle_request(self, request, req_no, pick_id, customer_no, customer_site, new_status):
        try:
            with connection.cursor() as cursor:
                sql = """
                UPDATE [BUYP].[dbo].[WHR_SAVE_TRUCK_DETAILS_TBL]
                SET [SCAN_STATUS] = %s
                WHERE [req_no] = %s 
                AND [pick_id] = %s 
                AND [Customer_no] = %s 
                AND [Customer_Site] = %s
                AND [Product_code] != 'empty'
                AND [Serial_No] != 'empty'
                """
                cursor.execute(sql, [new_status, req_no, pick_id, customer_no, customer_site])
                
                row_count = cursor.rowcount
                
                if row_count == 0:
                    return JsonResponse({
                        'message': 'No records found matching the criteria',
                        'updated': False
                    }, status=404)
                
                return JsonResponse({
                    'message': f'Successfully updated {row_count} record(s)',
                    'updated': True,
                    'req_no': req_no,
                    'pick_id':pick_id,
                    'customer_no': customer_no,
                    'customer_site': customer_site,
                    'new_status': new_status
                })
                
        except Exception as e:
            return JsonResponse({'error': str(e)}, status=500)
        
# # Function to execute the raw SQL query
def get_dispatch_requests():
    with connection.cursor() as cursor:
        query = """
        SELECT REQ_ID, 
               PHYSICAL_WAREHOUSE, 
               ORG_ID, 
               ORG_NAME, 
               INVOICE_NUMBER,
               CUSTOMER_NUMBER,
               CUSTOMER_NAME,
               CUSTOMER_SITE_ID,
               SALESMAN_NO, 
               SALESMAN_NAME, 
               SUM(TOT_QUANTITY) AS TOT_QUANTITY, 
               SUM(PICKED_QTY) AS DISPATCHED_QTY, 
               STATUS
        FROM WHR_DISPATCH_REQUEST
        WHERE STATUS = 'Finished'
        GROUP BY REQ_ID, PHYSICAL_WAREHOUSE, ORG_ID, ORG_NAME, INVOICE_NUMBER, CUSTOMER_NUMBER, CUSTOMER_NAME, CUSTOMER_SITE_ID,
                SALESMAN_NO, SALESMAN_NAME, STATUS
        HAVING COUNT(*) = (
            SELECT COUNT(*) 
            FROM WHR_DISPATCH_REQUEST AS inner_table
            WHERE inner_table.REQ_ID = WHR_DISPATCH_REQUEST.REQ_ID
        )
        """
        cursor.execute(query)
        result = cursor.fetchall()
    return result

# Django view to retrieve data using the custom query
@api_view(['GET'])
def dispatch_request_list(request):
    data = get_dispatch_requests()
    
    # You can serialize the result into the desired format
    serialized_data = []
    
    for row in data:
        serialized_data.append({
            "REQ_ID": row[0],                 # REQ_ID
            "PHYSICAL_WAREHOUSE": row[1],           # TO_WAREHOUSE
            "ORG_ID": row[2],                 # ORG_ID
            "ORG_NAME": row[3],               # ORG_NAME
            "INVOICE_NUMBER": row[4],         # INVOICE_NUMBER
            "CUSTOMER_NUMBER": row[5],        # CUSTOMER_NUMBER
            "CUSTOMER_NAME": row[6],          # CUSTOMER_NAME
            "CUSTOMER_SITE_ID": row[7], 
          
            "SALESMAN_NO": row[8],            # SALESMAN_NO
            "SALESMAN_NAME": row[9],          # SALESMAN_NAME
            "TOT_QUANTITY": row[10],           # Aggregated TOT_QUANTITY
            "DISPATCHED_QTY": row[11],         # Aggregated DISPATCHED_QTY
            "STATUS": row[12],                 # STATUS
        })
    
    # Return the serialized data as a JSON response
    return Response(serialized_data)



# Step 1: Fetch pending REQ_ID level aggregated dispatch data
def filtered_pending_get_dispatch_requests():
    with connection.cursor() as cursor:
        query = """
        SELECT REQ_ID, 
               PHYSICAL_WAREHOUSE, 
               ORG_ID, 
               ORG_NAME, 
               INVOICE_NUMBER,
               CUSTOMER_NUMBER,
               CUSTOMER_NAME,
               CUSTOMER_SITE_ID,
               SALESMAN_NO, 
               SALESMAN_NAME, 
               SUM(TOT_QUANTITY) AS TOT_QUANTITY, 
               SUM(PICKED_QTY) AS DISPATCHED_QTY, 
               STATUS
        FROM WHR_DISPATCH_REQUEST
        WHERE STATUS = 'pending' 
          AND REQ_ID IN (
              SELECT REQ_ID 
              FROM WHR_DISPATCH_REQUEST 
              WHERE STATUS = 'pending' AND FLAG != 'OU'
          )
        GROUP BY 
               REQ_ID, 
               PHYSICAL_WAREHOUSE, 
               ORG_ID, 
               ORG_NAME, 
               INVOICE_NUMBER,
               CUSTOMER_NUMBER,
               CUSTOMER_NAME,
               CUSTOMER_SITE_ID,
               SALESMAN_NO, 
               SALESMAN_NAME, 
               STATUS
        ORDER BY REQ_ID ASC;
        """
        cursor.execute(query)
        result = cursor.fetchall()
    return result


# Step 2: Fetch unique PENDING PICK_IDs grouped by REQ_ID
def get_pending_pick_ids_by_req_id():
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT REQ_ID, 
                   STUFF((SELECT DISTINCT ', ' + CAST(PICK_ID AS VARCHAR)
                          FROM WHR_DISPATCH_REQUEST AS inner_tbl
                          WHERE inner_tbl.REQ_ID = outer_tbl.REQ_ID
                            AND inner_tbl.STATUS = 'pending' 
                            AND inner_tbl.FLAG != 'OU'
                          FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, '') AS PICK_IDS
            FROM WHR_DISPATCH_REQUEST AS outer_tbl
            WHERE outer_tbl.STATUS = 'pending' AND outer_tbl.FLAG != 'OU'
            GROUP BY REQ_ID
        """)
        rows = cursor.fetchall()
        return {row[0]: row[1] for row in rows}


# Step 3: API View to combine and return the paginated result
@api_view(['GET'])
def filtered_pending_dispatch_request_list(request):
    data = filtered_pending_get_dispatch_requests()
    pick_ids_map = get_pending_pick_ids_by_req_id()

    serialized_data = []

    for row in data:
        req_id = row[0]
        serialized_data.append({
            "REQ_ID": req_id,
            "PHYSICAL_WAREHOUSE": row[1],
            "ORG_ID": row[2],
            "ORG_NAME": row[3],
            "INVOICE_NUMBER": row[4],
            "CUSTOMER_NUMBER": row[5],
            "CUSTOMER_NAME": row[6],
            "CUSTOMER_SITE_ID": row[7],
            "SALESMAN_NO": row[8],
            "SALESMAN_NAME": row[9],
            "TOT_QUANTITY": row[10],
            "DISPATCHED_QTY": row[11],
            "STATUS": row[12],
            "PICK_IDS": pick_ids_map.get(req_id, "")
        })

    # Apply paginationpagination_class = TruckResultsPagination   # ?? custom unique class
    paginator = TruckResultsPagination()
    result_page = paginator.paginate_queryset(serialized_data, request)
    return paginator.get_paginated_response(result_page)

# Step 1: Fetch REQ_ID level aggregated dispatch data
def filtered_Completed_get_dispatch_requests():
    with connection.cursor() as cursor:
        query = """
        SELECT REQ_ID, 
               PHYSICAL_WAREHOUSE, 
               ORG_ID, 
               ORG_NAME, 
               INVOICE_NUMBER,
               CUSTOMER_NUMBER,
               CUSTOMER_NAME,
               CUSTOMER_SITE_ID,
               SALESMAN_NO, 
               SALESMAN_NAME, 
               SUM(TOT_QUANTITY) AS TOT_QUANTITY, 
               SUM(PICKED_QTY) AS DISPATCHED_QTY, 
               STATUS
        FROM WHR_DISPATCH_REQUEST
        WHERE STATUS = 'Finished' 
          AND REQ_ID IN (
              SELECT REQ_ID 
              FROM WHR_DISPATCH_REQUEST 
              WHERE STATUS = 'Finished' AND FLAG != 'OU'
          )
        GROUP BY 
               REQ_ID, 
               PHYSICAL_WAREHOUSE, 
               ORG_ID, 
               ORG_NAME, 
               INVOICE_NUMBER,
               CUSTOMER_NUMBER,
               CUSTOMER_NAME,
               CUSTOMER_SITE_ID,
               SALESMAN_NO, 
               SALESMAN_NAME, 
               STATUS
        ORDER BY REQ_ID ASC;
        """
        cursor.execute(query)
        result = cursor.fetchall()
    return result

# Step 2: Fetch PICK_IDs grouped by REQ_ID
def get_pick_ids_by_req_id():
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT REQ_ID, 
                   STUFF((
                       SELECT DISTINCT ', ' + CAST(PICK_ID AS VARCHAR)
                       FROM WHR_DISPATCH_REQUEST AS inner_tbl
                       WHERE inner_tbl.REQ_ID = outer_tbl.REQ_ID
                         AND inner_tbl.STATUS = 'Finished' 
                         AND inner_tbl.FLAG != 'OU'
                       FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, '') AS PICK_IDS
            FROM WHR_DISPATCH_REQUEST AS outer_tbl
            WHERE STATUS = 'Finished' AND FLAG != 'OU'
            GROUP BY REQ_ID
        """)
        rows = cursor.fetchall()
        return {row[0]: row[1] for row in rows}

# Django view with pagination
@api_view(['GET'])
def filtered_Completed_dispatch_request_list(request):
    dispatch_data = filtered_Completed_get_dispatch_requests()
    pick_ids_map = get_pick_ids_by_req_id()

    serialized_data = []
    for row in dispatch_data:
        req_id = row[0]
        serialized_data.append({
            "REQ_ID": req_id,
            "PHYSICAL_WAREHOUSE": row[1],
            "ORG_ID": row[2],
            "ORG_NAME": row[3],
            "INVOICE_NUMBER": row[4],
            "CUSTOMER_NUMBER": row[5],
            "CUSTOMER_NAME": row[6],
            "CUSTOMER_SITE_ID": row[7],
            "SALESMAN_NO": row[8],
            "SALESMAN_NAME": row[9],
            "TOT_QUANTITY": row[10],
            "DISPATCHED_QTY": row[11],
            "STATUS": row[12],
            "PICK_IDS": pick_ids_map.get(req_id, "")
        })

    paginator = TruckResultsPagination()
    result_page = paginator.paginate_queryset(serialized_data, request)

    return paginator.get_paginated_response(result_page)

class GetPickmanDetails_PendingView(viewsets.ModelViewSet):
    serializer_class = Dispatch_requestserializers

    def get_queryset(self):
        reqno = self.kwargs['reqno']
        pending = self.kwargs['pending']
        queryset = WHRDispatchRequest.objects.all()

        # Filter based on reqno and status
        if pending.lower() == 'pending':
            queryset = queryset.filter(REQ_ID=reqno, STATUS='pending')
        
        return queryset



class GetPickmanDetails_CompletedView(viewsets.ModelViewSet):
    serializer_class = Dispatch_requestserializers

    def get_queryset(self):
        reqno = self.kwargs['reqno']
        queryset = WHRDispatchRequest.objects.all()

        # Filter based on reqno and status
        queryset = queryset.filter(REQ_ID=reqno, STATUS='Finished')
        
        return queryset

class GetPickmanDetails_FinishedView(viewsets.ModelViewSet):
    serializer_class = Pickman_ScanModelsserializers

    def get_queryset(self):
        # Extract filter parameters from the URL
        reqno = self.kwargs.get('reqno')  # Assuming reqno maps to REQ_ID
        cusno = self.kwargs.get('cusno')  # Assuming cusno maps to CUSTOMER_NUMBER
        cussite = self.kwargs.get('cussite')  # Assuming cussite maps to CUSTOMER_SITE_ID

        # Filter and annotate the queryset to calculate the sum of PICKED_QTY
        queryset = (
            Pickman_ScanModels.objects.filter(
                REQ_ID=reqno,
                CUSTOMER_NUMBER=cusno,
                CUSTOMER_SITE_ID=cussite,
            )
            .values('REQ_ID','PICK_ID', 'CUSTOMER_NUMBER','CUSTOMER_NAME', 'CUSTOMER_SITE_ID','ASSIGN_PICKMAN','PHYSICAL_WAREHOUSE','ORG_ID','ORG_NAME','SALESMAN_NO','SALESMAN_NAME','INVOICE_NUMBER','INVENTORY_ITEM_ID','ITEM_DESCRIPTION')  # Group by relevant fields
            .annotate(
                total_picked_qty=Sum('PICKED_QTY')
            )  # Calculate sum of PICKED_QTY
            .order_by('REQ_ID')  # Optional: ordering
        )
        return queryset

    def list(self, request, *args, **kwargs):
        # Call the filtered queryset
        queryset = self.get_queryset()

        # Return the data as a custom response since the queryset is aggregated
        return Response(queryset)

def get_salesman_data(request):
    salesman_no = request.GET.get('salesman_no')

    if not salesman_no:
        return JsonResponse({"error": "salesman_no is required"}, status=400)

    query = """
        SELECT DISTINCT SALESMAN_NO, ORG_ID, TO_WAREHOUSE, ORG_NAME
        FROM [BUYP].[BUYP].[XXALJE_UNDELIVERED_DATA_BUYP1]
        WHERE SALESMAN_NO = %s
    """

    with connection.cursor() as cursor:
        cursor.execute(query, [salesman_no])
        rows = cursor.fetchall()

    # Format results
    results = []
    for row in rows:
        results.append({
            "salesman_no": row[0],
            "org_id": row[1],
            "to_warehouse": row[2],
            "org_name": row[3]
        })

    return JsonResponse(results, safe=False)

class CompletedDispatchFilteredLivestageView(viewsets.ModelViewSet):
    serializer_class = Pickman_ScanModelsserializers
    pagination_class = StandardResultsSetPagination

    def get_queryset(self):
        # Retrieve 'reqno' from the URL kwargs
        reqno = self.kwargs.get('reqno')
        
        # Fetch queryset from Pickman_ScanModels, group by PICK_ID and REQ_ID, and filter by REQ_ID=reqno
        queryset = (
            Pickman_ScanModels.objects.filter(REQ_ID=reqno).exclude(FLAG__in=['SR'])  # Filter by REQ_ID
            .values(
                'PICK_ID',  # Grouping by PICK_ID
                'REQ_ID',   # Grouping by REQ_ID             
                'ASSIGN_PICKMAN',
                'PHYSICAL_WAREHOUSE',
                'ORG_ID',
                'ORG_NAME',
                'SALESMAN_NO',
                'SALESMAN_NAME',
                'MANAGER_NO',
                'MANAGER_NAME',                
                'PICKMAN_NO',
                'PICKMAN_NAME',
                'CUSTOMER_NUMBER',
                'CUSTOMER_NAME',
                'CUSTOMER_SITE_ID',
                "FLAG"            
            )
            .annotate(
                PICKED_QTY=Sum('PICKED_QTY'),  # Summing PICKED_QTY
                DISPATCHED_QTY=Sum('DISPATCHED_QTY'),  # Summing DISPATCHED_QTY if needed
                BALANCE_QTY=Sum('BALANCE_QTY'),  # Summing BALANCE_QTY if needed
                TOT_QUANTITY=Sum('TOT_QUANTITY')  # Summing TOT_QUANTITY if needed
            )
            .order_by('PICK_ID', 'REQ_ID')  # Order by PICK_ID and REQ_ID
        )

        # Return the filtered and aggregated queryset
        return queryset


# @csrf_exempt
# def dispatch_progress_raw_view(request):
#     if request.method == 'GET':
#         warehouse = request.GET.get('warehouse')
#         status = request.GET.get('status')  # 'Progressing' or 'Fullfilled'

#         if not warehouse or status not in ['Progressing', 'Fullfilled']:
#             return JsonResponse({'status': 'error', 'message': 'Missing or invalid parameters'}, status=400)

#         try:
#             close_old_connections()
#             with connection.cursor() as cursor:
#                 query = """
#                 ;WITH DispatchData AS (
#                     SELECT
#                         MAX(d.ID) AS id,
#                         d.REQ_ID,
#                         d.COMMERCIAL_NO,
#                         d.COMMERCIAL_NAME,
#                         d.SALESMAN_NO,
#                         d.SALESMAN_NAME,
#                         d.CUSTOMER_NUMBER,
#                         d.CUSTOMER_NAME,
#                         d.CUSTOMER_SITE_ID,
#                         d.INVOICE_DATE,
#                         d.DELIVERY_DATE,
#                         SUM(d.DISPATCHED_QTY) AS dis_qty_total,
#                         SUM(d.DISPATCHED_BY_MANAGER) AS dis_mangerQty_total
#                     FROM BUYP.dbo.WHR_CREATE_DISPATCH d
#                     WHERE d.FLAG NOT IN ('R','OU')
#                       AND d.PHYSICAL_WAREHOUSE = %s
#                     GROUP BY d.REQ_ID, d.COMMERCIAL_NO, d.COMMERCIAL_NAME,
#                              d.SALESMAN_NO, d.SALESMAN_NAME,
#                              d.CUSTOMER_NUMBER, d.CUSTOMER_NAME, d.CUSTOMER_SITE_ID,
#                              d.INVOICE_DATE, d.DELIVERY_DATE
#                 ),
#                 TruckQty AS (
#                     SELECT REQ_ID, COUNT(*) AS previous_truck_qty
#                     FROM BUYP.dbo.WHR_TRUCK_SCAN_DETAILS
#                     WHERE FLAG NOT IN ('R','SR','OU')
#                     GROUP BY REQ_ID
#                 ),
#                 ReturnQty AS (
#                     SELECT REQ_ID, CUSTOMER_NUMBER, CUSTOMER_SITE_ID, COUNT(*) AS return_qty
#                     FROM BUYP.dbo.WHR_RETURN_DISPATCH
#                     WHERE RE_ASSIGN_STATUS != 'Re-Assign-Finished'
#                     GROUP BY REQ_ID, CUSTOMER_NUMBER, CUSTOMER_SITE_ID
#                 ),
#                 PickedQty AS (
#                     SELECT REQ_ID, SUM(PICKED_QTY) AS picked_qty
#                     FROM BUYP.dbo.WHR_PICKED_MAN
#                     WHERE FLAG NOT IN ('R','OU')
#                     GROUP BY REQ_ID
#                 )
#                 SELECT 
#                     dd.id, dd.REQ_ID, dd.COMMERCIAL_NO, dd.COMMERCIAL_NAME,
#                     dd.SALESMAN_NO, dd.SALESMAN_NAME,
#                     dd.CUSTOMER_NUMBER, dd.CUSTOMER_NAME, dd.CUSTOMER_SITE_ID,
#                     dd.INVOICE_DATE, dd.DELIVERY_DATE,
#                     dd.dis_qty_total, dd.dis_mangerQty_total,
#                     (dd.dis_qty_total - dd.dis_mangerQty_total) AS balance_qty,
#                     ISNULL(t.previous_truck_qty,0) AS previous_truck_qty,
#                     ISNULL(r.return_qty,0) AS return_qty,
#                     ISNULL(p.picked_qty,0) AS picked_qty
#                 FROM DispatchData dd
#                 LEFT JOIN TruckQty t ON dd.REQ_ID = t.REQ_ID
#                 LEFT JOIN ReturnQty r ON dd.REQ_ID = r.REQ_ID 
#                     AND dd.CUSTOMER_NUMBER = r.CUSTOMER_NUMBER 
#                     AND dd.CUSTOMER_SITE_ID = r.CUSTOMER_SITE_ID
#                 LEFT JOIN PickedQty p ON dd.REQ_ID = p.REQ_ID;
#                 """
#                 cursor.execute(query, [warehouse])
#                 rows = cursor.fetchall()

#                 result = []
#                 for row in rows:
#                     (
#                         row_id, req_id, commercial_no, commercial_name, salesman_no, salesman_name,
#                         cusno, cusname, cussite, invoice_date, delivery_date,
#                         dis_qty_total, dis_mangerQty_total, balance_qty,
#                         previous_truck_qty, return_qty, picked_qty
#                     ) = row

#                     if (status == 'Progressing' and dis_qty_total != previous_truck_qty) or \
#                        (status == 'Fullfilled' and dis_qty_total == previous_truck_qty):

#                         result.append({
#                             'id': row_id,
#                             'reqno': req_id,
#                             'commercialNo': commercial_no,
#                             'commercialName': commercial_name,
#                             'salesman_no': salesman_no,
#                             'salesmanName': salesman_name,
#                             'cusno': cusno,
#                             'cusname': cusname,
#                             'cussite': cussite,
#                             'date': invoice_date,
#                             'deliverydate': delivery_date,
#                             'dis_qty_total': dis_qty_total,
#                             'dis_mangerQty_total': dis_mangerQty_total,
#                             'balance_qty': balance_qty,
#                             'previous_truck_qty': previous_truck_qty,
#                             'return_qty': return_qty,
#                             'picked_qty': picked_qty
#                         })

#                 result.sort(key=lambda x: x['id'], reverse=True)
#                 return JsonResponse(result, safe=False)

#         except Exception as e:
#             return JsonResponse({'status': 'error', 'message': str(e)}, status=500)

#     return JsonResponse({'status': 'error', 'message': 'Invalid request method'}, status=405)

@csrf_exempt
def dispatch_progress_raw_view(request):
    if request.method == 'GET':
        warehouse = request.GET.get('warehouse')
        status = request.GET.get('status')  # 'Progressing' or 'Fullfilled'

        if not warehouse or status not in ['Progressing', 'Fullfilled']:
            return JsonResponse({'status': 'error', 'message': 'Missing or invalid parameters'}, status=400)

        try:
            close_old_connections()
            with connection.cursor() as cursor:
                query = """
                ;WITH DispatchData AS (
                    SELECT
                        MAX(d.ID) AS id,
                        d.REQ_ID,
                        d.COMMERCIAL_NO,
                        d.COMMERCIAL_NAME,
                        d.SALESMAN_NO,
                        d.SALESMAN_NAME,
                        d.CUSTOMER_NUMBER,
                        d.CUSTOMER_NAME,
                        d.CUSTOMER_SITE_ID,
                        d.INVOICE_DATE,
                        d.DELIVERY_DATE,
                        SUM(d.DISPATCHED_QTY) AS dis_qty_total,
                        SUM(d.DISPATCHED_BY_MANAGER) AS dis_mangerQty_total
                    FROM BUYP.dbo.WHR_CREATE_DISPATCH d
                    WHERE d.FLAG NOT IN ('R','OU')
                      AND d.PHYSICAL_WAREHOUSE = %s
                    GROUP BY d.REQ_ID, d.COMMERCIAL_NO, d.COMMERCIAL_NAME,
                             d.SALESMAN_NO, d.SALESMAN_NAME,
                             d.CUSTOMER_NUMBER, d.CUSTOMER_NAME, d.CUSTOMER_SITE_ID,
                             d.INVOICE_DATE, d.DELIVERY_DATE
                ),
                TruckQty AS (
                    SELECT REQ_ID, COUNT(*) AS previous_truck_qty
                    FROM BUYP.dbo.WHR_TRUCK_SCAN_DETAILS
                    WHERE FLAG NOT IN ('R','SR','OU')
                    GROUP BY REQ_ID
                ),
                ReturnQty AS (
                    SELECT REQ_ID, CUSTOMER_NUMBER, CUSTOMER_SITE_ID, COUNT(*) AS return_qty
                    FROM BUYP.dbo.WHR_RETURN_DISPATCH
                    WHERE RE_ASSIGN_STATUS != 'Re-Assign-Finished'
                    GROUP BY REQ_ID, CUSTOMER_NUMBER, CUSTOMER_SITE_ID
                ),
                PickedQty AS (
                    SELECT REQ_ID, SUM(PICKED_QTY) AS picked_qty
                    FROM BUYP.dbo.WHR_PICKED_MAN
                    WHERE FLAG NOT IN ('R','OU')
                    GROUP BY REQ_ID
                ),
                InvoiceList AS (
                    SELECT 
                        REQ_ID,
                        STRING_AGG(INVOICE_NUMBER, ',') AS invoice_no_list
                    FROM BUYP.dbo.WHR_CREATE_DISPATCH
                    WHERE FLAG NOT IN ('R','OU')
                    GROUP BY REQ_ID
                )
                SELECT 
                    dd.id, dd.REQ_ID, dd.COMMERCIAL_NO, dd.COMMERCIAL_NAME,
                    dd.SALESMAN_NO, dd.SALESMAN_NAME,
                    dd.CUSTOMER_NUMBER, dd.CUSTOMER_NAME, dd.CUSTOMER_SITE_ID,
                    dd.INVOICE_DATE, dd.DELIVERY_DATE,
                    dd.dis_qty_total, dd.dis_mangerQty_total,
                    (dd.dis_qty_total - dd.dis_mangerQty_total) AS balance_qty,
                    ISNULL(t.previous_truck_qty,0) AS previous_truck_qty,
                    ISNULL(r.return_qty,0) AS return_qty,
                    ISNULL(p.picked_qty,0) AS picked_qty,
                    ISNULL(i.invoice_no_list,'') AS invoice_no_list
                FROM DispatchData dd
                LEFT JOIN TruckQty t ON dd.REQ_ID = t.REQ_ID
                LEFT JOIN ReturnQty r ON dd.REQ_ID = r.REQ_ID 
                    AND dd.CUSTOMER_NUMBER = r.CUSTOMER_NUMBER 
                    AND dd.CUSTOMER_SITE_ID = r.CUSTOMER_SITE_ID
                LEFT JOIN PickedQty p ON dd.REQ_ID = p.REQ_ID
                LEFT JOIN InvoiceList i ON dd.REQ_ID = i.REQ_ID;
                """
                cursor.execute(query, [warehouse])
                rows = cursor.fetchall()

                result = []
                for row in rows:
                    (
                        row_id, req_id, commercial_no, commercial_name,
                        salesman_no, salesman_name,
                        cusno, cusname, cussite,
                        invoice_date, delivery_date,
                        dis_qty_total, dis_mangerQty_total, balance_qty,
                        previous_truck_qty, return_qty, picked_qty,
                        invoice_no_list
                    ) = row  # ✅ 18 values now

                    if (status == 'Progressing' and dis_qty_total != previous_truck_qty) or \
                       (status == 'Fullfilled' and dis_qty_total == previous_truck_qty):

                        result.append({
                            'id': row_id,
                            'reqno': req_id,
                            'commercialNo': commercial_no,
                            'commercialName': commercial_name,
                            'salesman_no': salesman_no,
                            'salesmanName': salesman_name,
                            'cusno': cusno,
                            'cusname': cusname,
                            'cussite': cussite,
                            'date': invoice_date,
                            'deliverydate': delivery_date,
                            'dis_qty_total': dis_qty_total,
                            'dis_mangerQty_total': dis_mangerQty_total,
                            'balance_qty': balance_qty,
                            'previous_truck_qty': previous_truck_qty,
                            'return_qty': return_qty,
                            'picked_qty': picked_qty,
                            'invoice_no_list': invoice_no_list
                        })

                result.sort(key=lambda x: x['id'], reverse=True)
                return JsonResponse(result, safe=False)

        except Exception as e:
            return JsonResponse({'status': 'error', 'message': str(e)}, status=500)

    return JsonResponse({'status': 'error', 'message': 'Invalid request method'}, status=405)

@csrf_exempt
def dispatch_progress_Oracle_Cancel_view(request):
    if request.method == 'GET':
        warehouse = request.GET.get('warehouse')
        status = request.GET.get('status')  # 'Progressing' or 'Fullfilled'

        if not warehouse or status not in ['Progressing', 'Fullfilled']:
            return JsonResponse({'status': 'error', 'message': 'Missing or invalid parameters'}, status=400)

        try:
            # ✅ Step 1: Get grouped dispatch data
            query = """
                SELECT
                    MAX(ID) AS id,
                    REQ_ID,
                    COMMERCIAL_NO,
                    COMMERCIAL_NAME,
                    SALESMAN_NO,
                    SALESMAN_NAME,
                    CUSTOMER_NUMBER,
                    CUSTOMER_NAME,
                    CUSTOMER_SITE_ID,
                    INVOICE_DATE,
                    DELIVERY_DATE,
                    SUM(DISPATCHED_QTY) AS dis_qty_total,
                    SUM(DISPATCHED_BY_MANAGER) AS dis_mangerQty_total
                FROM BUYP.dbo.WHR_CREATE_DISPATCH
                WHERE FLAG != 'R' AND FLAG = 'OU' AND PHYSICAL_WAREHOUSE = %s
                GROUP BY
                    REQ_ID, COMMERCIAL_NO, COMMERCIAL_NAME,SALESMAN_NO, SALESMAN_NAME,
                    CUSTOMER_NUMBER, CUSTOMER_NAME, CUSTOMER_SITE_ID,
                    INVOICE_DATE, DELIVERY_DATE
            """

            with connection.cursor() as cursor:
                cursor.execute(query, [warehouse])
                dispatch_rows = cursor.fetchall()

            result = []

            for row in dispatch_rows:
                (
                    row_id, req_id, commercial_no, commercial_name, salesman_no, salesman_name,
                    cusno, cusname, cussite, invoice_date, delivery_date,
                    dis_qty_total, dis_mangerQty_total
                ) = row

                dis_qty_total = dis_qty_total or 0
                dis_mangerQty_total = dis_mangerQty_total or 0
                balance_qty = dis_qty_total - dis_mangerQty_total

                # ✅ Step 2: Get previous_truck_qty
                truck_query = """
                    SELECT COUNT(*) FROM BUYP.dbo.WHR_TRUCK_SCAN_DETAILS
                    WHERE REQ_ID = %s AND FLAG NOT IN ('R', 'SR')
                """
                with connection.cursor() as cursor:
                    cursor.execute(truck_query, [req_id])
                    previous_truck_qty = cursor.fetchone()[0]

                # ✅ Step 3: Get return_qty
                return_query = """
                    SELECT COUNT(*) FROM BUYP.dbo.WHR_RETURN_DISPATCH
                    WHERE REQ_ID = %s AND CUSTOMER_NUMBER = %s AND CUSTOMER_SITE_ID = %s
                    AND RE_ASSIGN_STATUS != 'Re-Assign-Finished'
                """
                with connection.cursor() as cursor:
                    cursor.execute(return_query, [req_id, cusno, cussite])
                    return_qty = cursor.fetchone()[0]

                # ✅ Step 4: Get picked_qty
                picked_query = """
                    SELECT SUM(PICKED_QTY) FROM BUYP.dbo.WHR_PICKED_MAN
                    WHERE REQ_ID = %s AND FLAG != 'R' AND FLAG = 'OU' 
                """
                with connection.cursor() as cursor:
                    cursor.execute(picked_query, [req_id])
                    picked_qty_result = cursor.fetchone()[0]
                    picked_qty = picked_qty_result if picked_qty_result else 0

                # ✅ Step 5: Filter results
                if (status == 'Progressing' and dis_qty_total != previous_truck_qty) or \
                   (status == 'Fullfilled' and dis_qty_total == previous_truck_qty):

                    result.append({
                        'id': row_id,
                        'reqno': req_id,
                        'commercialNo': commercial_no,
                        'commercialName': commercial_name,
                        'salesman_no': salesman_no,
                        'salesmanName': salesman_name,
                        'cusno': cusno,
                        'cusname': cusname,
                        'cussite': cussite,
                        'date': invoice_date,
                        'deliverydate': delivery_date,
                        'dis_qty_total': dis_qty_total,
                        'dis_mangerQty_total': dis_mangerQty_total,
                        'balance_qty': balance_qty,
                        'previous_truck_qty': previous_truck_qty,
                        'return_qty': return_qty,
                        'picked_qty': picked_qty
                    })

            # ✅ Sort by ID descending
            result.sort(key=lambda x: x['id'], reverse=True)
            return JsonResponse(result, safe=False)

        except Exception as e:
            return JsonResponse({'status': 'error', 'message': str(e)}, status=500)

    return JsonResponse({'status': 'error', 'message': 'Invalid request method'}, status=405)

class FilteredLivestageView(viewsets.ModelViewSet):
    serializer_class = Pickman_ScanModelsserializers
    pagination_class = StandardResultsSetPagination

    def get_queryset(self):
        # Fetch queryset from Pickman_ScanModels, filter FLAG not equal to 'R' or 'SR', and group by PICK_ID and REQ_ID
        queryset = (
            Pickman_ScanModels.objects.filter(~Q(FLAG__in=['R', 'SR']))  # Exclude FLAG values 'R' and 'SR'
            .values(
                'PICK_ID',  # Grouping by PICK_ID
                'REQ_ID',   # Grouping by REQ_ID             
                'ASSIGN_PICKMAN',
                'PHYSICAL_WAREHOUSE',
                'ORG_ID',
                'ORG_NAME',
                'SALESMAN_NO',
                'SALESMAN_NAME',
                'MANAGER_NO',
                'MANAGER_NAME',                
                'PICKMAN_NO',
                'PICKMAN_NAME',                
                'CUSTOMER_NUMBER',
                'CUSTOMER_NAME',
                'CUSTOMER_SITE_ID',
                "FLAG"            
            )
            .annotate(
                PICKED_QTY=Sum('PICKED_QTY'),  # Summing PICKED_QTY
                DISPATCHED_QTY=Sum('DISPATCHED_QTY'),  # Summing DISPATCHED_QTY if needed
                BALANCE_QTY=Sum('BALANCE_QTY'),  # Summing BALANCE_QTY if needed
                TOT_QUANTITY=Sum('TOT_QUANTITY')  # Summing TOT_QUANTITY if needed
            )
            .order_by('PICK_ID', 'REQ_ID')  # Order by PICK_ID and REQ_ID
        )

        # Return the aggregated queryset
        return queryset

class LivestagebuttonstausView(viewsets.ViewSet):
    """
    A view to handle dispatch data and return status based on conditions.
    This version checks for full match on req_no, pick_id, Customer_no, Customer_Site
    and only counts rows with SCAN_STATUS='Request for Delivery'.
    """

    def retrieve(self, request, reqno, pickno, Customer_no, count, cussite):
        try:
            # First, validate if a dispatch with all matching values exists (ignoring SCAN_STATUS)
            base_queryset = ToGetGenerateDispatch.objects.filter(
                req_no=reqno,
                pick_id=pickno,
                Customer_no=Customer_no,
                Customer_Site=cussite
            )

            if not base_queryset.exists():
                return Response({
                    "status": "Not Available",
                    "message": f"No dispatch found for given details (reqno: {reqno}, pickno: {pickno}, Customer_no: {Customer_no}, Customer_Site: {cussite}).",
                    "total_filtered_rows": 0
                }, status=status.HTTP_200_OK)

            # Now, count rows matching SCAN_STATUS = "Request for Delivery"
            filtered_count = base_queryset.filter(SCAN_STATUS="Request for Delivery").count()

            # Compare with expected count
            expected_count = int(count)

            if filtered_count == expected_count:
                status_message = "Completed"
            elif filtered_count < expected_count:
                status_message = "Processing"
            else:
                status_message = "Invalid Data"

            # Return the result
            return Response({
                "status": status_message,
                "total_filtered_rows": filtered_count,
                "expected_count": expected_count
            }, status=status.HTTP_200_OK)

        except Exception as e:
            return Response({
                "error": "An error occurred while processing the request.",
                "details": str(e)
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

# class CombinedLivestageReportView(APIView):
#     def get(self, request):
#         try:
#             paginator = StandardResultsSetPagination()
#             base_queryset = (
#                 Pickman_ScanModels.objects
#                 .filter(~Q(FLAG__in=['R', 'SR', 'OU']))
#                 .annotate(
#                     numeric_req_id=Cast(Substr('PICK_ID', 3), IntegerField())  # Extract number part after 'DR'
#                 )
#                 .values(
#                     'PICK_ID', 'REQ_ID', 'ASSIGN_PICKMAN', 'PHYSICAL_WAREHOUSE',
#                     'ORG_ID', 'ORG_NAME', 'SALESMAN_NO', 'SALESMAN_NAME',
#                     'MANAGER_NO', 'MANAGER_NAME', 'PICKMAN_NO', 'PICKMAN_NAME',
#                     'CUSTOMER_NUMBER', 'CUSTOMER_NAME', 'CUSTOMER_SITE_ID', 'FLAG'
#                 )
#                 .annotate(
#                     PICKED_QTY=Sum('PICKED_QTY'),
#                     DISPATCHED_QTY=Sum('DISPATCHED_QTY'),
#                     BALANCE_QTY=Sum('BALANCE_QTY'),
#                     TOT_QUANTITY=Sum('TOT_QUANTITY')
#                 )
#                 .order_by('-numeric_req_id')  # Descending order by numeric part
#             )


#             paginated_queryset = paginator.paginate_queryset(list(base_queryset), request)

#             results = []

#             for row in paginated_queryset:
#                 reqno = row['REQ_ID']
#                 pickno = row['PICK_ID']
#                 customer_no = row['CUSTOMER_NUMBER']
#                 cussite = row['CUSTOMER_SITE_ID']
#                 expected_count = row.get('PICKED_QTY') or 0

#                 # Dispatch status logic
#                 dispatch_qs = ToGetGenerateDispatch.objects.filter(
#                     req_no=reqno,
#                     pick_id=pickno,
#                     Customer_no=customer_no,
#                     Customer_Site=cussite
#                 )

#                 if not dispatch_qs.exists():
#                     status_message = "Not Available"
#                     filtered_count = 0
#                 else:
#                     filtered_count = dispatch_qs.filter(SCAN_STATUS="Request for Delivery").count()
#                     if filtered_count == expected_count:
#                         status_message = "Completed"
#                     elif filtered_count < expected_count:
#                         status_message = "Processing"
#                     else:
#                         status_message = "Invalid Data"

#                 # Previous truck count (FLAG='A')
#                 previous_truck_count = Truck_scanModels.objects.filter(
#                     REQ_ID=reqno,
#                     PICK_ID=pickno,
#                     FLAG='A'
#                 ).count()

#                 # Truck scan count where FLAG != 'R'
#                 truck_scan_count = Truck_scanModels.objects.filter(
#                     REQ_ID=reqno,
#                     PICK_ID=pickno
#                 ).exclude(FLAG='R').count()

#                 # Truck status logic
#                 picked_qty = expected_count or 0
#                 truckstatus = truck_scan_count == 0 or truck_scan_count < picked_qty

#                 # Format row: all fields as strings except int fields
#                 formatted_row = {k: str(v) for k, v in row.items()}
#                 formatted_row.update({
#                     "status": str(status_message),
#                     "total_filtered_rows": filtered_count,
#                     "expected_count": str(expected_count),
#                     "previous_truck_qty": previous_truck_count,
#                     "truckstatus": truckstatus  # Boolean
#                 })

#                 results.append(formatted_row)

#             return paginator.get_paginated_response(results)

#         except Exception as e:
#             return Response({
#                 "error": "An error occurred while processing the data.",
#                 "details": str(e)
#             }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

from django.db import models
from django.db.models import (
    Sum, Count, Q, F, Value, Case, When, BooleanField,
    OuterRef, Subquery, IntegerField, ExpressionWrapper
)
from django.db.models.functions import Coalesce
from django.http import JsonResponse
from django.views import View



class CombinedLivestageReportView(APIView):
    def get(self, request):
        try:
            paginator = StandardResultsSetPagination()
            base_queryset = (
                Pickman_ScanModels.objects
                .filter(~Q(FLAG__in=['R', 'SR', 'OU']))
                .annotate(
                    numeric_req_id=Cast(Substr('PICK_ID', 3), IntegerField())  # Extract number part after 'DR'
                )
                .values(
                    'PICK_ID', 'REQ_ID', 'ASSIGN_PICKMAN', 'PHYSICAL_WAREHOUSE',
                    'ORG_ID', 'ORG_NAME', 'SALESMAN_NO', 'SALESMAN_NAME',
                    'MANAGER_NO', 'MANAGER_NAME', 'PICKMAN_NO', 'PICKMAN_NAME',
                    'CUSTOMER_NUMBER', 'CUSTOMER_NAME', 'CUSTOMER_SITE_ID', 'FLAG'
                )
                .annotate(
                    PICKED_QTY=Sum('PICKED_QTY'),
                    DISPATCHED_QTY=Sum('DISPATCHED_QTY'),
                    BALANCE_QTY=Sum('BALANCE_QTY'),
                    TOT_QUANTITY=Sum('TOT_QUANTITY')
                )
                .order_by('-numeric_req_id')  # Descending order by numeric part
            )


            paginated_queryset = paginator.paginate_queryset(list(base_queryset), request)

            results = []

            for row in paginated_queryset:
                reqno = row['REQ_ID']
                pickno = row['PICK_ID']
                customer_no = row['CUSTOMER_NUMBER']
                cussite = row['CUSTOMER_SITE_ID']
                expected_count = row.get('PICKED_QTY') or 0

                # Dispatch status logic
                dispatch_qs = ToGetGenerateDispatch.objects.filter(
                    req_no=reqno,
                    pick_id=pickno,
                    Customer_no=customer_no,
                    Customer_Site=cussite
                )

                if not dispatch_qs.exists():
                    status_message = "Not Available"
                    filtered_count = 0
                else:
                    filtered_count = dispatch_qs.filter(SCAN_STATUS="Request for Delivery").count()
                    if filtered_count == expected_count:
                        status_message = "Completed"
                    elif filtered_count < expected_count:
                        status_message = "Processing"
                    else:
                        status_message = "Invalid Data"

                # Previous truck count (FLAG='A')
                previous_truck_count = Truck_scanModels.objects.filter(
                    REQ_ID=reqno,
                    PICK_ID=pickno,
                    FLAG='A'
                ).count()

                # Truck scan count where FLAG != 'R'
                truck_scan_count = Truck_scanModels.objects.filter(
                    REQ_ID=reqno,
                    PICK_ID=pickno
                ).exclude(FLAG='R').count()

                # Truck status logic
                picked_qty = expected_count or 0
                truckstatus = truck_scan_count == 0 or truck_scan_count < picked_qty

                # Format row: all fields as strings except int fields
                formatted_row = {k: str(v) for k, v in row.items()}
                formatted_row.update({
                    "status": str(status_message),
                    "total_filtered_rows": filtered_count,
                    "expected_count": str(expected_count),
                    "previous_truck_qty": previous_truck_count,
                    "truckstatus": truckstatus  # Boolean
                })

                results.append(formatted_row)

            return paginator.get_paginated_response(results)

        except Exception as e:
            return Response({
                "error": "An error occurred while processing the data.",
                "details": str(e)
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)



class NewCombinedLivestageReportView(View):
    def get(self, request):
        try:
            wh_name = request.GET.get('warehousename')
            status_param = request.GET.get('status')

            where_conditions = ["ps.FLAG NOT IN ('R', 'SR', 'OU')"]
            params = []

            if wh_name:
                where_conditions.append("ps.PHYSICAL_WAREHOUSE = %s")
                params.append(wh_name)

            # Optimized query with CTEs
            base_query = f"""
                ;WITH truck_details AS (
                    SELECT 
                        d.req_no,
                        d.pick_id,
                        d.customer_no,
                        d.customer_site,
                        COUNT(*) AS dispatch_count,
                        SUM(CASE WHEN d.SCAN_STATUS = 'Request for Delivery' THEN 1 ELSE 0 END) AS total_filtered_rows
                    FROM WHR_SAVE_TRUCK_DETAILS_TBL d
                    GROUP BY d.req_no, d.pick_id, d.customer_no, d.customer_site
                ),
                truck_scans AS (
                    SELECT 
                        t.REQ_ID,
                        t.PICK_ID,
                        SUM(CASE WHEN t.FLAG = 'A' THEN 1 ELSE 0 END) AS previous_truck_qty,
                        SUM(CASE WHEN t.FLAG != 'R' THEN 1 ELSE 0 END) AS truck_scan_count
                    FROM WHR_TRUCK_SCAN_DETAILS t
                    GROUP BY t.REQ_ID, t.PICK_ID
                )
                SELECT 
                    ps.PICK_ID,
                    ps.REQ_ID,
                    ps.ASSIGN_PICKMAN,
                    ps.PHYSICAL_WAREHOUSE,
                    ps.ORG_ID,
                    ps.ORG_NAME,
                    ps.SALESMAN_NO,
                    ps.SALESMAN_NAME,
                    ps.MANAGER_NO,
                    ps.MANAGER_NAME,
                    ps.PICKMAN_NO,
                    ps.PICKMAN_NAME,
                    ps.CUSTOMER_NUMBER,
                    ps.CUSTOMER_NAME,
                    ps.CUSTOMER_SITE_ID,
                    ps.FLAG,
                    SUM(ps.PICKED_QTY) AS PICKED_QTY,
                    SUM(ps.DISPATCHED_QTY) AS DISPATCHED_QTY,
                    SUM(ps.BALANCE_QTY) AS BALANCE_QTY,
                    SUM(ps.TOT_QUANTITY) AS TOT_QUANTITY,
                    COALESCE(td.total_filtered_rows, 0) AS total_filtered_rows,
                    COALESCE(td.dispatch_count, 0) AS dispatch_count,
                    COALESCE(ts.previous_truck_qty, 0) AS previous_truck_qty,
                    COALESCE(ts.truck_scan_count, 0) AS truck_scan_count
                FROM WHR_PICKED_MAN ps
                LEFT JOIN truck_details td 
                    ON td.req_no = ps.REQ_ID
                    AND td.pick_id = ps.PICK_ID
                    AND td.customer_no = ps.CUSTOMER_NUMBER
                    AND td.customer_site = ps.CUSTOMER_SITE_ID
                LEFT JOIN truck_scans ts
                    ON ts.REQ_ID = ps.REQ_ID
                    AND ts.PICK_ID = ps.PICK_ID
                WHERE {' AND '.join(where_conditions)}
                GROUP BY 
                    ps.PICK_ID, ps.REQ_ID, ps.ASSIGN_PICKMAN, ps.PHYSICAL_WAREHOUSE,
                    ps.ORG_ID, ps.ORG_NAME, ps.SALESMAN_NO, ps.SALESMAN_NAME,
                    ps.MANAGER_NO, ps.MANAGER_NAME, ps.PICKMAN_NO, ps.PICKMAN_NAME,
                    ps.CUSTOMER_NUMBER, ps.CUSTOMER_NAME, ps.CUSTOMER_SITE_ID, ps.FLAG,
                    td.total_filtered_rows, td.dispatch_count,
                    ts.previous_truck_qty, ts.truck_scan_count
                ORDER BY ps.PICK_ID
            """


            results = []
            with connection.cursor() as cursor:
                cursor.execute(base_query, params)
                columns = [col[0] for col in cursor.description]

                for row in cursor.fetchall():
                    row_dict = dict(zip(columns, row))

                    expected_count = row_dict['PICKED_QTY']
                    total_progress = row_dict['dispatch_count'] + row_dict['previous_truck_qty']

                    # Status logic
                    if expected_count == 0:
                        status = "Not Available"
                    elif total_progress == expected_count:
                        status = "Completed"
                    elif 0 < total_progress < expected_count:
                        status = "Processing"
                    else:
                        status = "Not Available"

                    truckstatus = row_dict['truck_scan_count'] < expected_count

                    # Apply status filters
                    if status_param == "on_livestage":
                        if row_dict['total_filtered_rows'] <= 0:
                            continue
                    elif status_param == "on_livestage_stage":
                        if expected_count <= (row_dict['previous_truck_qty'] + row_dict['total_filtered_rows']):
                            continue

                    if not truckstatus:
                        continue

                    row_dict['expected_count'] = expected_count
                    row_dict['total_progress'] = total_progress
                    row_dict['status'] = status
                    row_dict['truckstatus'] = truckstatus

                    results.append(row_dict)

            return JsonResponse({"results": results}, safe=False, json_dumps_params={'ensure_ascii': False})

        except Exception as e:
            return JsonResponse({
                "error": "Failed to process data",
                "details": str(e)
            }, status=500)


class Quick_Bill_CombinedLivestageReportView(View):
    def get(self, request):
        try:
            warehousename = request.GET.get("warehousename", None)

            where_conditions = []
            params = []

            if warehousename:
                where_conditions.append("c.PHYSICAL_WAREHOUSE = %s")
                params.append(warehousename)

            # Always filter pick_id starting with QKR
            where_conditions.append("da.pick_id LIKE %s")
            params.append("QKB%")

            where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"

            base_query = f"""
            ;WITH d_agg AS (
                SELECT
                    d.pick_id,
                    d.req_no,
                    MAX(d.pickman_name)      AS pickman_name,
                    MAX(d.pickman_no)        AS pickman_no,
                    MAX(d.salesman_no)       AS salesman_no,
                    MAX(d.salesman_name)     AS salesman_name,
                    MAX(d.manager_no)        AS manager_no,
                    MAX(d.manager_name)      AS manager_name,
                    MAX(d.Customer_no)       AS CUSTOMER_NUMBER,
                    MAX(d.Customer_name)     AS CUSTOMER_NAME,
                    MAX(d.Customer_Site)     AS CUSTOMER_SITE_ID,
                    SUM(COALESCE(d.DisReq_Qty, 0)) AS PICKED_QTY,
                    SUM(COALESCE(d.Send_qty, 0))   AS DISPATCHED_QTY,
                    SUM(COALESCE(d.Send_qty, 0)) - SUM(COALESCE(d.DisReq_Qty, 0)) AS BALANCE_QTY,
                    SUM(COALESCE(d.Send_qty, 0))   AS TOT_QUANTITY,
                    COUNT(*) AS truck_scan_count
                FROM WHR_SAVE_TRUCK_DETAILS_TBL d
                GROUP BY d.pick_id, d.req_no
            ),
            c_distinct AS (
                SELECT DISTINCT
                    REQ_ID,
                    PHYSICAL_WAREHOUSE,
                    ORG_ID,
                    ORG_NAME
                FROM WHR_CREATE_DISPATCH
            )
            SELECT
                da.pick_id                AS PICK_ID,
                da.req_no                 AS REQ_ID,
                da.pickman_name           AS ASSIGN_PICKMAN,
                c.PHYSICAL_WAREHOUSE,
                c.ORG_ID,
                c.ORG_NAME,
                da.salesman_no            AS SALESMAN_NO,
                da.salesman_name          AS SALESMAN_NAME,
                da.manager_no             AS MANAGER_NO,
                da.manager_name           AS MANAGER_NAME,
                da.pickman_no             AS PICKMAN_NO,
                da.pickman_name           AS PICKMAN_NAME,
                da.CUSTOMER_NUMBER,
                da.CUSTOMER_NAME,
                da.CUSTOMER_SITE_ID,
                'Completed'               AS status,
                'A'                       AS FLAG,
                da.PICKED_QTY,
                da.DISPATCHED_QTY,
                da.BALANCE_QTY,
                da.TOT_QUANTITY,
                da.truck_scan_count
            FROM d_agg da
            INNER JOIN c_distinct c
                ON da.req_no = c.REQ_ID
            WHERE {where_clause}
            ORDER BY da.pick_id
            """

            with connection.cursor() as cursor:
                cursor.execute(base_query, params)
                columns = [col[0] for col in cursor.description]
                results = [dict(zip(columns, row)) for row in cursor.fetchall()]

            return JsonResponse(results, safe=False, json_dumps_params={'ensure_ascii': False})

        except Exception as e:
            return JsonResponse({
                "error": "Failed to process data",
                "details": str(e)
            }, status=500)

            
class UpdateProductCodeView(viewsets.ViewSet):
    @action(detail=False, methods=['put'], url_path='update/(?P<itemcode>[^/.]+)/(?P<productcode>[^/.]+)')
    def update_product_code(self, request, itemcode, productcode):
        """
        Handle PUT requests for updating CUT_PRODUCT_CODE and OLD_PRODUCT_CODE.
        """
        try:
            # Validate that both parameters are provided
            if not itemcode or not productcode:
                return Response(
                    {'error': 'ITEM_CODE and CUT_PRODUCT_CODE are required.'},
                    status=status.HTTP_400_BAD_REQUEST
                )

            # Update the CUT_PRODUCT_CODE and OLD_PRODUCT_CODE in the database
            update_query = '''
                UPDATE [ALJE_ITEM_MASTER]
                SET 
                    CUT_PRODUCT_CODE = %s,
                    OLD_PRODUCT_CODE = 
                        CASE 
                            WHEN OLD_PRODUCT_CODE IS NULL THEN CONCAT('{', %s, '}')
                            ELSE CONCAT(
                                LEFT(OLD_PRODUCT_CODE, LEN(OLD_PRODUCT_CODE) - 1),
                                ', "',
                                %s, 
                                '"}'
                            )
                        END
                WHERE ITEM_CODE = %s;
            '''
            with connection.cursor() as cursor:
                cursor.execute(update_query, [productcode, productcode, productcode, itemcode])

            return Response(
                {'message': f'Product code successfully updated for ITEM_CODE {itemcode}.'},
                status=status.HTTP_200_OK
            )
        except Exception as e:
            # Handle any exceptions during processing
            return Response(
                {'error': f'Failed to update product code: {str(e)}'},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class TruckResultsPagination(PageNumberPagination):
    page_query_param = "pg"        # instead of "page"
    page_size_query_param = "limit"  # instead of "page_size"
    max_page_size = 5000000           # set a safe max
    page_size = 100000                # default page size

 
# class filtered_TruckView(viewsets.ViewSet):
#     pagination_class = TruckResultsPagination   # ?? custom unique class
 
#     def list(self, request):
#         # pagination class will handle `pg` and `limit`
#         paginator = self.pagination_class()
        
#         # Count total rows (for pagination metadata)
#         with connection.cursor() as cursor:
#             cursor.execute("""
#                 SELECT COUNT(DISTINCT T.DISPATCH_ID + '-' + CAST(T.REQ_ID AS VARCHAR))
#                 FROM WHR_TRUCK_SCAN_DETAILS T
#                 WHERE T.FLAG != 'R';
#             """)
#             total_count = cursor.fetchone()[0]
 
#         # Extract offset/limit from paginator
#         page = int(request.GET.get('pg', 1))
#         page_size = int(request.GET.get('limit', paginator.page_size))
#         offset = (page - 1) * page_size
#         limit = page_size
 
#         # Main paginated query
#         with connection.cursor() as cursor:
#             cursor.execute(f"""
#                 SELECT 
#                         T.ORG_ID,
#                          T.ORG_NAME,
#                         T.DISPATCH_ID,
#                         T.SALESMAN_NO,
#                         T.SALESMAN_NAME,
#                         T.PICK_ID,
#                         T.REQ_ID,
#                         T.DATE,
#                         T.CUSTOMER_NUMBER,
#                         T.CUSTOMER_NAME,
#                         T.CUSTOMER_SITE_ID,
#                         T.DELIVERY_STATUS,
#                         T.TRANSPORT_CHARGES,
#                         T.MISC_CHARGES,
#                         T.LOADING_CHARGES,
#                         T.SCAN_PATH,
#                         SUM(T.TRUCK_SEND_QTY) AS TRUCK_SEND_QTY,
#                         SUM(T.DISREQ_QTY) AS DISREQ_QTY,
#                         S.SALESREP_NUMBER AS RETAIL_SALESMAN,   
#                        STRING_AGG(CAST(T.INVOICE_NO AS VARCHAR), ',') AS INVOICE_LIST
#                     FROM WHR_TRUCK_SCAN_DETAILS T
#                     LEFT JOIN [BUYP].[BUYP].[ALJE_SALESREP] S
#                         ON T.SALESMAN_NO = S.SALESREP_ID
#                     WHERE T.FLAG != 'R'
#                     GROUP BY 
#                         T.ORG_ID, T.ORG_NAME, T.DISPATCH_ID, T.SALESMAN_NO, T.SALESMAN_NAME,
#                         T.PICK_ID, T.REQ_ID, T.DATE, T.CUSTOMER_NUMBER, T.CUSTOMER_NAME,
#                         T.CUSTOMER_SITE_ID, T.DELIVERY_STATUS,
#                         T.TRANSPORT_CHARGES, T.MISC_CHARGES, T.LOADING_CHARGES, T.SCAN_PATH,
#                         S.SALESREP_NUMBER
#                     ORDER BY 
#                         CAST(SUBSTRING(T.DISPATCH_ID, 3, LEN(T.DISPATCH_ID)) AS INT),
#                         T.PICK_ID,
#                         T.REQ_ID
#                     OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY;
#             """)
#             rows = cursor.fetchall()
 
#         results = [
#             {
#                 'ORG_ID': row[0],
#                 'ORG_NAME': row[1],
#                 'DISPATCH_ID': row[2],
#                 'SALESMAN_NO': row[3],
#                 'SALESMAN_NAME': row[4],
#                 'PICK_ID': row[5],
#                 'REQ_ID': row[6],
#                 'DATE': row[7],
#                 'CUSTOMER_NUMBER': row[8],
#                 'CUSTOMER_NAME': row[9],
#                 'CUSTOMER_SITE_ID': row[10],
#                 'DELIVERY_STATUS': row[11],
#                 'TRANSPORT_CHARGES': row[12],
#                 'MISC_CHARGES': row[13],
#                 'LOADING_CHARGES': row[14],
#                 'SCAN_PATH': row[15],
#                 'TRUCK_SEND_QTY': row[16],
#                 'DISREQ_QTY': row[17],
#                 'RETAIL_SALESMAN': row[18],
#                 'INVOICE_LISTS': row[19],
#             }
#             for row in rows
#         ]
 
#         return Response({
#             "total_records": total_count,
#             "current_page": page,
#             "rows_per_page": page_size,
#             "results": results
#         })
    

class filtered_TruckView(viewsets.ViewSet):
    pagination_class = TruckResultsPagination   # custom pagination class
 
    def list(self, request):
        # pagination class will handle `pg` and `limit`
        paginator = self.pagination_class()
        
        # Extract query parameters
        org_name = request.GET.get("ORG_NAME", None)

        # Build WHERE conditions dynamically
        where_conditions = ["T.FLAG != 'R'"]
        params = []

        if org_name:
            where_conditions.append("T.ORG_NAME = %s")
            params.append(org_name)

        where_clause = " AND ".join(where_conditions)

        # Count total rows (for pagination metadata)
        with connection.cursor() as cursor:
            cursor.execute(f"""
                SELECT COUNT(DISTINCT T.DISPATCH_ID + '-' + CAST(T.REQ_ID AS VARCHAR))
                FROM WHR_TRUCK_SCAN_DETAILS T
                WHERE {where_clause};
            """, params)
            total_count = cursor.fetchone()[0]
 
        # Extract offset/limit from paginator
        page = int(request.GET.get('pg', 1))
        page_size = int(request.GET.get('limit', paginator.page_size))
        offset = (page - 1) * page_size
        limit = page_size
 
        # Main paginated query
        with connection.cursor() as cursor:
            # cursor.execute(f"""
            #     SELECT 
            #             T.ORG_ID,
            #             T.ORG_NAME,
            #             T.DISPATCH_ID,
            #             T.SALESMAN_NO,
            #             T.SALESMAN_NAME,
            #             T.PICK_ID,
            #             T.REQ_ID,
            #             T.DATE,
            #             T.CUSTOMER_NUMBER,
            #             T.CUSTOMER_NAME,
            #             T.CUSTOMER_SITE_ID,
            #             T.DELIVERY_STATUS,
            #             T.TRANSPORT_CHARGES,
            #             T.MISC_CHARGES,
            #             T.LOADING_CHARGES,
            #             T.SCAN_PATH,
            #             SUM(T.TRUCK_SEND_QTY) AS TRUCK_SEND_QTY,
            #             SUM(T.DISREQ_QTY) AS DISREQ_QTY,
            #             S.SALESREP_NUMBER AS RETAIL_SALESMAN,   
            #             STRING_AGG(CAST(T.INVOICE_NO AS VARCHAR), ',') AS INVOICE_LIST
            #         FROM WHR_TRUCK_SCAN_DETAILS T
            #         LEFT JOIN [BUYP].[BUYP].[ALJE_SALESREP] S
            #             ON T.SALESMAN_NO = S.SALESREP_ID
            #         WHERE {where_clause}
            #         GROUP BY 
            #             T.ORG_ID, T.ORG_NAME, T.DISPATCH_ID, T.SALESMAN_NO, T.SALESMAN_NAME,
            #             T.PICK_ID, T.REQ_ID, T.DATE, T.CUSTOMER_NUMBER, T.CUSTOMER_NAME,
            #             T.CUSTOMER_SITE_ID, T.DELIVERY_STATUS,
            #             T.TRANSPORT_CHARGES, T.MISC_CHARGES, T.LOADING_CHARGES, T.SCAN_PATH,
            #             S.SALESREP_NUMBER
            #         ORDER BY 
            #             CAST(SUBSTRING(T.DISPATCH_ID, 3, LEN(T.DISPATCH_ID)) AS INT),
            #             T.PICK_ID,
            #             T.REQ_ID
            #         OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY;
            # """, params)

            cursor.execute(f"""
                SELECT 
                    T.ORG_ID,
                    T.ORG_NAME,
                    T.DISPATCH_ID,
                    T.SALESMAN_NO,
                    T.SALESMAN_NAME,
                    T.PICK_ID,
                    T.REQ_ID,
                    T.DATE,
                    T.CUSTOMER_NUMBER,
                    T.CUSTOMER_NAME,
                    T.CUSTOMER_SITE_ID,
                    T.DELIVERY_STATUS,
                    T.TRANSPORT_CHARGES,
                    T.MISC_CHARGES,
                    T.LOADING_CHARGES,
                    T.SCAN_PATH,
                    SUM(T.TRUCK_SEND_QTY) AS TRUCK_SEND_QTY,
                    SUM(T.DISREQ_QTY) AS DISREQ_QTY,
                    S.SALESREP_NUMBER AS RETAIL_SALESMAN,   
                    STRING_AGG(CAST(T.INVOICE_NO AS VARCHAR(MAX)), ',') AS INVOICE_LIST
                FROM WHR_TRUCK_SCAN_DETAILS T
                LEFT JOIN [BUYP].[BUYP].[ALJE_SALESREP] S
                    ON T.SALESMAN_NO = S.SALESREP_ID
                WHERE {where_clause}
                GROUP BY 
                    T.ORG_ID, T.ORG_NAME, T.DISPATCH_ID, T.SALESMAN_NO, T.SALESMAN_NAME,
                    T.PICK_ID, T.REQ_ID, T.DATE, T.CUSTOMER_NUMBER, T.CUSTOMER_NAME,
                    T.CUSTOMER_SITE_ID, T.DELIVERY_STATUS,
                    T.TRANSPORT_CHARGES, T.MISC_CHARGES, T.LOADING_CHARGES, T.SCAN_PATH,
                    S.SALESREP_NUMBER
                ORDER BY 
                    CAST(SUBSTRING(T.DISPATCH_ID, 3, LEN(T.DISPATCH_ID)) AS INT),
                    T.PICK_ID,
                    T.REQ_ID
                OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY;
            """, params)
            rows = cursor.fetchall()
 
        results = [
            {
                'ORG_ID': row[0],
                'ORG_NAME': row[1],
                'DISPATCH_ID': row[2],
                'SALESMAN_NO': row[3],
                'SALESMAN_NAME': row[4],
                'PICK_ID': row[5],
                'REQ_ID': row[6],
                'DATE': row[7],
                'CUSTOMER_NUMBER': row[8],
                'CUSTOMER_NAME': row[9],
                'CUSTOMER_SITE_ID': row[10],
                'DELIVERY_STATUS': row[11],
                'TRANSPORT_CHARGES': row[12],
                'MISC_CHARGES': row[13],
                'LOADING_CHARGES': row[14],
                'SCAN_PATH': row[15],
                'TRUCK_SEND_QTY': row[16],
                'DISREQ_QTY': row[17],
                'RETAIL_SALESMAN': row[18],
                'INVOICE_LISTS': row[19],
            }
            for row in rows
        ]
 
        return Response({
            "total_records": total_count,
            "current_page": page,
            "rows_per_page": page_size,
            "results": results
        })

@csrf_exempt
def update_truck_scan_details(request):
    if request.method == 'POST':
        try:
            data = json.loads(request.body)

            delivery_id = data.get('deliveryid')
            loading_charge = data.get('loadingcharge')
            transport_charge = data.get('transportcharge')
            misc_charge = data.get('misecharge')
            delivery_status = data.get('deliverystatus')
            scan_path = data.get('scanpath')

            if not delivery_id:
                return JsonResponse({'status': 'error', 'message': 'Delivery ID is required'}, status=400)

            with connection.cursor() as cursor:
                cursor.execute("""
                    UPDATE [BUYP].[dbo].[WHR_TRUCK_SCAN_DETAILS]
                    SET
                        LOADING_CHARGES = %s,
                        TRANSPORT_CHARGES = %s,
                        MISC_CHARGES = %s,
                        DELIVERY_STATUS = %s,
                        SCAN_PATH = %s,
                        LAST_UPDATE_DATE = GETDATE()
                    WHERE DISPATCH_ID = %s
                """, [
                    loading_charge,
                    transport_charge,
                    misc_charge,
                    delivery_status,
                    scan_path,
                    delivery_id
                ])

            return JsonResponse({'status': 'success', 'message': 'Record updated successfully'})
        
        except Exception as e:
            return JsonResponse({'status': 'error', 'message': str(e)}, status=500)
    
    return JsonResponse({'status': 'error', 'message': 'Invalid request method'}, status=405)

class ProductcodeGetView(viewsets.ModelViewSet):
    queryset = ProductcodeGetModels.objects.all()
    serializer_class = ProductcodeGetserializers
    pagination_class = StandardResultsSetPagination

class DispatchDetailsView(viewsets.ModelViewSet):
    serializer_class = create_Dispatchserializers
    pagination_class = StandardResultsSetPagination

    def get_queryset(self):
        req_no = self.kwargs.get('REQ_ID', None)
        if req_no is not None:
            # Filter by REQ_ID and exclude records with DISPATCHED_BY_MANAGER = 'D'
            return WHRCreateDispatch.objects.filter(
                REQ_ID=req_no
            ).exclude(FLAG='D')
        return WHRCreateDispatch.objects.none()  # Return empty if no REQ_ID is provided

    def list(self, request, *args, **kwargs):
        queryset = self.get_queryset()

        # Grouping the results
        result = {}
        for record in queryset:
            if record.REQ_ID not in result:
                result[record.REQ_ID] = {
                    "REQ_ID": record.REQ_ID,
                    "TO_WAREHOUSE": record.PHYSICAL_WAREHOUSE,
                    "ORG_ID": record.ORG_ID,
                    "ORG_NAME": record.ORG_NAME,
                    "SALESMAN_NO": record.SALESMAN_NO,
                    "SALESMAN_NAME": record.SALESMAN_NAME,
                    "CUSTOMER_NUMBER": record.CUSTOMER_NUMBER,
                    "CUSTOMER_NAME": record.CUSTOMER_NAME,
                    "CUSTOMER_SITE_ID": record.CUSTOMER_SITE_ID,
                    "INVOICE_DATE": record.INVOICE_DATE,
                    "INVOICE_NUMBER": record.INVOICE_NUMBER,
                    "CUSTOMER_TRX_ID": record.CUSTOMER_TRX_ID,
                    "CUSTOMER_TRX_LINE_ID": record.CUSTOMER_TRX_LINE_ID,
                    "DELIVERYADDRESS": record.DELIVERYADDRESS,
                    "REMARKS": record.REMARKS,
                    "TABLE_DETAILS": []
                }

            # Add detail to TABLE_DETAILS
            detail = {
                "ID": record.id,
                "INVOICE_NUMBER": record.INVOICE_NUMBER,
                "LINE_NUMBER": record.LINE_NUMBER,
                "INVENTORY_ITEM_ID": record.INVENTORY_ITEM_ID,
                "ITEM_DESCRIPTION": record.ITEM_DESCRIPTION,
                "TOT_QUANTITY": record.TOT_QUANTITY,
                "DISPATCHED_QTY": record.DISPATCHED_QTY,
                "BALANCE_QTY": record.BALANCE_QTY,
                "DISPATCHED_BY_MANAGER": record.DISPATCHED_BY_MANAGER,                
                "BALANCE_QTY": record.BALANCE_QTY,
            }
            result[record.REQ_ID]["TABLE_DETAILS"].append(detail)

        # Serialize and return the grouped results
        return Response(list(result.values()))
  
class filtedshippingproductdetailsView(viewsets.ModelViewSet):
    serializer_class = Truck_scanserializers

    def get_queryset(self):
        # Retrieve the `dispatchid` from the URL if available
        dispatch_id = self.kwargs.get('dispatchid')
        if dispatch_id:
            # Filter by DISPATCH_ID and FLAG != 'R'
            return Truck_scanModels.objects.filter(DISPATCH_ID=dispatch_id).exclude(FLAG='R')
        # Default behavior: Exclude FLAG = 'R' globally
        return Truck_scanModels.objects.exclude(FLAG='R')

class LogReportsView(viewsets.ModelViewSet):
    queryset = LogReportsModels.objects.all()
    serializer_class = LogReportsserializers
    pagination_class = StandardResultsSetPagination

class TransactionDetailView(viewsets.ModelViewSet):
    queryset = WHRTransactionDetail.objects.all()
    serializer_class = WHRTransactionDetailserializers
    pagination_class = StandardResultsSetPagination
 
class Filtered_ReturnDispatchView(viewsets.ViewSet):

    def list(self, request, *args, **kwargs):
        dispatch_id = kwargs.get('dispatch_id')
        
        if not dispatch_id:
            return Response({"error": "Dispatch ID is required."}, status=status.HTTP_400_BAD_REQUEST)

        queryset = Truck_scanModels.objects.filter(DISPATCH_ID=dispatch_id)

        if not queryset.exists():
            return Response({"error": "No data found for the given Dispatch ID."}, status=status.HTTP_404_NOT_FOUND)

        # Grouping the results
        result = {}
        for record in queryset:
            if record.DISPATCH_ID not in result:
                result[record.DISPATCH_ID] = {
                    "DISPATCH_ID": record.DISPATCH_ID,
                    "REQ_ID": record.REQ_ID,
                    "PHYSICAL_WAREHOUSE": record.PHYSICAL_WAREHOUSE,
                    "ORG_ID": record.ORG_ID,
                    "ORG_NAME": record.ORG_NAME,
                    "SALESMAN_NO": record.SALESMAN_NO,
                    "SALESMAN_NAME": record.SALESMAN_NAME,
                    "MANAGER_NO": record.MANAGER_NO,
                    "MANAGER_NAME": record.MANAGER_NAME,
                    "CUSTOMER_NUMBER": record.CUSTOMER_NUMBER,
                    "CUSTOMER_NAME": record.CUSTOMER_NAME,
                    "CUSTOMER_SITE_ID": record.CUSTOMER_SITE_ID,
                    "DATE": record.DATE,
                    "INVOICE_NO": record.INVOICE_NO,
                    "TABLE_DETAILS": []
                }

            # Add detail to TABLE_DETAILS
            detail = {
                "INVOICE_NO": record.INVOICE_NO,
                "CUSTOMER_TRX_ID": record.CUSTOMER_TRX_ID,
                "CUSTOMER_TRX_LINE_ID": record.CUSTOMER_TRX_LINE_ID,
                "LINE_NO": record.LINE_NO,
                "ITEM_CODE": record.ITEM_CODE,
                "ITEM_DETAILS": record.ITEM_DETAILS,
                "DISREQ_QTY": record.DISREQ_QTY,
                "BALANCE_QTY": record.BALANCE_QTY,
                "TRUCK_SEND_QTY": record.TRUCK_SEND_QTY,
            }
            result[record.DISPATCH_ID]["TABLE_DETAILS"].append(detail)

        # Serialize and return the grouped results
        return Response(list(result.values()), status=status.HTTP_200_OK)

class Truck_Productcode(viewsets.ModelViewSet):
    serializer_class = Truck_scanTableDetailSerializer

    def get_queryset(self):
        # Get URL parameters from kwargs
        dispatchid = self.kwargs.get('dispatchid', None)
        product_code = self.kwargs.get('productcode', None)
        serial_no = self.kwargs.get('serialno', None)

        # Base queryset for filtering
        queryset = Truck_scanModels.objects.all()

        # Apply filters if both product_code and serial_no are provided
        if dispatchid and product_code and serial_no:
            queryset = queryset.filter(DISPATCH_ID=dispatchid,PRODUCT_CODE=product_code, SERIAL_NO=serial_no)

        return queryset

    def list(self, request, *args, **kwargs):
        # Retrieve the filtered queryset
        queryset = self.get_queryset()

        # Serialize the queryset without pagination
        serializer = self.get_serializer(queryset, many=True)

        # Return the serialized data directly, without pagination structure
        return Response(serializer.data, status=status.HTTP_200_OK)

class Return_dispatchView(viewsets.ModelViewSet):
    queryset = WHRReturnDispatch.objects.all()
    serializer_class = Return_dispatchSerializer
    pagination_class = StandardResultsSetPagination

class filteredreturnView(viewsets.ModelViewSet):
    serializer_class = Return_dispatchSerializer
    pagination_class = None  # Disable pagination since we only return a count.

    def get_queryset(self):
        """
        Filter the queryset based on the request parameters.
        """
        queryset = WHRReturnDispatch.objects.exclude(RE_ASSIGN_STATUS='Re-Assign-Finished')
        
        reqid = self.kwargs.get('reqid')
        cusno = self.kwargs.get('cusno')
        cussite = self.kwargs.get('cussite')

        if reqid:
            queryset = queryset.filter(REQ_ID=reqid)
        if cusno:
            queryset = queryset.filter(CUSTOMER_NUMBER=cusno)
        if cussite:
            queryset = queryset.filter(CUSTOMER_SITE_ID=cussite)

        return queryset

    def list(self, request, *args, **kwargs):
        """
        Override the list method to return only the count of filtered data.
        """
        queryset = self.get_queryset()
        total_count = queryset.count()
        return Response({'total_count': total_count})

class PendingQtyView(APIView):
    def get(self, request, customer_no, customer_site_id):
        try:
            with connection.cursor() as cursor:
                # Raw SQL query to fetch invoice data with ITEM_CODE from a join
                cursor.execute("""
                    SELECT 
                        u.INVOICE_NUMBER, 
                        i.ITEM_CODE, 
                        i.DESCRIPTION, 
                        u.QUANTITY 
                    FROM BUYP.XXALJE_UNDELIVERED_DATA_BUYP1 u
                    JOIN BUYP.ALJE_ITEM_CATEGORIES_CPD_V i 
                        ON u.INVENTORY_ITEM_ID = i.INVENTORY_ITEM_ID
                    WHERE u.CUSTOMER_NUMBER = %s 
                    AND u.CUSTOMER_SITE_ID = %s
                """, [customer_no, customer_site_id])

                # Fetch data from the query
                invoices = cursor.fetchall()

            data = []

            for invoice in invoices:
                invoice_no, item_code,description, total_quantity = invoice

                # Calculate total dispatched quantity for the invoice
                dispatched_data = WHRCreateDispatch.objects.filter(
                    INVOICE_NUMBER=invoice_no, INVENTORY_ITEM_ID=item_code
                ).aggregate(total_dispatched=Sum('DISPATCHED_QTY'))

                dispatched_qty = dispatched_data['total_dispatched'] or 0
                pending_qty = total_quantity - dispatched_qty

                # Fetch trucking quantity from WHR_TRUCK_SCAN_DETAILS
                trucking_data = Truck_scanModels.objects.filter(
                    INVOICE_NO=invoice_no, ITEM_CODE=item_code
                ).aggregate(total_trucking=Sum('TRUCK_SEND_QTY'))

                trucking_qty = trucking_data['total_trucking'] or 0

                # Calculate onprogress_qty = dispatched_qty - trucking_qty
                onprogress_qty = dispatched_qty - trucking_qty

                data.append({
                    'invoice_no': invoice_no,
                    'item_code': item_code,
                    'description': description,
                    'total_quantity': total_quantity,
                    'dispatched_qty': dispatched_qty,
                    'trucking_qty': trucking_qty,
                    'onprogress_qty': onprogress_qty,
                    'pending_qty': pending_qty,
                })

            return Response({'customer_no': customer_no, 'customer_site_id': customer_site_id, 'pending_details': data})

        except Exception as e:
            return Response({'error': str(e)}, status=500)

class updatedfilteredProductcodeGetView(viewsets.ModelViewSet):
    queryset = ProductcodeGetModels.objects.all()
    serializer_class = ProductcodeGetserializers

    def get_queryset(self):
        """
        Optionally filter the queryset based on the `itemcode` passed in the URL.
        """
        queryset = ProductcodeGetModels.objects.all()
        itemcode = self.kwargs.get('itemcode', None)
        if itemcode:
            queryset = ProductcodeGetModels.objects.filter(ITEM_CODE=itemcode)
            if not queryset.exists():
                raise NotFound(detail="No item code found matching the provided ITEM_CODE.")
            
            # Check if any of the items have SERIAL_STATUS as 'N'
            for product in queryset:
                if product.SERIAL_STATUS == "N":
                    raise NotFound(detail="This product is a bypass product.")
            
            return queryset

        return ProductcodeGetModels.objects.none()

# @csrf_exempt
# def Generate_dispatch_details_print(request):
#     if request.method == 'GET':
#         try:
#             # Get all parameters
#             deliveryno = request.GET.get('deliveryno', '')
#             region = request.GET.get('region', '')
#             transportor_Name = request.GET.get('transportor_Name', '')
#             vehicleNo = request.GET.get('vehicleNo', '')
#             driverName = request.GET.get('driverName', '')
#             driverMobileNo = request.GET.get('driverMobileNo', '')
#             date = request.GET.get('date', '')
#             customerNo = request.GET.get('customerNo', '')
#             customername = request.GET.get('customername', '')
#             customersite = request.GET.get('customersite', '')
#             deliveryaddress = request.GET.get('deliveryaddress', '')
#             remmarks = request.GET.get('remmarks', '')
#             salesmanremmarks = request.GET.get('salesmanremmarks', '')
#             itemtotalqty = request.GET.get('itemtotalqty', '')
#             products_param = request.GET.get('products_param', '')

#             # Decode and parse product data
#             decoded_products = urllib.parse.unquote(products_param)
#             product_sets = re.findall(r'\{(.*?)\}', decoded_products)

#             grouped_products = {}

#             for product_set in product_sets:
#                 parts = product_set.split('|')
#                 if len(parts) == 6:
#                     s_no, inv_No, item_code, item_details, productcode, serialno = map(str.strip, parts)

#                     # Treat empty or null serial numbers as "NoSR00"
#                     if serialno.lower() == 'null' or serialno == '':
#                         serialno = 'NoSR00'

#                     key = f"{inv_No}|{item_code}|{item_details}|{productcode}"

#                     if key not in grouped_products:
#                         grouped_products[key] = {
#                             's_no': s_no,
#                             'inv_No': inv_No,
#                             'item_code': item_code,
#                             'item_details': item_details,
#                             'productcode': productcode,
#                             'serialnos': [],  # full list for display
#                             'unique_serialnos': set()
#                         }

#                     # Custom logic to add "NoSR00" only once
#                     if serialno == "NoSR00":
#                         if "NoSR00" not in grouped_products[key]['serialnos']:
#                             grouped_products[key]['serialnos'].append(serialno)
#                             grouped_products[key]['unique_serialnos'].add(serialno)
#                     else:
#                         grouped_products[key]['serialnos'].append(serialno)
#                         grouped_products[key]['unique_serialnos'].add(serialno)

#             # Prepare data for template
#             items = [{
#                 's_no': p['s_no'],
#                 'inv_No': p['inv_No'],
#                 'item_code': p['item_code'],
#                 'item_details': p['item_details'],
#                 'productcode': p['productcode'],
#                 'serialnos': ', '.join(sorted(p['serialnos'])),
#                 'grouped_serialnos': sorted(p['unique_serialnos']),
#                 'total_serialnos': len(p['serialnos']),  # includes repeated, except NoSR00 only once
#             } for p in grouped_products.values()]

#             # Ensure remmarks and deliveryaddress have default values if None or empty
#             remmarks = remmarks if remmarks not in [None, 'null', 'None'] else ''
#             deliveryaddress = deliveryaddress if deliveryaddress not in [None, 'null', 'None'] else ''
#             salesmanremmarks = salesmanremmarks if salesmanremmarks not in [None, 'null', 'None'] else ''
#             context = {
#                 'deliveryno': deliveryno,
#                 'date': date,
#                 'region': region,
#                 'transportor_Name': transportor_Name,
#                 'vehicleNo': vehicleNo,
#                 'driverName': driverName,
#                 'driverMobileNo': driverMobileNo,
#                 'customerNo': customerNo,
#                 'customername': customername,
#                 'customersite': customersite,
#                 'deliveryaddress': deliveryaddress,
#                 'salesmanremmarks': salesmanremmarks,
#                 'remmarks': remmarks,
#                 'itemtotalqty': itemtotalqty,
#                 'items': items,
#             }

#             return render(request, 'Generate_dispatch_details_print.html', context)

#         except Exception as e:
#             return JsonResponse({'error': str(e)}, status=400)

#     return JsonResponse({'error': 'Invalid request method'}, status=405)



@csrf_exempt
def Generate_dispatch_details_print(request):
    if request.method == 'GET':
        try:
            # Extract query parameters
            deliveryno = request.GET.get('deliveryno', '')
            region = request.GET.get('region', '')
            pickid = request.GET.get('pickid', '')
            pickmanname = request.GET.get('pickmanname', '')
            transportor_Name = request.GET.get('transportor_Name', '')
            vehicleNo = request.GET.get('vehicleNo', '')
            driverName = request.GET.get('driverName', '')
            driverMobileNo = request.GET.get('driverMobileNo', '')
            date = request.GET.get('date', '')
            customerNo = request.GET.get('customerNo', '')
            customername = request.GET.get('customername', '')
            customersite = request.GET.get('customersite', '')
            deliveryaddress = request.GET.get('deliveryaddress', '')
            remmarks = request.GET.get('remmarks', '')
            salesmanremmarks = request.GET.get('salesmanremmarks', '')
            itemtotalqty = request.GET.get('itemtotalqty', '')
            products_param = request.GET.get('products_param', '')

            # Decode and extract product sets
            decoded_products = urllib.parse.unquote(products_param)
            product_sets = re.findall(r'\{(.*?)\}', decoded_products)

            grouped_products = {}

            for product_set in product_sets:
                parts = product_set.split('|')
                if len(parts) == 6:
                    s_no, inv_No, item_code, item_details, productcode, serialno = map(str.strip, parts)

                    # Treat null/empty serial numbers as NoSR00
                    if serialno.lower() == 'null' or serialno == '':
                        serialno = 'NoSR00'

                    key = f"{inv_No}|{item_code}|{item_details}|{productcode}"

                    if key not in grouped_products:
                        grouped_products[key] = {
                            's_no': s_no,
                            'inv_No': inv_No,
                            'item_code': item_code,
                            'item_details': item_details,
                            'productcode': productcode,
                            'serialnos_raw': [],      # All serials for count
                            'serialnos_display': set() # Unique for display (NoSR00 once)
                        }

                    grouped_products[key]['serialnos_raw'].append(serialno)

                    # Add to display set (NoSR00 only once)
                    if serialno == 'NoSR00':
                        grouped_products[key]['serialnos_display'].add('NoSR00')
                    else:
                        grouped_products[key]['serialnos_display'].add(serialno)

            # Final item list to render
            items = []
            for p in grouped_products.values():
                items.append({
                    's_no': p['s_no'],
                    'inv_No': p['inv_No'],
                    'item_code': p['item_code'],
                    'item_details': p['item_details'],
                    'productcode': p['productcode'],
                    'serialnos': ', '.join(sorted(p['serialnos_display'])),  # show each only once
                    'grouped_serialnos': sorted(p['serialnos_display']),
                    'total_serialnos': len(p['serialnos_raw']),  # full count including repeated NoSR00
                })

            # Default fallback for empty/null fields
            remmarks = '' if remmarks in ['null', 'None', None, ''] else remmarks
            salesmanremmarks = '' if salesmanremmarks in ['null', 'None', None, ''] else salesmanremmarks
            deliveryaddress = '' if deliveryaddress in ['null', 'None', None, ''] else deliveryaddress

            # Context to render template
            context = {
                'deliveryno': deliveryno,
                'date': date,
                'region': region,
                'transportor_Name': transportor_Name,    
                'pickid': pickid,
                'pickmanname': pickmanname,
                'vehicleNo': vehicleNo,
                'driverName': driverName,
                'driverMobileNo': driverMobileNo,
                'customerNo': customerNo,
                'customername': customername,
                'customersite': customersite,
                'deliveryaddress': deliveryaddress,
                'salesmanremmarks': salesmanremmarks,
                'remmarks': remmarks,
                'itemtotalqty': itemtotalqty,
                'items': items,
            }

            return render(request, 'Generate_dispatch_details_print.html', context)

        except Exception as e:
            return JsonResponse({'error': str(e)}, status=400)

    return JsonResponse({'error': 'Invalid request method'}, status=405)

# def Generate_dispatch_print(request, deliveryno, region, transportor_Name, vehicleNo, driverName, driverMobileNo, date, customerNo, customername, customersite, deliveryaddress, remmarks,salesmanremmarks, itemtotalqty, products_param):
#     # Decode the product parameters
#     decoded_products = urllib.parse.unquote(products_param)
    
#     # Regular expression to extract product sets enclosed in curly braces
#     product_sets = re.findall(r'\{(.*?)\}', decoded_products)

#     items = []
#     for product_set in product_sets:
#         # Split product details using '|'
#         parts = product_set.split('|')
        
#         if len(parts) == 5:  # Ensure each product entry has exactly 5 parts
#             s_no = parts[0].strip()
#             inv_No = parts[1].strip()
#             item_code = parts[2].strip()
#             item_details = parts[3].strip()
#             qty = parts[4].strip()

#             # Append parsed product details to the items list
#             items.append({
#                 's_no': s_no,
#                 'inv_No': inv_No,
#                 'item_code': item_code,
#                 'item_details': item_details,
#                 'qty': qty,
#             })

            
#     # Ensure remmarks and deliveryaddress have default values if None or empty
#     remmarks = remmarks if remmarks not in [None, 'null', 'None'] else ''
#     deliveryaddress = deliveryaddress if deliveryaddress not in [None, 'null', 'None'] else ''
#     salesmanremmarks = salesmanremmarks if salesmanremmarks not in [None, 'null', 'None'] else ''
    
#     context = {
#         'deliveryno': deliveryno,
#         'date': date,
#         'region': region,
#         'transportor_Name': transportor_Name,
#         'vehicleNo': vehicleNo,
#         'driverName': driverName, 
#         'driverMobileNo': driverMobileNo,
#         'customerNo': customerNo,
#         'customername': customername,
#         'customersite': customersite,
#         'deliveryaddress': deliveryaddress,
#         'salesmanremmarks':salesmanremmarks,
#         'remmarks': remmarks,
#         'itemtotalqty':itemtotalqty,
#         'items': items,

#     }
    
#     return render(request, 'Generate_dispatch_print.html', context)


@csrf_exempt
def Generate_dispatch_print(request):
    if request.method == 'GET':
        try:
            # Extract query parameters
            deliveryno = request.GET.get('deliveryno', '')
            region = request.GET.get('region', '')
            transportor_Name = request.GET.get('transportor_Name', '')
            pickid = request.GET.get('pickid', '')
            pickmanname = request.GET.get('pickmanname', '')
            vehicleNo = request.GET.get('vehicleNo', '')
            driverName = request.GET.get('driverName', '')
            driverMobileNo = request.GET.get('driverMobileNo', '')
            date = request.GET.get('date', '')
            customerNo = request.GET.get('customerNo', '')
            customername = request.GET.get('customername', '')
            customersite = request.GET.get('customersite', '')
            deliveryaddress = request.GET.get('deliveryaddress', '')
            remmarks = request.GET.get('remmarks', '')
            salesmanremmarks = request.GET.get('salesmanremmarks', '')
            itemtotalqty = request.GET.get('itemtotalqty', '')
            products_param = request.GET.get('products_param', '')

            # Decode and extract product sets
            decoded_products = urllib.parse.unquote(products_param)
            product_sets = re.findall(r'\{(.*?)\}', decoded_products)

            items = []

            for product_set in product_sets:
                parts = product_set.split('|')
                if len(parts) == 5:
                    sno, invoiceno, itemcode, itemdetails, sendqty = map(str.strip, parts)
                    items.append({
                        's_no': sno,
                        'inv_No': invoiceno,
                        'item_code': itemcode,
                        'item_details': itemdetails,
                        'sendqty': sendqty,
                    })

            # Default fallback for null/empty values
            remmarks = '' if remmarks in ['null', 'None', None, ''] else remmarks
            salesmanremmarks = '' if salesmanremmarks in ['null', 'None', None, ''] else salesmanremmarks
            deliveryaddress = '' if deliveryaddress in ['null', 'None', None, ''] else deliveryaddress

            # Context to render the template
            context = {
                'deliveryno': deliveryno,
                'date': date,
                'region': region,
                'transportor_Name': transportor_Name,
                'pickid': pickid,
                'pickmanname': pickmanname,
                'vehicleNo': vehicleNo,
                'driverName': driverName,
                'driverMobileNo': driverMobileNo,
                'customerNo': customerNo,
                'customername': customername,
                'customersite': customersite,
                'deliveryaddress': deliveryaddress,
                'salesmanremmarks': salesmanremmarks,
                'remmarks': remmarks,
                'itemtotalqty': itemtotalqty,
                'items': items,
            }
            return render(request, 'Generate_dispatch_print.html', context)

        except Exception as e:
            return JsonResponse({'error': str(e)}, status=400)

    return JsonResponse({'error': 'Invalid request method'}, status=405)

# def Generate_picking_print(request, pickid, reqno, region, pickmanname, deliveryaddress, date, customerNo, customername, customersite, itemtotalqty, products_param):
#     # Decode the product parameters
#     decoded_products = urllib.parse.unquote(products_param)
    
#     # Parse the product details into a list of dictionaries
#     items = []
#     product_sets = re.findall(r'\{(.*?)\}', decoded_products) # Split by comma to get each product entry
    
#     for product in product_sets:
#         # Split each product entry into its components using '|'
#         parts = product.split('|')
#         if len(parts) == 5:  # Ensure the product entry has exactly 5 parts
#             s_no = parts[0]
#             inv_No = parts[1]
#             item_code = parts[2]
#             item_details = parts[3]
#             qty = parts[4]
#             # Append the parsed product details to the items list
#             items.append({
#                 's_no': s_no,
#                 'inv_No': inv_No,
#                 'item_code': item_code,
#                 'item_details': item_details,
#                 'qty': qty,
#             })
    
#     context = {
        
#         'pickid': pickid,
#         'reqno': reqno,
#         'date': date,
#         'pickmanname': pickmanname,
#         'deliveryaddress': deliveryaddress,
#         'region': region,
#         'customerNo': customerNo,
#         'customername': customername,
#         'customersite': customersite,
#         'itemtotalqty':itemtotalqty,
#         'items': items,

#     }
    
#     return render(request, 'Generate_picking_print.html', context)


@csrf_exempt
def Generate_picking_print(request):
    if request.method == 'GET':
        try:
            # Parse JSON body
           

            pickid = request.GET.get("pickid")
            reqno = request.GET.get("reqno")
            region = request.GET.get("region")
            pickmanname = request.GET.get("pickmanname")
            deliveryaddress = request.GET.get("deliveryaddress")
            date = request.GET.get("date")
            customerNo = request.GET.get("customerNo")
            customername = request.GET.get("customername")
            customersite = request.GET.get("customersite")
            itemtotalqty = request.GET.get("itemtotalqty")
            products_param = request.GET.get("products_param", "")

            # Decode and parse product details
            decoded_products = urllib.parse.unquote(products_param)
            items = []
            product_sets = re.findall(r'\{(.*?)\}', decoded_products)

            for product in product_sets:
                parts = product.split('|')
                if len(parts) == 5:
                    s_no, inv_No, item_code, item_details, qty = parts
                    items.append({
                        's_no': s_no,
                        'inv_No': inv_No,
                        'item_code': item_code,
                        'item_details': item_details,
                        'qty': qty,
                    })

            context = {
                'pickid': pickid,
                'reqno': reqno,
                'date': date,
                'pickmanname': pickmanname,
                'deliveryaddress': deliveryaddress,
                'region': region,
                'customerNo': customerNo,
                'customername': customername,
                'customersite': customersite,
                'itemtotalqty': itemtotalqty,
                'items': items,
            }

            return render(request, 'Generate_picking_print.html', context)

        except Exception as e:
            return JsonResponse({"error": str(e)}, status=400)

    return JsonResponse({"error": "Only POST method allowed"}, status=405)

# @csrf_exempt
# def Return_invoice_print(request):
#     # ✅ Allow only POST requests
#     if request.method != "POST":
#         return JsonResponse({"error": "Only POST method is allowed"}, status=405)

#     try:
#         # Parse JSON body
#         body = json.loads(request.body.decode('utf-8'))

#         uniqulastreqno = body.get("uniqulastreqno")
#         remarks = body.get("remarks")
#         superuserno = body.get("superuserno")
#         superusername = body.get("superusername")
#         orgid = body.get("orgid")
#         date = body.get("date")
#         salesmano = body.get("salesmano")
#         customerNo = body.get("customerNo")
#         customername = body.get("customername")
#         customersite = body.get("customersite")
#         invoiceno = body.get("invoiceno")
#         itemtotalqty = body.get("itemtotalqty")
#         products_param = body.get("products_param", "")

#         # 🔍 Decode and parse product parameters
#         decoded_products = urllib.parse.unquote(products_param)
#         product_sets = re.findall(r'\{(.*?)\}', decoded_products)

#         items = []
#         for product_set in product_sets:
#             parts = product_set.split('|')
#             if len(parts) == 5:
#                 s_no = parts[0].strip()
#                 item_code = parts[2].strip()
#                 item_details = parts[3].strip()
#                 qty = parts[4].strip()

#                 items.append({
#                     's_no': s_no,
#                     'item_code': item_code,
#                     'item_details': item_details,
#                     'qty': qty,
#                 })

#         # 🔍 Fetch region name
#         region_name = ""
#         with connection.cursor() as cursor:
#             cursor.execute("""
#                 SELECT TOP 1 NAME 
#                 FROM [BUYP].[BUYP].[ALJE_REGIONS] 
#                 WHERE ORGANIZATION_ID = %s
#             """, [orgid])
#             row = cursor.fetchone()
#             if row:
#                 region_name = row[0]

#         # 🔍 Fetch salesman name using SALESREP_NUMBER
#         salesman_name_fetched = ""
#         with connection.cursor() as cursor:
#             cursor.execute("""
#                 SELECT TOP 1 NAME 
#                 FROM [BUYP].[BUYP].[ALJE_SALESREP] 
#                 WHERE SALESREP_NUMBER = %s
#             """, [salesmano])
#             row = cursor.fetchone()
#             if row:
#                 salesman_name_fetched = row[0]

#         context = {
#             'uniqulastreqno': uniqulastreqno,
#             'remarks': remarks,
#             'superuserno': superuserno,
#             'superusername': superusername,
#             'region': region_name,
#             'date': date,
#             'salesmano': salesmano,
#             'salesmanoname': salesman_name_fetched,
#             'customerNo': customerNo,
#             'customername': customername,
#             'customersite': customersite,
#             'invoiceno': invoiceno,
#             'itemtotalqty': itemtotalqty,
#             'items': items,
#         }

#         return render(request, 'Return_invoce.html', context)

#     except Exception as e:
#         return JsonResponse({"error": str(e)}, status=400)




def Return_invoice_print(request):
    if request.method == 'GET':
        try:
            # Get parameters from query string
            uniqulastreqno = request.GET.get("uniqulastreqno")
            remarks = request.GET.get("remarks")
            superuserno = request.GET.get("superuserno")
            superusername = request.GET.get("superusername")
            orgid = request.GET.get("orgid")  # may be "ORG45" or "45"
            date = request.GET.get("date")
            salesmano = request.GET.get("salesmano")
            customerNo = request.GET.get("customerNo")
            customername = request.GET.get("customername")
            customersite = request.GET.get("customersite")
            invoiceno = request.GET.get("invoiceno")
            itemtotalqty = request.GET.get("itemtotalqty")
            products_param = request.GET.get("products_param")

            # Decode products
            decoded_products = urllib.parse.unquote(products_param or "")
            product_sets = re.findall(r'\{(.*?)\}', decoded_products)

            items = []
            for product_set in product_sets:
                parts = product_set.split('|')
                if len(parts) == 5:
                    s_no = parts[0].strip()
                    item_code = parts[2].strip()
                    item_details = parts[3].strip()
                    qty = parts[4].strip()

                    items.append({
                        's_no': s_no,
                        'item_code': item_code,
                        'item_details': item_details,
                        'qty': qty,
                    })

            # 🟢 Clean orgid (convert to int if numeric)
            clean_orgid = None
            if orgid:
                match = re.search(r'(\d+)', orgid)  # extract number if "ORG45"
                if match:
                    clean_orgid = int(match.group(1))

            # Fetch region name
            region_name = ""
            if clean_orgid:
                with connection.cursor() as cursor:
                    cursor.execute("""
                        SELECT TOP 1 NAME 
                        FROM [BUYP].[BUYP].[ALJE_REGIONS] 
                        WHERE ORGANIZATION_ID = %s
                    """, [clean_orgid])
                    row = cursor.fetchone()
                    if row:
                        region_name = row[0]

            # Fetch salesman name
            salesman_name_fetched = ""
            if salesmano:
                with connection.cursor() as cursor:
                    cursor.execute("""
                        SELECT TOP 1 NAME 
                        FROM [BUYP].[BUYP].[ALJE_SALESREP] 
                        WHERE SALESREP_NUMBER = %s
                    """, [salesmano])
                    row = cursor.fetchone()
                    if row:
                        salesman_name_fetched = row[0]

            # ✅ Clean remarks safely
            remarks = '' if remarks in ['null', 'None', None, ''] else remarks

            context = {
                'uniqulastreqno': uniqulastreqno,
                'remarks': remarks,
                'superuserno': superuserno,
                'superusername': superusername,
                'region': region_name,
                'date': date,
                'salesmano': salesmano,
                'salesmanoname': salesman_name_fetched,
                'customerNo': customerNo,
                'customername': customername,
                'customersite': customersite,
                'invoiceno': invoiceno,
                'itemtotalqty': itemtotalqty,
                'items': items,
            }

            return render(request, 'Return_invoce.html', context)
        except Exception as e:
            return JsonResponse({"error": str(e)}, status=400)

    return JsonResponse({"error": "Only GET method allowed"}, status=405)

    
class InvoiceReturnSummaryView(APIView):
    def get(self, request):
        try:
            # Raw SQL to group by INVOICE_RETURN_ID and sum RETURNED_QTY
            query = """
                SELECT 
                    INVOICE_RETURN_ID,
                    DATE,
                    ORG_ID,
                    ORG_NAME,
                    MANAGER_NO,
                    MANAGER_NAME,
                    SALESMANO_NO,
                    SALESMAN_NAME,
                    CUSTOMER_NUMBER,
                    CUSTOMER_NAME,
                    CUSTOMER_SITE_ID,
                    INVOICE_NUMBER,
                    REMARKS,
                    SUM(TRY_CAST(RETURNED_QTY AS INT)) AS TOTAL_RETURNED_QTY
                FROM [BUYP].[dbo].[WHR_INVOICE_RETURN_HISTORY_TBL]
                GROUP BY 
                    INVOICE_RETURN_ID,
                    DATE,
                    ORG_ID,
                    ORG_NAME,
                    MANAGER_NO,
                    MANAGER_NAME,
                    SALESMANO_NO,
                    SALESMAN_NAME,
                    CUSTOMER_NUMBER,
                    CUSTOMER_NAME,
                    CUSTOMER_SITE_ID,
                    INVOICE_NUMBER,
                    REMARKS
                ORDER BY DATE ASC

            """

            with connection.cursor() as cursor:
                cursor.execute(query)
                rows = cursor.fetchall()

            # Column names
            columns = [
                'INVOICE_RETURN_ID', 'DATE', 'ORG_ID', 'ORG_NAME',
                'MANAGER_NO', 'MANAGER_NAME', 'SALESMANO_NO', 'SALESMAN_NAME',
                'CUSTOMER_NUMBER', 'CUSTOMER_NAME', 'CUSTOMER_SITE_ID',
                'INVOICE_NUMBER', 'REMARKS', 'TOTAL_RETURNED_QTY'
            ]

            result = [dict(zip(columns, row)) for row in rows]

            # Apply pagination
            paginator = TruckResultsPagination()
            paginated_result = paginator.paginate_queryset(result, request)
            return paginator.get_paginated_response(paginated_result)

        except Exception as e:
            return Response({"error": str(e)}, status=status.HTTP_400_BAD_REQUEST)


class Exported_InvoiceReturnSummaryView(APIView):
    def get(self, request):
        try:
            # Get from/to dates from query params (format: 12-Sep-2025)
            from_date = request.query_params.get("from_date")
            to_date = request.query_params.get("to_date")

            # Validate and format dates
            date_filter = ""
            params = []
            if from_date and to_date:
                try:
                    # Parse from dd-MMM-yyyy (e.g. 12-Sep-2025)
                    from_date_obj = datetime.strptime(from_date, "%d-%b-%Y")
                    to_date_obj = datetime.strptime(to_date, "%d-%b-%Y")

                    # Convert to SQL-friendly YYYY-MM-DD
                    from_date_str = from_date_obj.strftime("%Y-%m-%d")
                    to_date_str = to_date_obj.strftime("%Y-%m-%d")

                    date_filter = "WHERE DATE BETWEEN %s AND %s"
                    params.extend([from_date_str, to_date_str])
                except ValueError:
                    return Response(
                        {"error": "Invalid date format. Use dd-MMM-yyyy (e.g. 12-Sep-2025)"},
                        status=status.HTTP_400_BAD_REQUEST,
                    )

            # Raw SQL with optional date filter
            query = f"""
                SELECT 
                    INVOICE_RETURN_ID,
                    DATE,
                    ORG_ID,
                    ORG_NAME,
                    CAST(MANAGER_NO AS BIGINT) AS MANAGER_NO,
                    MANAGER_NAME,
                    CAST(SALESMANO_NO AS BIGINT) AS SALESMANO_NO,
                    SALESMAN_NAME,
                    CAST(CUSTOMER_NUMBER AS BIGINT) AS CUSTOMER_NUMBER,
                    CUSTOMER_NAME,
                    CAST(CUSTOMER_SITE_ID AS BIGINT) AS CUSTOMER_SITE_ID,
                    CAST(INVOICE_NUMBER AS BIGINT) AS INVOICE_NUMBER,
                    ITEM_CODE,
                    ITEM_DESCRIPTION,
                    REMARKS,
                    SUM(TRY_CAST(RETURNED_QTY AS INT)) AS TOTAL_RETURNED_QTY
                FROM [BUYP].[dbo].[WHR_INVOICE_RETURN_HISTORY_TBL]
                {date_filter}
                GROUP BY 
                    INVOICE_RETURN_ID,
                    DATE,
                    ORG_ID,
                    ORG_NAME,
                    MANAGER_NO,
                    MANAGER_NAME,
                    SALESMANO_NO,
                    SALESMAN_NAME,
                    CUSTOMER_NUMBER,
                    CUSTOMER_NAME,
                    CUSTOMER_SITE_ID,
                    INVOICE_NUMBER,
                    ITEM_CODE,
                    ITEM_DESCRIPTION,
                    REMARKS
                ORDER BY DATE ASC
            """


            with connection.cursor() as cursor:
                cursor.execute(query, params)
                rows = cursor.fetchall()

            # Column names
            columns = [
                'INVOICE_RETURN_ID', 'DATE', 'ORG_ID', 'ORG_NAME',
                'MANAGER_NO', 'MANAGER_NAME', 'SALESMANO_NO', 'SALESMAN_NAME',
                'CUSTOMER_NUMBER', 'CUSTOMER_NAME', 'CUSTOMER_SITE_ID',
                'INVOICE_NUMBER', 'ITEM_CODE', 'ITEM_DESCRIPTION',
                'REMARKS', 'TOTAL_RETURNED_QTY'
            ]

            result = [dict(zip(columns, row)) for row in rows]

            # Apply pagination
            paginator = TruckResultsPagination()
            paginated_result = paginator.paginate_queryset(result, request)
            return paginator.get_paginated_response(paginated_result)

        except Exception as e:
            return Response({"error": str(e)}, status=status.HTTP_400_BAD_REQUEST)



class FilterInvoiceReturnDetails(APIView):
    def get(self, request, invoice_return_id):
        try:
            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT 
                        [id],
                        [INVOICE_RETURN_ID],
                        [DATE],
                        [ORG_ID],
                        [ORG_NAME],
                        [MANAGER_NO],
                        [MANAGER_NAME],
                        [SALESMANO_NO],
                        [SALESMAN_NAME],
                        [CUSTOMER_NUMBER],
                        [CUSTOMER_NAME],
                        [CUSTOMER_SITE_ID],
                        [INVOICE_NUMBER],
                        [CUSTOMER_TRX_ID],
                        [CUSTOMER_TRX_LINE_ID],
                        [LINE_NUMBER],
                        [ITEM_CODE],
                        [ITEM_DESCRIPTION],
                        [TOT_QUANTITY],
                        [DISPATCHED_QTY],
                        [RETURNED_QTY],
                        [FLAG_STATUS]
                    FROM [BUYP].[dbo].[WHR_INVOICE_RETURN_HISTORY_TBL]
                    WHERE INVOICE_RETURN_ID = %s
                """, [invoice_return_id])

                columns = [col[0] for col in cursor.description]
                result = [dict(zip(columns, row)) for row in cursor.fetchall()]

            return Response(result, status=status.HTTP_200_OK)

        except Exception as e:
            return Response({"error": str(e)}, status=status.HTTP_400_BAD_REQUEST)

class CreateDispatchWithTruckScanView(APIView):
    pagination_class = StandardResultsSetPagination()

    def get(self, request):
        # Get filtering parameters from the request
        req_id = request.query_params.get("REQ_ID", None)
        customer_number = request.query_params.get("CUSTOMER_NUMBER", None)
        customer_site_id = request.query_params.get("CUSTOMER_SITE_ID", None)
        invoice_number = request.query_params.get("INVOICE_NUMBER", None)
        inventory_item_id = request.query_params.get("INVENTORY_ITEM_ID", None)

        # Subquery to sum TRUCK_SEND_QTY with explicit output field
        truck_send_qty_subquery = Truck_scanModels.objects.filter(
            REQ_ID=OuterRef('REQ_ID'),
            CUSTOMER_NUMBER=OuterRef('CUSTOMER_NUMBER'),
            CUSTOMER_SITE_ID=OuterRef('CUSTOMER_SITE_ID'),
            INVOICE_NO=OuterRef('INVOICE_NUMBER'),
            ITEM_CODE=OuterRef('INVENTORY_ITEM_ID')
        ).values('REQ_ID').annotate(
            total_qty=Coalesce(Sum('TRUCK_SEND_QTY'), Value(0), output_field=IntegerField())
        ).values('total_qty')

        # Filter only necessary records from WHRCreateDispatch
        dispatch_queryset = WHRCreateDispatch.objects.annotate(
            total_truck_send_qty=Coalesce(
                Subquery(truck_send_qty_subquery, output_field=IntegerField()), 
                Value(0), 
                output_field=IntegerField()
            ),
            dispatched_qty_int=Cast('DISPATCHED_QTY', IntegerField()),
            remaining_qty=Coalesce(
                Cast('DISPATCHED_QTY', IntegerField()) - Coalesce(Subquery(truck_send_qty_subquery), Value(0)),
                Value(0), 
                output_field=IntegerField()
            )
        ).values(
            'REQ_ID',
            'PHYSICAL_WAREHOUSE',
            'ORG_ID',
            'ORG_NAME',
            'COMMERCIAL_NO',
            'COMMERCIAL_NAME',
            'SALESMAN_NO',
            'SALESMAN_NAME',
            'CUSTOMER_NUMBER',
            'CUSTOMER_NAME',
            'CUSTOMER_SITE_ID',
            'INVOICE_DATE',
            'INVOICE_NUMBER',
            'CUSTOMER_TRX_ID',
            'CUSTOMER_TRX_LINE_ID',
            'LINE_NUMBER',
            'INVENTORY_ITEM_ID',
            'ITEM_DESCRIPTION',
            'TOT_QUANTITY',
            'DISPATCHED_QTY',
            'BALANCE_QTY',
            'DISPATCHED_BY_MANAGER',
            'CREATION_DATE',
            'CREATED_BY',
            'CREATED_IP',
            'CREATED_MAC',
            'LAST_UPDATE_DATE',
            'LAST_UPDATED_BY',
            'LAST_UPDATE_IP',
            'FLAG',
            'DELIVERYADDRESS',
            'REMARKS',
            'DELIVERY_DATE',
            'total_truck_send_qty',
            'remaining_qty'
        )

        # Apply filters based on request parameters
        if req_id:
            dispatch_queryset = dispatch_queryset.filter(REQ_ID=req_id)
        if customer_number:
            dispatch_queryset = dispatch_queryset.filter(CUSTOMER_NUMBER=customer_number)
        if customer_site_id:
            dispatch_queryset = dispatch_queryset.filter(CUSTOMER_SITE_ID=customer_site_id)
        if invoice_number:
            dispatch_queryset = dispatch_queryset.filter(INVOICE_NUMBER=invoice_number)
        if inventory_item_id:
            dispatch_queryset = dispatch_queryset.filter(INVENTORY_ITEM_ID=inventory_item_id)

        # Apply pagination
        paginator = self.pagination_class
        result_page = paginator.paginate_queryset(dispatch_queryset, request)
        return paginator.get_paginated_response(result_page)
    
class InvoiceReportsUndeliveredDataView(viewsets.ViewSet):
    """
    A ViewSet for listing undelivered data using a custom SQL query with filters.
    """
    pagination_class = StandardResultsSetPagination

    def list(self, request, status, salesman_no, columnname, columnvalue, fromdate, todate):
        try:
            # Convert string dates to Python date format
            from_date = datetime.strptime(fromdate, "%Y-%m-%d").date()
            to_date = datetime.strptime(todate, "%Y-%m-%d").date() + timedelta(days=1)  # Include end date properly

            # Table and model mapping based on status
            table_mapping = {
                "pending": "BUYP.XXALJE_UNDELIVERED_DATA_BUYP1",
                "ongoing": "WHR_CREATE_DISPATCH",
                "finished": "WHR_TRUCK_SCAN_DETAILS"
            }

            model_mapping = {
                "pending": None,  # No model available for raw SQL table
                "ongoing": WHRCreateDispatch,
                "finished": Truck_scanModels
            }

            serializer_mapping = {
                "pending": UndeliveredDataSerializer,
                "ongoing": create_Dispatchserializers,
                "finished": Truck_scanserializers
            }

            # Validate table name and serializer
            table_name = table_mapping.get(status.lower())
            model_class = model_mapping.get(status.lower())
            serializer_class = serializer_mapping.get(status.lower())

            if not table_name or not serializer_class:
                return Response({"error": "Invalid status parameter"}, status=status.HTTP_400_BAD_REQUEST)
        
        except ValueError:
            return Response({"error": "Invalid date format. Use YYYY-MM-DD."}, status=status.HTTP_400_BAD_REQUEST)

        try:
            if model_class:
                # Query the database using Django ORM for `ongoing` and `finished` statuses
                filters = {columnname: columnvalue, "CREATION_DATE__gte": from_date, "CREATION_DATE__lt": to_date}
                if salesman_no.strip():  # Check if salesman_no is not empty
                    filters["SALESMAN_NO"] = salesman_no
                
                queryset = model_class.objects.filter(**filters).values()

                # Convert datetime fields to string format "YYYY-MM-DD HH:MM:SS"
                for item in queryset:
                    for date_col in ['CREATION_DATE', 'DELIVERY_DATE', 'LAST_UPDATE_DATE']:
                        if date_col in item and isinstance(item[date_col], datetime):
                            item[date_col] = item[date_col].strftime("%Y-%m-%d %H:%M:%S")

            else:
                # Raw SQL query for `pending` data
                if salesman_no.strip():
                    query = f'''
                        SELECT * FROM {table_name}
                        WHERE SALESMAN_NO = %s 
                        AND {columnname} = %s 
                        AND CREATION_DATE >= %s
                        AND CREATION_DATE < %s  
                    '''
                    query_params = [salesman_no, columnvalue, from_date, to_date]
                else:
                    query = f'''
                        SELECT * FROM {table_name}
                        WHERE {columnname} = %s 
                        AND CREATION_DATE >= %s
                        AND CREATION_DATE < %s  
                    '''
                    query_params = [columnvalue, from_date, to_date]
                
                with connection.cursor() as cursor:
                    cursor.execute(query, query_params)
                    columns = [col[0].lower() for col in cursor.description]
                    rows = cursor.fetchall()

                # Convert result into a list of dictionaries with formatted dates
                queryset = []
                for row in rows:
                    row_dict = dict(zip(columns, row))
                    for date_col in ['invoice_date', 'creation_date', 'last_update_date']:
                        if date_col in row_dict and isinstance(row_dict[date_col], datetime):
                            row_dict[date_col] = row_dict[date_col].strftime("%Y-%m-%d %H:%M:%S")
                    queryset.append(row_dict)

            # Apply pagination
            paginator = StandardResultsSetPagination()
            paginated_queryset = paginator.paginate_queryset(queryset, request)

            # Serialize the paginated data
            serializer = serializer_class(paginated_queryset, many=True)

            # Return paginated response
            return paginator.get_paginated_response(serializer.data)

        except Exception as e:
            return Response({"error": f"Database error: {str(e)}"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


# class Shipment_detialsView(viewsets.ViewSet):
#     pagination_class = StandardResultsSetPagination

#     @action(detail=False, methods=["get"], url_path="(?P<transfer_type>[^/]+)/(?P<shipment_header_id>[^/]+)/(?P<warehouse_name>[^/]+)")
#     def by_shipment(self, request, transfer_type=None, shipment_header_id=None, warehouse_name=None):
#         transfer_type = transfer_type.lower()
#         table_name = "XXALJEBYP_INTERORG_TBL"

#         # Step 1: Get ORGANIZATION_IDs from warehouse name
#         try:
#             with connection.cursor() as cursor:
#                 cursor.execute("""
#                     SELECT ORGANIZATION_ID
#                     FROM BUYP.dbo.ALJE_PHYSICAL_WHR
#                     WHERE WAREHOUSE_NAME = %s
#                 """, [warehouse_name])

#                 org_ids = [str(row[0]) for row in cursor.fetchall()]

#             if not org_ids:
#                 return Response({"error": f"Invalid warehouse name '{warehouse_name}'."}, status=400)

#         except Exception as e:
#             return Response({"error": f"Error fetching organization ID: {str(e)}"}, status=500)

#         # Step 2: Determine which field to filter on based on transfer_type
#         if transfer_type == "shipment number":
#             shipment_field = "SHIPMENT_NUM"
#         elif transfer_type == "receipt number":
#             shipment_field = "RECEIPT_NUM"
#         else:
#             return Response({"error": "Invalid transfer type. Use 'Shipment Number' or 'Receipt Number'."}, status=400)

#         try:
#             with connection.cursor() as cursor:
#                 cursor.execute(f"""
#                     SELECT *
#                     FROM {table_name}
#                     WHERE {shipment_field} = %s
#                     ORDER BY LINE_NUM ASC;
#                 """, [shipment_header_id])

#                 columns = [col[0].lower() for col in cursor.description]
#                 rows = cursor.fetchall()

#             # Convert rows to list of dictionaries
#             queryset = [dict(zip(columns, row)) for row in rows]

#             if not queryset:
#                 return Response({"error": f"No records found for {transfer_type} '{shipment_header_id}'."}, status=404)

#             # Step 3: Check for organization match
#             filtered_queryset = [
#                 row for row in queryset if row.get("attribute5") in org_ids
#             ]

#             if not filtered_queryset:
#                 return Response({
#                     "results": [],
#                     "count": 0,
#                     "next": None,
#                     "previous": None,
#                     "Message": f"The {transfer_type} '{shipment_header_id}' belongs to a different warehouse."
#                 }, status=200)

#             # ✅ Step 4: Only return rows where SYS_QUANTITY_SHIPPED != PHY_QUANTITY_SHIPPED
#             unequal_rows = []
#             for row in filtered_queryset:
#                 try:
#                     sys_qty = float(row.get("sys_quantity_shipped", 0))
#                     phy_qty = float(row.get("phy_quantity_shipped", 0))
#                 except (TypeError, ValueError):
#                     sys_qty = 0
#                     phy_qty = 0

#                 if sys_qty != phy_qty:
#                     diff = sys_qty - phy_qty
#                     # Show as int if whole number, else one decimal place
#                     row["balance_quantity_shipped"] = int(diff) if diff.is_integer() else round(diff, 1)
#                     unequal_rows.append(row)

#             if not unequal_rows:
#                 return Response({
#                     "results": [],
#                     "count": 0,
#                     "next": None,
#                     "previous": None,
#                     "Message": f"There is no data to ship for {transfer_type} '{shipment_header_id}'."
#                 }, status=200)

#             # Step 5: Apply pagination to unequal_rows
#             paginator = StandardResultsSetPagination()
#             paginated_queryset = paginator.paginate_queryset(unequal_rows, request)
#             return paginator.get_paginated_response(paginated_queryset)

#         except Exception as e:
#             return Response({"error": f"Database error: {str(e)}"}, status=500)


class Shipment_detialsView(viewsets.ViewSet):
    pagination_class = StandardResultsSetPagination

    @action(detail=False, methods=["get"], url_path="(?P<transfer_type>[^/]+)/(?P<shipment_header_id>[^/]+)/(?P<warehouse_name>[^/]+)")
    def by_shipment(self, request, transfer_type=None, shipment_header_id=None, warehouse_name=None):
        transfer_type = transfer_type.lower()
        table_name = "XXALJEBYP_INTERORG_TBL"

        # Step 1: Get ORGANIZATION_IDs from warehouse name
        try:
            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT ORGANIZATION_ID
                    FROM BUYP.dbo.ALJE_PHYSICAL_WHR
                    WHERE WAREHOUSE_NAME = %s
                """, [warehouse_name])

                org_ids = [str(row[0]) for row in cursor.fetchall()]

            if not org_ids:
                return Response({"error": f"Invalid warehouse name '{warehouse_name}'."}, status=400)

        except Exception as e:
            return Response({"error": f"Error fetching organization ID: {str(e)}"}, status=500)

        # Step 2: Determine which field to filter on based on transfer_type
        if transfer_type == "shipment number":
            shipment_field = "SHIPMENT_NUM"
        elif transfer_type == "receipt number":
            shipment_field = "RECEIPT_NUM"
        else:
            return Response({"error": "Invalid transfer type. Use 'Shipment Number' or 'Receipt Number'."}, status=400)

        try:
            with connection.cursor() as cursor:
                cursor.execute(f"""
                    SELECT *
                    FROM {table_name}
                    WHERE {shipment_field} = %s
                    ORDER BY LINE_NUM ASC;
                """, [shipment_header_id])

                columns = [col[0].lower() for col in cursor.description]
                rows = cursor.fetchall()

            # Convert rows to list of dictionaries
            queryset = [dict(zip(columns, row)) for row in rows]

            if not queryset:
                return Response({"error": f"No records found for {transfer_type} '{shipment_header_id}'."}, status=404)

            # Step 3: Check for organization match
            filtered_queryset = [
                row for row in queryset if row.get("attribute5") in org_ids
            ]

            if not filtered_queryset:
                return Response({
                    "results": [],
                    "count": 0,
                    "next": None,
                    "previous": None,
                    "Message": f"The {transfer_type} '{shipment_header_id}' belongs to a different warehouse."
                }, status=200)

            # ✅ Step 4: Only return rows where SYS_QUANTITY_SHIPPED != PHY_QUANTITY_SHIPPED
            unequal_rows = []
            for row in filtered_queryset:
                # Treat NULL as 0
                sys_qty = float(row.get("sys_quantity_shipped") or 0)
                phy_qty = float(row.get("phy_quantity_shipped") or 0)

                if sys_qty != phy_qty:
                    diff = sys_qty - phy_qty
                    row["balance_quantity_shipped"] = int(diff) if diff.is_integer() else round(diff, 1)
                    unequal_rows.append(row)

            if not unequal_rows:
                return Response({
                    "results": [],
                    "count": 0,
                    "next": None,
                    "previous": None,
                    "Message": f"There is no data to ship for {transfer_type} '{shipment_header_id}'."
                }, status=200)

            # Step 5: Apply pagination to unequal_rows
            paginator = StandardResultsSetPagination()
            paginated_queryset = paginator.paginate_queryset(unequal_rows, request)
            return paginator.get_paginated_response(paginated_queryset)

        except Exception as e:
            return Response({"error": f"Database error: {str(e)}"}, status=500)


@api_view(['GET'])
def get_shipment_by_warehouse(request):
    warehouse_name = request.GET.get('warehousename')
    status = request.GET.get('status')  # Default to 'Not Received'

    if not warehouse_name:
        return Response({'error': 'Missing warehousename parameter'}, status=400)

    try:
        # Filter by warehouse_name and active_status, order by ID descending, limit to 1000
        shipments = ShimentDispatchModels.objects.filter(
            warehouse_name=warehouse_name,
            active_status=status
        ).order_by('-id')

        serializer = ShipmentDispatchSerializer(shipments, many=True)
        return Response(serializer.data)

    except Exception as e:
        return Response({'error': str(e)}, status=500)
    
@api_view(['GET'])
def get_shipment_by_receviedwarehouse(request):
    shipment_id = request.GET.get('shipment_id')

    if not shipment_id:
        return Response({'error': 'Missing shipment_id parameter'}, status=400)

    try:
        # Filter shipment by shipment_num and exclude already received
        shipments = ShimentDispatchModels.objects.filter(
            shipment_num=shipment_id
        
        ).order_by('-id')  # Order by ID in descending order

        if not shipments.exists():
            return Response({'message': 'No shipment found with the given shipment_id'}, status=404)

        serializer = ShipmentDispatchSerializer(shipments, many=True)
        return Response(serializer.data)

    except Exception as e:
        return Response({'error': str(e)}, status=500)

@api_view(['GET'])
def get_shipment_by_shipment_numwise_receviedwarehouse(request):
    warehousename = request.GET.get('warehousename')
    status = request.GET.get('status')  # 'Recevied' or 'Not Recevied'

    if not warehousename:
        return Response({'error': 'Missing warehousename parameter'}, status=400)

    # Step 1: Get ORGANIZATION_IDs for given warehouse name
    organization_ids = Physical_WarehouseModels.objects.filter(
        WAREHOUSE_NAME=warehousename
    ).values_list('ORGANIZATION_ID', flat=True)

    if not organization_ids:
        return Response({'error': 'No ORGANIZATION_IDs found for given warehousename'}, status=404)

    # Step 2: Get all shipments
    all_shipments = ShimentDispatchModels.objects.filter(
        to_warehouse_name__in=organization_ids
    ).order_by('shipment_num', 'shipment_id')

    if not all_shipments.exists():
        return Response({'message': 'No shipments found'}, status=404)

    # Step 3: Group by shipment_num
    grouped_shipments = {}

    for shipment in all_shipments:
        snum = shipment.shipment_num
        if snum not in grouped_shipments:
            grouped_shipments[snum] = {
                "organization_id": shipment.organization_id,
                "organization_code": shipment.organization_code,
                "organization_name": shipment.organization_name,
                "shipment_num": snum,
                "receipt_num": shipment.receipt_num,
                "shipped_date": shipment.shipped_date,
                "to_orgn_id": shipment.to_orgn_id,
                "to_orgn_code": shipment.to_orgn_code,
                "to_orgn_name": shipment.to_orgn_name,
                "shipment_ids_received": set(),
                "shipment_ids_unreceived": set(),
                "total_quantity_shipped_overall": 0,
            }

        # Classify by status
        if shipment.active_status == 'Received':
            grouped_shipments[snum]["shipment_ids_received"].add(shipment.shipment_id)
        else:
            grouped_shipments[snum]["shipment_ids_unreceived"].add(shipment.shipment_id)
            grouped_shipments[snum]["total_quantity_shipped_overall"] += shipment.quantity_shipped or 0

    # Step 4: Final result based on status
    result = []

    for shipment_data in grouped_shipments.values():
        count_received = len(shipment_data["shipment_ids_received"])
        count_unreceived = len(shipment_data["shipment_ids_unreceived"])

        # Filter logic based on 'status'
        if status == "Not Recevied" and count_unreceived == 0:
            continue
        elif status == "Received" and count_unreceived != 0:
            continue

        result.append({
            "organization_id": shipment_data["organization_id"],
            "organization_code": shipment_data["organization_code"],
            "organization_name": shipment_data["organization_name"],
            "shipment_num": shipment_data["shipment_num"],
            "receipt_num": shipment_data["receipt_num"],
            "shipped_date": shipment_data["shipped_date"],
            "to_orgn_id": shipment_data["to_orgn_id"],
            "to_orgn_code": shipment_data["to_orgn_code"],
            "to_orgn_name": shipment_data["to_orgn_name"],
            "distinct_shipment_id_count_unreceived": count_unreceived,
            "distinct_shipment_id_count_received": count_received,
            "total_quantity_shipped_overall": shipment_data["total_quantity_shipped_overall"],
        })

    return Response(result)

@api_view(['POST'])
def update_active_status_by_shipment_id(request):
    shipment_id = request.data.get('shipment_id')
    
    if not shipment_id:
        return Response({'error': 'Missing shipment_id in request'}, status=400)

    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                UPDATE [BUYP].[dbo].[WHR_SHIMENT_DISPATCH]
                SET ACTIVE_STATUS = 'Received'
                WHERE SHIPMENT_ID = %s
            """, [shipment_id])

        return Response({'message': f'ACTIVE_STATUS updated to "Received" for SHIPMENT_ID {shipment_id}'})
    
    except Exception as e:
        return Response({'error': str(e)}, status=500)

class GetUndeliveredDataColumnNameview(APIView):
    """
    A View for listing undelivered data column names based on status.
    """
    def get(self, request, status):
        # Define the mapping of status to table names and their respective columns to exclude
        table_mapping = {
            "pending": {
                "table_name": "XXALJE_UNDELIVERED_DATA_BUYP1",
                "exclude_columns": [
                    "TO_WAREHOUSE",
                    "ORG_ID",
                    "ORG_NAME",
                    "SALESMAN_NO",
                    "SALESMAN_NAME",
                    "CUSTOMER_NUMBER",
                    "CUSTOMER_NAME",
                    "CUSTOMER_TRX_ID",
                    "CUSTOMER_TRX_LINE_ID",
                    "INVOICE_DATE",
                    "INVOICE_NUMBER",
                    "LINE_NUMBER",
                    "INVENTORY_ITEM_ID",
                    "QUANTITY",
                    "DISPATCH_QTY",
                    # "AMOUNT",
                    "WAREHOUSE_ID",
                    "WAREHOUSE_NAME",
                    # "INV_ROW_ID",
                    # "RETURN_QTY",
                ]
            },
            "ongoing": {
                "table_name": "WHR_CREATE_DISPATCH",
                "exclude_columns": [
                    "REQ_ID",
                    "PHYSICAL_WAREHOUSE",
                    "ORG_ID",
                    "ORG_NAME",
                    "COMMERCIAL_NO",
                    "COMMERCIAL_NAME",
                    "SALESMAN_NO",
                    "SALESMAN_NAME",
                    "CUSTOMER_NUMBER",
                    "CUSTOMER_NAME",
                    "INVOICE_DATE",
                    "INVOICE_NUMBER",
                    "CUSTOMER_TRX_ID",
                    "CUSTOMER_TRX_LINE_ID",
                    "LINE_NUMBER",
                    "INVENTORY_ITEM_ID",
                    "ITEM_DESCRIPTION",
                    "TOT_QUANTITY",
                    "DISPATCHED_QTY",
                    # "BALANCE_QTY",
                    # "DISPATCHED_BY_MANAGER",
                    # "TRUCK_SCAN_QTY",
                    # "DELIVERYADDRESS",
                    # "DELIVERY_DATE",
                    # "UNDEL_ID"
                ]  # Add columns to exclude if any
            },
            "finished": {
                "table_name": "WHR_TRUCK_SCAN_DETAILS",
                "exclude_columns": [
                    # "ID",
                    "DISPATCH_ID",
                    # "REQ_ID",
                    # "PICK_ID",
                    "DATE",
                    "PHYSICAL_WAREHOUSE",
                    "ORG_ID",
                    "ORG_NAME",
                    "SALESMAN_NO",
                    "SALESMAN_NAME",
                    "MANAGER_NO",
                    "MANAGER_NAME",
                    "PICKMAN_NO",
                    "PICKMAN_NAME",
                    "STAFF_NO",
                    "STAFF_NAME",
                    "CUSTOMER_NUMBER",
                    "CUSTOMER_NAME",
                    "CUSTOMER_SITE_ID",
                    "LINE_NO",
                    "TRANSPORTER_NAME",
                    "DRIVER_NAME",
                    "DRIVER_MOBILENO",
                    "VEHICLE_NO",
                    "TRUCK_DIMENSION",
                    "LOADING_CHARGES",
                    "TRANSPORT_CHARGES",
                    "MISC_CHARGES",
                    "DELIVERYADDRESS",
                    "SALESMANREMARKS",
                    "REMARKS",
                    "INVOICE_NO",
                    "CUSTOMER_TRX_ID",
                    "CUSTOMER_TRX_LINE_ID",
                    "ITEM_CODE",
                    "ITEM_DETAILS",
                    "PRODUCT_CODE",
                    "SERIAL_NO",
                    "DISREQ_QTY",
                    # "BALANCE_QTY",
                    "TRUCK_SEND_QTY",
                    # "CREATION_DATE",
                    # "CREATED_BY",
                    # "CREATED_IP",
                    # "CREATED_MAC",
                    # "LAST_UPDATE_DATE",
                    # "LAST_UPDATED_BY",
                    # "LAST_UPDATE_IP",
                    # "FLAG",
                    "DELIVERY_DATE",
                    # "UNDEL_ID"
                ]  # Add columns to exclude if any
            }
        }
       
        # Get the corresponding table name and columns to exclude
        table_info = table_mapping.get(status.lower())
 
        # If the status is invalid, return an error response
        if not table_info:
            return Response({"error": "Invalid status parameter"}, status=400)
 
        table_name = table_info["table_name"]
        exclude_columns = table_info["exclude_columns"]
 
        # Prepare the SQL query to get all columns except the excluded ones
        exclude_columns_str = ", ".join([f"'{col}'" for col in exclude_columns])
        query = f'''
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = '{table_name}'
            AND COLUMN_NAME NOT IN ({exclude_columns_str});
        '''
       
        # Execute the raw SQL query
        with connection.cursor() as cursor:
            cursor.execute(query)
            included_rows = cursor.fetchall()
 
        # Prepare the data for response
        included_columns = [row[0] for row in included_rows]
 
        # Return only the excluded columns as a list
        return Response(exclude_columns)

class GetShipmentTableHeadersView(APIView):
    """
    A View for listing specific column names of the table XXALJEBYP_INTERORG_TBL.
    """
    def get(self, request):
        # Manually define the required headers
        headers = [
        # "SHIPMENT_LINE_ID",
        "LINE_NUM",
        # "SH_CREATION_DATE",
        # "SH_CREATED_BY",
        # "FROM_ORGN_ID",
        # "FROM_ORGN_CODE",
        # "FROM_ORGN_NAME",
        # "SHIPMENT_NUM",
        # "RECEIPT_NUM",
        # "SHIPPED_DATE",
        # # "TO_ORGN_ID",
        # "TO_ORGN_CODE",
        # "TO_ORGN_NAME",
        "ITEM_CODE",
        "DESCRIPTION",
        "SYS_QUANTITY_SHIPPED",
        "SYS_QUANTITY_RECEIVED",
        # "UNIT_OF_MEASURE",
        # "ITEM_ID",

        # "FRANCHISE",
        # "FAMILY",
        # "CLASS",
        # "SUBCLASS",
        # "SYS_SHIPMENT_LINE_STATUS",
        # "CREATED_BY",
        # "CREATED_DATE",
        # "LAST_UPDATE_DATE",
        # "LAST_UPDATED_BY",
        "PHY_QUANTITY_SHIPPED",
        "BALANCE_QUANTITY_SHIPPED",
        "PHY_QUANTITY_RECEIVED",
        # "PHY_SHIPMENT_LINE_STATUS",
        # "CANCEL_LINE_FLAG"
            # "SHIPMENT_LINE_ID",
            # "LINE_NUM",
            # "CREATION_DATE",
            # "CREATED_BY",
            # "ORGANIZATION_ID",
            # "ORGANIZATION_CODE",
            # "ORGANIZATION_NAME",
            # "SHIPMENT_NUM",
            # "RECEIPT_NUM",
            # "SHIPPED_DATE",
            # "TO_ORGN_ID",
            # "TO_ORGN_CODE",
            # "TO_ORGN_NAME",
            # "QUANTITY_SHIPPED",
            # "QUANTITY_RECEIVED",
            # "UNIT_OF_MEASURE",
            # "ITEM_ID",
            # "DESCRIPTION",
            # "FRANCHISE",
            # "FAMILY",
            # "CLASS",
            # "SUBCLASS",
            # "SHIPMENT_LINE_STATUS_CODE"

        ]
        
        # Return the headers as a Response
        return Response(headers)
    
class GetUndeliveredData_columnName_valuesView(APIView):
    """
    A View for listing undelivered data with record counts for a specific column,
    filtered by SALESMAN_NO if provided.
    """

    def get(self, request, salesmanno, status, columnname):
        # Define table mapping
        table_mapping = {
            "pending": "[BUYP].[XXALJE_UNDELIVERED_DATA_BUYP1]",
            "ongoing": "WHR_CREATE_DISPATCH",
            "finished": "WHR_TRUCK_SCAN_DETAILS"
        }

        # Get the correct table name
        table_name = table_mapping.get(status.lower())
        if not table_name:
            return Response({"error": "Invalid status parameter"}, status=400)

        try:
            # Adjust query based on whether salesmanno is empty
            if salesmanno == "":
                query = f'''
                    SELECT {columnname}, COUNT(*) AS RecordCount
                    FROM {table_name}
                    GROUP BY {columnname}
                    ORDER BY {columnname};
                '''
                params = []
            else:
                query = f'''
                    SELECT {columnname}, COUNT(*) AS RecordCount
                    FROM {table_name}
                    WHERE SALESMAN_NO = %s
                    GROUP BY {columnname}
                    ORDER BY {columnname};
                '''
                params = [salesmanno]

            # Execute the SQL query
            with connection.cursor() as cursor:
                cursor.execute(query, params)
                rows = cursor.fetchall()

            # Prepare response data
            data = [row[0] for row in rows]

            return Response(data)

        except Exception as e:
            return Response({"error": str(e)}, status=500)

def get_salesrep_details(request, salesrep_id):
    with connection.cursor() as cursor:
        base_query = """
            SELECT 
                A.SALESREP_ID,  
                A.SALESMAN_NO, 
                A.SALESMAN_NAME, 
                A.ORG_ID, 
                A.ORG_NAME, 
                A.INVOICE_NUMBER,
                A.INVENTORY_ITEM_ID,
                B.WAREHOUSE_NAME,
                C.FULL_NAME AS EMPLOYEE_FULL_NAME,
                SUM(A.QUANTITY) AS TOTAL_QUANTITY,
                SUM(ISNULL(A.DISPATCH_QTY, 0)) AS TOTAL_DISPATCH_QTY,
                SUM(ISNULL(A.RETURN_QTY, 0)) AS TOTAL_RETURN_QTY,
                SUM(ISNULL(A.DISPATCH_QTY, 0) + ISNULL(A.RETURN_QTY, 0)) AS TOTAL_DISPATCH_RETURN_QTY
            FROM 
                [BUYP].[XXALJE_UNDELIVERED_DATA_BUYP1] A
            LEFT JOIN 
                [BUYP].[dbo].[ALJE_PHYSICAL_WHR] B ON A.ORG_ID = B.ORGANIZATION_ID
            LEFT JOIN 
                [BUYP].[dbo].[ALJE_EMPLOYEE_BUYP] C ON A.SALESMAN_NO = C.EMPLOYEE_NUMBER
            WHERE 
                A.SALESMAN_NO IS NOT NULL
                AND A.SALESMAN_NO != ''
            GROUP BY 
                A.SALESREP_ID,  
                A.SALESMAN_NO, 
                A.SALESMAN_NAME, 
                A.ORG_ID, 
                A.ORG_NAME, 
                A.INVOICE_NUMBER,
                A.INVENTORY_ITEM_ID,
                B.WAREHOUSE_NAME,
                C.FULL_NAME
            HAVING 
                SUM(A.QUANTITY) > SUM(ISNULL(A.DISPATCH_QTY, 0) + ISNULL(A.RETURN_QTY, 0))
        """

        if str(salesrep_id) == '-3':
            base_query += " AND A.SALESREP_ID = '-3'"
        elif str(salesrep_id) == '3':
            base_query += " AND A.SALESREP_ID != '-3'"

        cursor.execute(base_query)
        columns = [col[0] for col in cursor.description]
        all_results = [dict(zip(columns, row)) for row in cursor.fetchall()]

    # Post-processing to extract unique SALESMAN_NO entries only
    unique_salesmen = {}
    for row in all_results:
        salesman_no = row['SALESMAN_NO']
        if salesman_no not in unique_salesmen:
            unique_salesmen[salesman_no] = {
                'SALESREP_ID': row['SALESREP_ID'],
                'SALESMAN_NO': row['SALESMAN_NO'],
                'SALESMAN_NAME': row['SALESMAN_NAME'],
                'ORG_ID': row['ORG_ID'],
                'WAREHOUSE_NAME': row['WAREHOUSE_NAME'],
                'EMPLOYEE_FULL_NAME': row['EMPLOYEE_FULL_NAME'],  # Add full name here
            }

    # Convert to list for JSON response
    unique_salesmen_list = list(unique_salesmen.values())

    return JsonResponse(unique_salesmen_list, safe=False)


from django.db import connections

class GetSalesmanByCustomer(View):
    def get(self, request):
        try:
            # ----------------------------
            # 1. Capture query parameters
            # ----------------------------
            customer_no = request.GET.get("customer_no")
            customer_site_id = request.GET.get("customer_site_id")

            if not customer_no or not customer_site_id:
                return JsonResponse({"status": "error", "message": "Missing parameters"}, status=400)

            # ----------------------------
            # 2. Query MSSQL Database
            # ----------------------------
            query = """
                WITH SalesmanData AS (
                    SELECT 
                        SALESREP_ID,
                        SALESMAN_NO,
                        SALESMAN_NAME,
                        ORG_ID,
                        WAREHOUSE_NAME,
                        ROW_NUMBER() OVER (
                            PARTITION BY SALESMAN_NO 
                            ORDER BY SALESREP_ID
                        ) AS rn
                    FROM [BUYP].[BUYP].[XXALJE_UNDELIVERED_DATA_BUYP1]
                    WHERE CUSTOMER_NUMBER = %s
                    AND CUSTOMER_SITE_ID = %s
                )
                SELECT 
                    SALESREP_ID,
                    SALESMAN_NO,
                    SALESMAN_NAME,
                    ORG_ID,
                    WAREHOUSE_NAME
                FROM SalesmanData
                WHERE rn = 1;

            """

            with connections['default'].cursor() as cursor:
                cursor.execute(query, [customer_no, customer_site_id])
                rows = cursor.fetchall()

            # ----------------------------
            # 3. Format Results as JSON
            # ----------------------------
            result = []
            for row in rows:
                result.append({
                    "SALESREP_ID": row[0],
                    "SALESMAN_NO": row[1],
                    "SALESMAN_NAME": row[2],
                    "ORG_ID": row[3],
                    "WAREHOUSE_NAME": row[4],
                    "EMPLOYEE_FULL_NAME": row[2].upper() if row[2] else None  # uppercase version
                })

            return JsonResponse(result, safe=False, json_dumps_params={'ensure_ascii': False})

        except Exception as e:
            return JsonResponse({"status": "error", "message": str(e)}, status=500)

def Get_sales_Supervisor_access(request, salesrep_id, supervisorno):
    with connection.cursor() as cursor:
        # Build conditionally dynamic SQL
        salesrep_condition = ""
        if str(salesrep_id) == '-3':
            salesrep_condition = " AND A.SALESREP_ID = '-3'"
        elif str(salesrep_id) == '3':
            salesrep_condition = " AND A.SALESREP_ID != '-3'"

        base_query = f"""
            SELECT 
                S.SUPERVISOR_NO,
                A.SALESREP_ID,  
                A.SALESMAN_NO, 
                A.SALESMAN_NAME, 
                A.ORG_ID, 
                A.ORG_NAME, 
                A.INVOICE_NUMBER,
                A.INVENTORY_ITEM_ID,
                B.WAREHOUSE_NAME,
                C.FULL_NAME AS EMPLOYEE_FULL_NAME,
                SUM(A.QUANTITY) AS TOTAL_QUANTITY,
                SUM(ISNULL(A.DISPATCH_QTY, 0)) AS TOTAL_DISPATCH_QTY,
                SUM(ISNULL(A.RETURN_QTY, 0)) AS TOTAL_RETURN_QTY,
                SUM(ISNULL(A.DISPATCH_QTY, 0) + ISNULL(A.RETURN_QTY, 0)) AS TOTAL_DISPATCH_RETURN_QTY
            FROM 
                [BUYP].[XXALJE_UNDELIVERED_DATA_BUYP1] A
            LEFT JOIN 
                [BUYP].[dbo].[ALJE_PHYSICAL_WHR] B ON A.ORG_ID = B.ORGANIZATION_ID
            LEFT JOIN 
                [BUYP].[dbo].[ALJE_EMPLOYEE_BUYP] C ON A.SALESMAN_NO = C.EMPLOYEE_NUMBER
            INNER JOIN 
                [BUYP].[dbo].[SUPERVISOR_ACCESS_TBL] S ON A.SALESMAN_NO = S.SALESMAN_NO
            WHERE 
                A.SALESMAN_NO IS NOT NULL
                AND A.SALESMAN_NO != ''
                AND S.SUPERVISOR_NO = '{supervisorno}'
                {salesrep_condition}
            GROUP BY 
                S.SUPERVISOR_NO,
                A.SALESREP_ID,  
                A.SALESMAN_NO, 
                A.SALESMAN_NAME, 
                A.ORG_ID, 
                A.ORG_NAME, 
                A.INVOICE_NUMBER,
                A.INVENTORY_ITEM_ID,
                B.WAREHOUSE_NAME,
                C.FULL_NAME
            HAVING 
                SUM(A.QUANTITY) > SUM(ISNULL(A.DISPATCH_QTY, 0) + ISNULL(A.RETURN_QTY, 0))
        """

        cursor.execute(base_query)
        columns = [col[0] for col in cursor.description]
        all_results = [dict(zip(columns, row)) for row in cursor.fetchall()]

    # Post-processing to extract unique SALESMAN_NO entries only
    unique_salesmen = {}
    for row in all_results:
        salesman_no = row['SALESMAN_NO']
        if salesman_no not in unique_salesmen:
            unique_salesmen[salesman_no] = {
                'SALESREP_ID': row['SALESREP_ID'],
                'SALESMAN_NO': row['SALESMAN_NO'],
                'SALESMAN_NAME': row['SALESMAN_NAME'],
                'ORG_ID': row['ORG_ID'],
                'WAREHOUSE_NAME': row['WAREHOUSE_NAME'],
                'EMPLOYEE_FULL_NAME': row['EMPLOYEE_FULL_NAME'],
            }

    # Convert to list for JSON response
    unique_salesmen_list = list(unique_salesmen.values())

    return JsonResponse(unique_salesmen_list, safe=False)

class DepartmentDashboardView(viewsets.ModelViewSet):
    queryset = DepartmentModel.objects.all()
    serializer_class = DepartmentSerializer
    pagination_class = StandardResultsSetPagination

class DepartmentView(viewsets.ModelViewSet):
    queryset = DepartmentModel.objects.all()
    serializer_class = DepartmentSerializer

class DepRolesView(viewsets.ModelViewSet):
    queryset = DepRolesModel.objects.all()
    serializer_class = DepRolesSerializer

class DepRoleFormsView(viewsets.ModelViewSet):
    queryset = DepRoleFormsModel.objects.all()
    serializer_class = DepRoleFormsSerializer

def view_pdf_content(request):
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT * FROM [BUYP].[dbo].[pdfsave]")
            rows = cursor.fetchall()

        result = []
        for row in rows:
            result.append({
                'id': row[0],
                'savepdf': row[1]  # If it's HTML or encoded PDF data
            })

        return JsonResponse({'status': 'success', 'data': result}, safe=False)

    except Exception as e:
        return JsonResponse({'status': 'error', 'message': str(e)}, status=500)
    
class DepUserManagementView(viewsets.ModelViewSet):
    queryset = DepUserManagementModel.objects.all()
    serializer_class = DepUserManagementSerializer

    def create(self, request, *args, **kwargs):
        # print("Incoming Data:", request.data)  # Debugging

        # If request.data is a list, allow bulk insert
        if isinstance(request.data, list):
            serializer = self.get_serializer(data=request.data, many=True)  # <-- Allow list
        else:
            serializer = self.get_serializer(data=request.data)

        if serializer.is_valid():
            serializer.save()
            return Response(serializer.data, status=status.HTTP_201_CREATED)
        else:
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

def get_department_details(request, empno):
    query = """
        SELECT DISTINCT 
            d.DEP_ID, 
            d.DEP_ROLE_ID, 
            o.DEP_NAME,
            r.DEP_ROLE_NAME
        FROM DEPARTMENT_USERMANAGEMENT d
        INNER JOIN ORGANIZED_DEPARTMENT o ON d.DEP_ID = o.DEP_ID
        INNER JOIN DEPARTMENT_ROLE r ON d.DEP_ROLE_ID = r.DEP_ROLE_ID
        WHERE d.EMP_ID = %s and d.STATUS !='0'
    """

    with connection.cursor() as cursor:
        cursor.execute(query, [empno])
        results = cursor.fetchall()

    # Convert to JSON format
    departments = [
        {"DEP_ID": row[0], "DEP_ROLE_ID": row[1], "DEP_NAME": row[2], "DEP_ROLE_NAME": row[3]} 
        for row in results
    ]

    return JsonResponse({"departments": departments}, safe=False)

def get_submenu_list(request, dep_role_id, empno):
    query = """
        SELECT DISTINCT SUBMENU
        FROM DEPARTMENT_USERMANAGEMENT
        WHERE DEP_ROLE_ID = %s AND EMP_ID = %s  AND STATUS ='1'
    """

    with connection.cursor() as cursor:
        cursor.execute(query, [int(dep_role_id), int(empno)])
        results = cursor.fetchall()

    # Convert to a list of submenu names
    submenu_list = [row[0] for row in results]

    return JsonResponse({"submenu": submenu_list}, safe=False) 

def get_submenu_depid_list(request, dep_role_id, empno): 
    # First, get the DEP_ID associated with the given DEP_ROLE_ID
    dep_id_query = """
        SELECT DISTINCT DEP_ID 
        FROM DEPARTMENT_USERMANAGEMENT 
        WHERE DEP_ROLE_ID = %s AND STATUS ='1'
    """

    with connection.cursor() as cursor:
        cursor.execute(dep_id_query, [dep_role_id])
        dep_id_result = cursor.fetchone()  # Fetch a single result

    # If no DEP_ID is found, return an empty submenu list
    if not dep_id_result:
        return JsonResponse({"submenu": []}, safe=False)

    dep_id = dep_id_result[0]  # Extract DEP_ID from the result tuple

    # Now, use the retrieved DEP_ID to get the submenu list
    submenu_query = """
        SELECT DISTINCT SUBMENU
        FROM DEPARTMENT_USERMANAGEMENT
        WHERE DEP_ID = %s AND EMP_ID = %s AND STATUS ='1'
    """

    with connection.cursor() as cursor:
        cursor.execute(submenu_query, [dep_id, empno])
        results = cursor.fetchall()

    # Convert to a list of submenu names
    submenu_list = [row[0] for row in results]

    return JsonResponse({"submenu": submenu_list}, safe=False)

def get_SearchEmployee_data(request, empid):
    try:
        with connection.cursor() as cursor:
            
            # Query to fetch employee data with department, role, and submenu details ordered by EMP_ID ASC
            query = """
                SELECT UM.EMP_ID, 
                       OD.DEP_NAME, 
                       DR.DEP_ROLE_ID, 
                       DR.DEP_ROLE_NAME,
                       UM.SUBMENU
                FROM DEPARTMENT_USERMANAGEMENT UM
                LEFT JOIN ORGANIZED_DEPARTMENT OD ON UM.DEP_ID = OD.DEP_ID
                LEFT JOIN DEPARTMENT_ROLE DR ON UM.DEP_ROLE_ID = DR.DEP_ROLE_ID
                WHERE UM.EMP_ID = %s  AND UM.STATUS = 1
                ORDER BY UM.EMP_ID ASC
            """
            
            cursor.execute(query, [empid])
            rows = cursor.fetchall()

            # Get column names from the cursor description
            columns = [col[0] for col in cursor.description]

            # Convert the result into a list of dictionaries
            data = [dict(zip(columns, row)) for row in rows]

            # Initialize data structures
            selected_departments = set()
            department_roles = defaultdict(list)
            role_submenus = defaultdict(set)

            # Loop through the results to build the lists
            for row in data:
                department_name = row['DEP_NAME']
                role_id = row['DEP_ROLE_ID']
                role_name = row['DEP_ROLE_NAME']
                submenu_name = row.get('SUBMENU')

                # Collect unique departments
                if department_name:
                    selected_departments.add(department_name)

                # Map roles to departments
                if department_name and role_name:
                    department_roles[department_name].append(role_name)

                # Map submenus to role IDs
                if role_id and submenu_name:
                    role_submenus[role_id].add(submenu_name)

            # Convert sets to lists
            selected_departments_list = list(selected_departments)
            department_roles = {dept: list(set(roles)) for dept, roles in department_roles.items()}
            role_submenus = {role_id: list(submenus) for role_id, submenus in role_submenus.items()}

        # Return the result as JSON
        return JsonResponse({
            'selected_departments': selected_departments_list,
            'selected_departments_wise_roles': department_roles,
            'selected_role_id_wise_submenus': role_submenus
        }, safe=False)

    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)

@csrf_exempt  # Bypass CSRF validation
def update_employee(request, emp_id):
    if request.method != "POST":
        return JsonResponse({"error": "Invalid request method. Use POST."}, status=400)

    try:
        data = json.loads(request.body)
        # print("Received JSON Data:", data)

        with transaction.atomic():
            with connection.cursor() as cursor:
                # Fetch all existing submenus for the given EMP_ID
                cursor.execute("""
                    SELECT DEP_ROLE_ID, SUBMENU, STATUS
                    FROM DEPARTMENT_USERMANAGEMENT
                    WHERE EMP_ID = %s
                """, [emp_id])

                existing_records = {f"{row[0]}_{row[1]}": row[2] for row in cursor.fetchall()}
                # print("Existing Records:", existing_records)

                new_records = []
                to_update = set()

                # Process the JSON data
                for dep_role_id, submenus in data.items():
                    for submenu in submenus:
                        record_key = f"{dep_role_id}_{submenu}"

                        if record_key in existing_records:
                            to_update.add(record_key)
                        else:
                            cursor.execute("""
                                SELECT DEP_ID 
                                FROM DEPARTMENT_ROLEWISE_SUBMENU
                                WHERE DEP_ROLE_ID = %s AND SUBMENU = %s
                            """, [dep_role_id, submenu])

                            dep_id_row = cursor.fetchone()
                            if dep_id_row:
                                dep_id = dep_id_row[0]
                                new_records.append((emp_id, dep_id, dep_role_id, submenu, True))
                                # print(f"New Record Added: {emp_id}, {dep_id}, {dep_role_id}, {submenu}")

                # Insert new records
                if new_records:
                    cursor.executemany("""
                        INSERT INTO DEPARTMENT_USERMANAGEMENT (EMP_ID, DEP_ID, DEP_ROLE_ID, SUBMENU, STATUS)
                        VALUES (%s, %s, %s, %s, %s)
                    """, new_records)
                    # print(f"Inserted {len(new_records)} new records")

                # Update existing records to true
                for record_key in to_update:
                    dep_role_id, submenu = record_key.split('_')
                    cursor.execute("""
                        UPDATE DEPARTMENT_USERMANAGEMENT
                        SET STATUS = 1
                        WHERE EMP_ID = %s AND DEP_ROLE_ID = %s AND SUBMENU = %s
                    """, [emp_id, dep_role_id, submenu])
                    # print(f"Updated record: {record_key}")

                # Set missing submenus to false
                for record_key, status in existing_records.items():
                    if record_key not in to_update:
                        dep_role_id, submenu = record_key.split('_')
                        cursor.execute("""
                            UPDATE DEPARTMENT_USERMANAGEMENT
                            SET STATUS = 0
                            WHERE EMP_ID = %s AND DEP_ROLE_ID = %s AND SUBMENU = %s
                        """, [emp_id, dep_role_id, submenu])
                        # print(f"Set status false for missing record: {record_key}")

        return JsonResponse({
            "message": "Employee data updated successfully",
            "new_records": len(new_records),
            "updated_records": len(to_update)
        })

    except Exception as e:
        # print("Error Traceback:", traceback.format_exc())
        return JsonResponse({'error': str(e)}, status=500)
  
@csrf_exempt
def insert_dispatch_data(request, dispatch_id, undel_id, qty):
    try:
        with connection.cursor() as cursor:
            query = '''
                INSERT INTO [BUYP].[dbo].[Oracle_Update_Tem_tbl] 
                ([dispatch_id], [undel_id], [dispatch_qty]) 
                VALUES (%s, %s, %s)
            '''
            cursor.execute(query, [dispatch_id, undel_id, qty])
        return JsonResponse({'status': 'success', 'message': 'Data inserted successfully'})
    except Exception as e:
        return JsonResponse({'status': 'error', 'message': str(e)})
    
@csrf_exempt
def insert_Inter_ORG_PHY_Shipped_data(request, shipment_id=None, shipment_line_id=None, qty=None):
    """
    Inserts a record into Inter_ORG_Oracle_Update_tbl.
    Handles empty parameters by inserting NULL into the database.
    """

    try:
        # Convert blank/None to None so DB stores NULL
        if not shipment_id or shipment_id.strip() == "":
            shipment_id = None
        if not shipment_line_id or shipment_line_id.strip() == "":
            shipment_line_id = None
        if not qty or str(qty).strip() == "":
            qty = None

        with connection.cursor() as cursor:
            query = '''
                INSERT INTO [BUYP].[dbo].[Inter_ORG_Oracle_Update_tbl]
                ([Shipment_id], [Shipment_Line_Id], [Phy_shipped_Qty])
                VALUES (%s, %s, %s)
            '''
            cursor.execute(query, [shipment_id, shipment_line_id, qty])

        return JsonResponse({
            'status': 'success',
            'message': 'Data inserted successfully',
            'data': {
                'Shipment_id': shipment_id,
                'Shipment_Line_Id': shipment_line_id,
                'Phy_shipped_Qty': qty
            }
        })
    except Exception as e:
        return JsonResponse({
            'status': 'error',
            'message': str(e)
        })

@csrf_exempt
def insert_Inter_ORG_PHY_Recevied_data(request, shipment_id, shipment_line_id, qty):
    try:
        with connection.cursor() as cursor:
            query = '''
                INSERT INTO [BUYP].[dbo].[Inter_ORG_Oracle_Update_tbl] 
                ([Shipment_id], [Shipment_Line_Id], [Phy_recevied_Qty]) 
                VALUES (%s, %s, %s)
            '''
            cursor.execute(query, [shipment_id, shipment_line_id, qty])
        return JsonResponse({'status': 'success', 'message': 'Data inserted successfully'})
    except Exception as e:
        return JsonResponse({'status': 'error', 'message': str(e)})

@csrf_exempt
def update_Phy_quantity_Recevied_interOrg(request):
    if request.method == "POST":
        try:
            # Parse incoming JSON data
            data = json.loads(request.body)

            shipment_line_id = data.get("SHIPMENT_LINE_ID")
            qty_recent = data.get("qty_recent")

            if not all([shipment_line_id, qty_recent]):
                return JsonResponse({"status": "error", "message": "Missing parameters"}, status=400)

            # Determine column based on transfer type
            with connection.cursor() as cursor:
                # Fetch existing quantity
                cursor.execute(f"""
                    SELECT PHY_QUANTITY_RECEIVED 
                    FROM BUYP.dbo.XXALJEBYP_INTERORG_TBL 
                    WHERE SHIPMENT_LINE_ID = %s
                """, [ shipment_line_id])

                row = cursor.fetchone()

                if not row:
                    return JsonResponse({"status": "error", "message": "Record not found"}, status=404)

                existing_qty = row[0] or 0
                new_qty = existing_qty + float(qty_recent)

                # Update quantity
                cursor.execute(f"""
                    UPDATE BUYP.dbo.XXALJEBYP_INTERORG_TBL 
                    SET PHY_QUANTITY_RECEIVED = %s 
                    WHERE  SHIPMENT_LINE_ID = %s
                """, [new_qty, shipment_line_id])

            return JsonResponse({
                "status": "success",
                "message": "PHY_QUANTITY_RECEIVED updated successfully",
                "old_qty": existing_qty,
                "added_qty": float(qty_recent),
                "new_qty": new_qty
            })

        except Exception as e:
            return JsonResponse({"status": "error", "message": str(e)}, status=500)

    return JsonResponse({"status": "error", "message": "Only POST method allowed"}, status=405)


@method_decorator(csrf_exempt, name='dispatch')
class UpdateView(View):
    def put(self, request, udel_id, qty):  # Ensure URL passes udel_id and qty
        try:
            udel_id = str(udel_id).strip()
            qty = float(qty)

            with connection.cursor() as cursor:
                # Fetch current quantity and dispatch quantity
                cursor.execute("""
                    SELECT QUANTITY, DISPATCH_QTY 
                    FROM [BUYP].[XXALJE_UNDELIVERED_DATA_BUYP1]
                    WHERE UNDEL_ID = %s
                """, [udel_id])
                row = cursor.fetchone()

                if not row:
                    return JsonResponse({"error": f"UNDEL_ID {udel_id} not found"}, status=404)

                quantity, dispatch_qty = row
                if quantity is None or dispatch_qty is None:
                    return JsonResponse({"error": "Invalid data in database (null values)."}, status=400)

                new_dispatch_qty = dispatch_qty + qty

                # Cap dispatch quantity to not exceed total quantity
                if new_dispatch_qty > quantity:
                    new_dispatch_qty = quantity
                    message = "Dispatch QTY capped at total QUANTITY."
                else:
                    message = "Dispatch QTY updated successfully."

                # Update the record
                cursor.execute("""
                    UPDATE [BUYP].[XXALJE_UNDELIVERED_DATA_BUYP1]
                    SET DISPATCH_QTY = %s, ATTRIBUTE1 = 'BUYP UPDATE'
                    WHERE UNDEL_ID = %s
                """, [new_dispatch_qty, udel_id])
                connection.commit()

            return JsonResponse({
                "message": message,
                "UNDEL_ID": udel_id,
                "QUANTITY": quantity,
                "OLD_DISPATCH_QTY": dispatch_qty,
                "NEW_DISPATCH_QTY": new_dispatch_qty
            }, status=200)

        except DatabaseError as e:
            return JsonResponse({"error": f"Database error: {str(e)}"}, status=500)
        except Exception as e:
            return JsonResponse({"error": f"Unexpected error: {str(e)}"}, status=500)
        
@method_decorator(csrf_exempt, name='dispatch')
class subractUpdateView(View):
    def put(self, request, udel_id, qty):  # Ensure function parameters match URL pattern
        try:
            # Update quantity in the database
            with connection.cursor() as cursor:
                cursor.execute(
                    "UPDATE [BUYP].[XXALJE_UNDELIVERED_DATA_BUYP1]  SET DISPATCH_QTY = DISPATCH_QTY - %s WHERE UNDEL_ID = %s",
                    [qty, udel_id]
                )
                connection.commit()

            return JsonResponse({"message": f"QTY updated successfully for UNDEL_ID {udel_id}"}, status=200)

        except DatabaseError as e:
            return JsonResponse({"error": str(e)}, status=500)

def Get_interorg_data_Shipment(request, transfer_type, shipment_header_id):
    try:
        if transfer_type.lower() == "shipment number":
            query = """
               SELECT * 
                    FROM [BUYP].[dbo].[XXALJEBYP_INTERORG_TBL]
                    WHERE [SHIPMENT_NUM] = %s
                    AND [SYS_QUANTITY_SHIPPED] <> ISNULL([PHY_QUANTITY_SHIPPED], 0)
                    ORDER BY LINE_NUM ASC
            """
        elif transfer_type.lower() == "receipt number":
            query = """
                SELECT * FROM [BUYP].[dbo].[XXALJEBYP_INTERORG_TBL]
                WHERE [RECEIPT_NUM] = %s
                AND [SYS_QUANTITY_SHIPPED] <> [PHY_QUANTITY_SHIPPED]
                ORDER BY LINE_NUM ASC
            """
        else:
            return JsonResponse({"error": "Invalid transfer type. Use 'Shipment Number' or 'Receipt Number'."}, status=400)

        with connection.cursor() as cursor:
            cursor.execute(query, [shipment_header_id])
            columns = [col[0] for col in cursor.description]
            results = [dict(zip(columns, row)) for row in cursor.fetchall()]

        return JsonResponse(results, safe=False)
    
    except Exception as e:
        return JsonResponse({"error": f"Database error: {str(e)}"}, status=500)

class Shiment_DispatchView(viewsets.ModelViewSet):
    queryset = ShimentDispatchModels.objects.all()
    serializer_class = ShipmentDispatchSerializer
    pagination_class = StandardResultsSetPagination

@csrf_exempt
def update_Phy_quantity_Shipped_interOrg(request):
    if request.method == "POST":
        try:
            # Parse incoming JSON data
            data = json.loads(request.body)

            transfer_type = data.get("TRANSFER_TYPE")
            shipment_num = data.get("SHIPMENT_NUM")
            shipment_line_id = data.get("SHIPMENT_LINE_ID")
            qty_recent = data.get("qty_recent")

            if not all([transfer_type, shipment_line_id, qty_recent]):
                return JsonResponse({"status": "error", "message": "Missing parameters"}, status=400)

            if transfer_type == "Shipment Number" and not shipment_num:
                return JsonResponse({"status": "error", "message": "Missing SHIPMENT_NUM"}, status=400)
            if transfer_type == "Receipt Number" and not shipment_num:
                return JsonResponse({"status": "error", "message": "Missing RECEIPT_NUM"}, status=400)

            # Determine column based on transfer type
            column_name = "SHIPMENT_NUM" if transfer_type == "Shipment Number" else "RECEIPT_NUM"

            with connection.cursor() as cursor:
                # Fetch existing quantity
                cursor.execute(f"""
                    SELECT PHY_QUANTITY_SHIPPED 
                    FROM BUYP.dbo.XXALJEBYP_INTERORG_TBL 
                    WHERE {column_name} = %s AND SHIPMENT_LINE_ID = %s
                """, [shipment_num, shipment_line_id])

                row = cursor.fetchone()

                if not row:
                    return JsonResponse({"status": "error", "message": "Record not found"}, status=404)

                existing_qty = row[0] or 0
                new_qty = existing_qty + float(qty_recent)

                # Update quantity
                cursor.execute(f"""
                    UPDATE BUYP.dbo.XXALJEBYP_INTERORG_TBL 
                    SET PHY_QUANTITY_SHIPPED = %s , ATTRIBUTE1 = 'BUYP UPDATE'
                    WHERE {column_name} = %s AND SHIPMENT_LINE_ID = %s
                """, [new_qty, shipment_num, shipment_line_id])

            return JsonResponse({
                "status": "success",
                "message": "PHY_QUANTITY_SHIPPED updated successfully",
                "old_qty": existing_qty,
                "added_qty": float(qty_recent),
                "new_qty": new_qty
            })

        except Exception as e:
            return JsonResponse({"status": "error", "message": str(e)}, status=500)

    return JsonResponse({"status": "error", "message": "Only POST method allowed"}, status=405)

def Generate_Shipment_dispatch_print(request, shipmentnum,receiptnum, shipmentid, transportor_Name, vehicleNo, driverName, driverMobileNo, date, remarks, itemtotalqty, products_param):
    # Decode the product parameters
    decoded_products = urllib.parse.unquote(products_param)
 
    # Extract product sets enclosed in curly braces
    product_sets = re.findall(r'\{(.*?)\}', decoded_products)
 
 
    items = []
    fromorgcode = ''  # Initialize fromorgcode
    fromorgname = ''
    toorgcode = ''
    toorgname = ''
    for i, product_set in enumerate(product_sets):
        parts = product_set.split('|')
        if len(parts) == 8:
            s_no = parts[0].strip()
            from_org_code = parts[1].strip()
            from_org_name = parts[2].strip()
            to_org_code = parts[3].strip()
            to_org_name = parts[4].strip()
            item_code = parts[5].strip()
            item_details = parts[6].strip()
            qty = parts[7].strip()
 
            # Get fromorgcode from the first valid product only
            if i == 0:
                fromorgcode = from_org_code
                fromorgname = from_org_name
                toorgcode = to_org_code
                toorgname = to_org_name
       
 
            items.append({
                's_no': s_no,
                'from_org_code': from_org_code,
                'to_org_code': to_org_code,
                'item_code': item_code,
                'item_details': item_details,
                'qty': qty,
            })
    remarks = '' if remarks in ['null', 'None', None, ''] else remarks
          
    context = {
        'shipmentnum': shipmentnum,
        'receiptnum': receiptnum,
        'shipmentid': shipmentid,
        'date': date,
        'transportor_Name': transportor_Name,
        'vehicleNo': vehicleNo,
        'driverName': driverName,
        'driverMobileNo': driverMobileNo,
        'remarks': remarks,
        'itemtotalqty': itemtotalqty,
        'items': items,
        'fromorgcode': fromorgcode,
        'fromorgname': fromorgname,
        'toorgcode': toorgcode,
        'toorgname': toorgname,
    }
 
    return render(request, 'Generate_shipmentdispatch_print.html', context)

@csrf_exempt
def Check_InvoiceStatus_CancelInvoice(request, customerno, customersiteid, invoiceno):
    customer_number = customerno
    customer_site_id = customersiteid
    invoice_number = invoiceno

    if not all([customer_number, customer_site_id, invoice_number]):
        return JsonResponse({'status': 'error', 'message': 'Missing parameters'}, status=400)

    # Fetch from WHR_CREATE_DISPATCH (Salesman Request)
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT * FROM BUYP.dbo.WHR_CREATE_DISPATCH
            WHERE CUSTOMER_NUMBER = %s AND CUSTOMER_SITE_ID = %s AND INVOICE_NUMBER = %s
        """, [customer_number, customer_site_id, invoice_number])
        dispatch_result = [dict(zip([col[0] for col in cursor.description], row)) for row in cursor.fetchall()]

    # Fetch from WHR_DISPATCH_REQUEST (Manager Assigns Pickman)
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT * FROM BUYP.dbo.WHR_DISPATCH_REQUEST
            WHERE CUSTOMER_NUMBER = %s AND CUSTOMER_SITE_ID = %s AND INVOICE_NUMBER = %s
        """, [customer_number, customer_site_id, invoice_number])
        dispatch_request_result = [dict(zip([col[0] for col in cursor.description], row)) for row in cursor.fetchall()]

    # Fetch from WHR_PICKED_MAN (Pickman Scans Items)
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT * FROM BUYP.dbo.WHR_PICKED_MAN
            WHERE CUSTOMER_NUMBER = %s AND CUSTOMER_SITE_ID = %s AND INVOICE_NUMBER = %s
        """, [customer_number, customer_site_id, invoice_number])
        picked_man_result = [dict(zip([col[0] for col in cursor.description], row)) for row in cursor.fetchall()]

    # Determine status and message
    if dispatch_result and not dispatch_request_result and not picked_man_result:
        message = "This invoice has already been requested by the salesman."
        details = dispatch_result[0]
    elif dispatch_result and dispatch_request_result and not picked_man_result:
        message = "This invoice has already been requested by the salesman. The manager has assigned a pickman."
        details = {**dispatch_result[0], **dispatch_request_result[0]}
    elif dispatch_result and dispatch_request_result and picked_man_result:
        message = "This invoice has already been requested by the salesman. The manager has assigned a pickman, and the pickman has scanned the invoice items (currently staged)."
        details = {**dispatch_result[0], **dispatch_request_result[0], **picked_man_result[0]}
    else:
        return JsonResponse({'status': 'not_found', 'message': 'Invoice not found in any records.'})

    response_data = {
        'status': 'success',
        'message': message,
        'invoice_number': invoice_number,
        'customer_number': customer_number,
        'customer_site_id': customer_site_id,

    }

    return JsonResponse(response_data, safe=False)

@csrf_exempt
def Update_flag_status_Underlivered(request):
    if request.method == "POST":
        try:
            data = json.loads(request.body)
            customer_number = data.get("CUSTOMER_NUMBER")
            customer_site_id = data.get("CUSTOMER_SITE_ID")
            invoice_number = data.get("INVOICE_NUMBER")

            if not all([customer_number, customer_site_id, invoice_number]):
                return JsonResponse({"error": "Missing required fields"}, status=400)

            with connection.cursor() as cursor:
                cursor.execute("""
                    UPDATE [BUYP].[XXALJE_UNDELIVERED_DATA_BUYP1]
                    SET FLAG_STATUS = 'IC'
                    WHERE CUSTOMER_NUMBER = %s
                      AND CUSTOMER_SITE_ID = %s
                      AND INVOICE_NUMBER = %s
                """, [customer_number, customer_site_id, invoice_number])
                updated_rows = cursor.rowcount

            if updated_rows > 0:
                return JsonResponse({"message": "FLAG_STATUS updated successfully", "updated_rows": updated_rows})
            else:
                return JsonResponse({"message": "No matching record found"}, status=404)

        except json.JSONDecodeError:
            return JsonResponse({"error": "Invalid JSON"}, status=400)
        except Exception as e:
            return JsonResponse({"error": str(e)}, status=500)

    return JsonResponse({"error": "Only POST method is allowed"}, status=405)

@csrf_exempt
def Update_Return_Invoice_Undelivered(request):
    if request.method == "POST":
        try:
            data = json.loads(request.body)

            customer_no = str(data.get("customerno")).strip()
            customer_site = str(data.get("customersite")).strip()
            invoice_no = str(data.get("invoiceno")).strip()
            undel_id = int(data.get("undel_id"))
            return_qty = data.get("return_qty")
            flag_status = data.get("flagstatus")

            logger.info(f"Received: {customer_no=}, {customer_site=}, {invoice_no=}, {undel_id=}, {flag_status=}")

            with connection.cursor() as cursor:
                if flag_status == "deleted":
                    cursor.execute("""
                        UPDATE [BUYP].[BUYP].[XXALJE_UNDELIVERED_DATA_BUYP1]
                        SET [RETURN_QTY] = ISNULL([RETURN_QTY], 0) + %s,
                            [FLAG_STATUS] = 'IC', [RETURN_FLAG] = 'R'
                        WHERE [CUSTOMER_NUMBER] = %s
                          AND [CUSTOMER_SITE_ID] = %s
                          AND [INVOICE_NUMBER] = %s
                          AND [UNDEL_ID] = %s
                    """, [return_qty ,customer_no, customer_site, invoice_no, undel_id])

                elif flag_status == "edited":
                    cursor.execute("""
                        UPDATE [BUYP].[BUYP].[XXALJE_UNDELIVERED_DATA_BUYP1]
                        SET [RETURN_QTY] = ISNULL([RETURN_QTY], 0) + %s,
                            [FLAG_STATUS] = '' , [RETURN_FLAG] = 'R'
                        WHERE [CUSTOMER_NUMBER] = %s
                          AND [CUSTOMER_SITE_ID] = %s
                          AND [INVOICE_NUMBER] = %s
                          AND [UNDEL_ID] = %s
                    """, [return_qty, customer_no, customer_site, invoice_no, undel_id])

                else:
                    return JsonResponse({"status": "error", "message": "Invalid flagstatus"}, status=400)

                if cursor.rowcount == 0:
                    return JsonResponse({
                        "status": "error",
                        "message": f"No matching row found for customer_no={customer_no}, customer_site={customer_site}, invoice_no={invoice_no}, undel_id={undel_id}"
                    }, status=404)

            return JsonResponse({"status": "success", "message": "Record updated successfully"})

        except Exception as e:
            return JsonResponse({"status": "error", "message": str(e)}, status=500)

    return JsonResponse({"status": "error", "message": "Invalid request method"}, status=405)

def login_connect_table_view(request):
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT *
            FROM [BUYP].[dbo].[LOGIN_CONNECT_TBL]
        """)
        rows = cursor.fetchall()
        columns = [col[0] for col in cursor.description]

    data = [dict(zip(columns, row)) for row in rows]

    return JsonResponse(data, safe=False)

# Admin sales supervisor code 

def get_salesmen_by_supervisor(request, supervisor_no):
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT SALESMAN_NO, SALESMAN_NAME
                FROM BUYP.dbo.SUPERVISOR_ACCESS_TBL
                WHERE SUPERVISOR_NO = %s
            """, [supervisor_no])
            rows = cursor.fetchall()

        # Format results
        salesmen = [{'SALESMAN_NO': row[0], 'SALESMAN_NAME': row[1]} for row in rows]

        return JsonResponse({'status': 'success', 'salesmen': salesmen}, status=200)
    
    except Exception as e:
        return JsonResponse({'status': 'error', 'message': str(e)}, status=500)

@csrf_exempt
def get_unassigned_supervisors(request):
    try:
        with connection.cursor() as cursor:
            query = f"""
                SELECT
                    SR.ORG_ID,
                    SR.SALESREP_ID,
                    SR.SALESREP_NUMBER,
                    SR.NAME,
                    WH.WAREHOUSE_NAME AS ORG_NAME,
                    WH.REGION_NAME
                FROM BUYP.BUYP.ALJE_SALESREP SR
                LEFT JOIN BUYP.dbo.ALJE_PHYSICAL_WHR WH
                    ON SR.ORG_ID = WH.ORGANIZATION_ID
                WHERE SR.SALESREP_NUMBER IN (
                    SELECT CAST(EMPLOYEE_ID AS NVARCHAR)
                    FROM BUYP.dbo.WHR_USER_MANAGEMENT
                    WHERE EMP_ROLE = 'Sales Supervisor'
                )
                AND SR.SALESREP_NUMBER NOT IN (
                    SELECT CAST(SUPERVISOR_NO AS NVARCHAR)
                    FROM BUYP.dbo.SUPERVISOR_ACCESS_TBL
                )
            """

            cursor.execute(query)
            rows = cursor.fetchall()

            columns = [col[0] for col in cursor.description]
            result = [dict(zip(columns, row)) for row in rows]

        return JsonResponse({'status': 'success', 'salesmen': result}, safe=False)

    except Exception as e:
        return JsonResponse({'status': 'error', 'message': str(e)}, status=500)

@csrf_exempt
def get_salesmen_excluding_negative3(request, supervisor_no=None):
    try:
        with connection.cursor() as cursor:
            # Decide the filtering condition based on whether supervisor_no is provided
            if supervisor_no:
                subquery = f"""
                    SELECT CAST(SALESMAN_NO AS NVARCHAR)
                    FROM BUYP.dbo.SUPERVISOR_ACCESS_TBL
                    WHERE SUPERVISOR_NO = '{supervisor_no}'
                """
            else:
                subquery = """
                    SELECT CAST(SALESMAN_NO AS NVARCHAR)
                    FROM BUYP.dbo.SUPERVISOR_ACCESS_TBL
                """

            query = f"""
                SELECT
                    SR.ORG_ID,
                    SR.SALESREP_ID,
                    SR.SALESREP_NUMBER,
                    SR.NAME,
                    WH.WAREHOUSE_NAME AS ORG_NAME,
                    WH.REGION_NAME
                FROM BUYP.BUYP.ALJE_SALESREP SR
                LEFT JOIN BUYP.dbo.ALJE_PHYSICAL_WHR WH
                    ON SR.ORG_ID = WH.ORGANIZATION_ID
                WHERE SR.SALESREP_ID != -3
                AND SR.SALESREP_NUMBER NOT IN ({subquery})
            """

            cursor.execute(query)
            rows = cursor.fetchall()

            columns = [col[0] for col in cursor.description]
            all_data = [dict(zip(columns, row)) for row in rows]

            # Deduplicate based on SALESREP_NUMBER
            seen_salesrep_numbers = set()
            unique_salesmen = []
            for item in all_data:
                rep_no = item['SALESREP_NUMBER']
                if rep_no not in seen_salesrep_numbers:
                    seen_salesrep_numbers.add(rep_no)
                    unique_salesmen.append(item)

        return JsonResponse({'status': 'success', 'salesmen': unique_salesmen}, safe=False)

    except Exception as e:
        return JsonResponse({'status': 'error', 'message': str(e)}, status=500)

@csrf_exempt
def add_supervisor_access(request):
    if request.method == 'GET':  # or change to 'POST' if needed
        try:
            # Get all parameters from URL
            physical_warehouse = request.GET.get('physical_warehouse')
            org_id = request.GET.get('org_id')
            org_name = request.GET.get('org_name')
            supervisor_no = request.GET.get('supervisor_no')
            supervisor_name = request.GET.get('supervisor_name')
            salesrep_id = request.GET.get('salesrep_id')
            salesman_no = request.GET.get('salesman_no')
            salesman_name = request.GET.get('salesman_name')

            # Validate required fields
            required_fields = [physical_warehouse, org_id, org_name, supervisor_no, supervisor_name, salesrep_id, salesman_no, salesman_name]
            if not all(required_fields):
                return JsonResponse({'status': 'error', 'message': 'Missing one or more required parameters'}, status=400)

            # Insert into the database
            with connection.cursor() as cursor:
                query = '''
                    INSERT INTO BUYP.dbo.SUPERVISOR_ACCESS_TBL 
                    (PHYSICAL_WAREHOUSE, ORG_ID, ORG_NAME, SUPERVISOR_NO, SUPERVISOR_NAME, SALESREP_ID, SALESMAN_NO, SALESMAN_NAME)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                '''
                cursor.execute(query, [
                    physical_warehouse, org_id, org_name,
                    supervisor_no, supervisor_name,
                    salesrep_id, salesman_no, salesman_name
                ])

            return JsonResponse({'status': 'success', 'message': 'Record inserted successfully'})

        except Exception as e:
            return JsonResponse({'status': 'error', 'message': str(e)}, status=500)

    return JsonResponse({'status': 'error', 'message': 'Invalid request method'}, status=405)

class Findstatusforstagereturn(APIView):
    def get(self, request, reqid, productcode, serialno):
        picked_data, truck_data, product_code_data, serialno_data, other_reqid_data = None, None, None, None, None

        # Use a single connection.cursor() block to handle all queries
        with connection.cursor() as cursor:
            # Check in WHR_PICKED_MAN
            cursor.execute("""
                SELECT FLAG 
                FROM WHR_PICKED_MAN
                WHERE REQ_ID = %s AND PRODUCT_CODE = %s AND SERIAL_NO = %s AND FLAG !='SR'
            """, [reqid, productcode, serialno])
            picked_data = cursor.fetchall()

            # Check in WHR_TRUCK_SCAN_DETAILS
            cursor.execute("""
                SELECT * 
                FROM WHR_TRUCK_SCAN_DETAILS
                WHERE REQ_ID = %s AND PRODUCT_CODE = %s AND SERIAL_NO = %s
            """, [reqid, productcode, serialno])
            truck_data = cursor.fetchall()

            # Check if the product code is available under the given ReqID but serial number is incorrect
            cursor.execute("""
                SELECT * 
                FROM WHR_PICKED_MAN
                WHERE REQ_ID = %s AND PRODUCT_CODE = %s
            """, [reqid, productcode])
            product_code_data = cursor.fetchall()

            # Check if the serial number is available under the given ReqID but product code is incorrect
            cursor.execute("""
                SELECT * 
                FROM WHR_PICKED_MAN
                WHERE REQ_ID = %s AND SERIAL_NO = %s
            """, [reqid, serialno])
            serialno_data = cursor.fetchall()

            # Check if both serialno and product code match, but they belong to another ReqID
            cursor.execute("""
                SELECT * 
                FROM WHR_SAVE_TRUCK_DETAILS_TBL
                WHERE Product_code = %s AND Serial_No = %s AND req_no = %s
            """, [productcode, serialno, reqid])
            other_reqid_data = cursor.fetchall()

        # Process the results
        if other_reqid_data:
            return Response({"message": "This product code and serialno are Loading to the truck."}, status=404)

        if picked_data:
            # Extract the FLAG values from the results
            flags = [row[0] for row in picked_data]

            # Check if any FLAG is "SR" or "R"
            if any(flag in ("SR", "R") for flag in flags):
                return Response({"message": "There is no matching data."}, status=404)

        # Check if data exists in both picked_data and truck_data
        if picked_data and truck_data:
            return Response({"message": "This product is already trucking."}, status=200)

        # If serial number is found, but product code is wrong for this ReqID
        if product_code_data and not serialno_data:
            return Response({"message": "The product code is available under the Reqid but serial no is wrong."}, status=404)

        # If product code is found, but serial number is wrong for this ReqID
        if serialno_data and not product_code_data:
            return Response({"message": "The serial no is correct but the product code is wrong."}, status=404)

        # Return appropriate message based on the presence of matching data
        if picked_data:
            return Response({"message": "This product is staging only."}, status=200)

        # Default case: No matching data
        return Response({"message": "There is no matching data."}, status=404)

class GetFilteredTruckDetailsView(viewsets.ViewSet):
    def list(self, request, *args, **kwargs):
        queryset = Truck_scanModels.objects.all()
        reqid = self.kwargs.get('reqid')
        cusno = self.kwargs.get('cusno')
        cussite = self.kwargs.get('cussite')
        itemcode = self.kwargs.get('itemcode')


        try:
            if reqid:
                reqid = int(reqid)  # Convert reqid to integer
                queryset = queryset.filter(REQ_ID=reqid)
            if cusno:
                cusno = int(cusno)  # Convert cusno to integer
                queryset = queryset.filter(CUSTOMER_NUMBER=cusno)
            if cussite:
                cussite = int(cussite)  # Convert cussite to integer
                queryset = queryset.filter(CUSTOMER_SITE_ID=cussite)
            if itemcode:
                itemcode = str(itemcode)  # itemcode might be a string, so no conversion needed
                queryset = queryset.filter(ITEM_CODE=itemcode)
        except ValueError:
            # Handle the case where conversion fails (e.g., invalid format)
            return Response({"detail": "Invalid query parameters."}, status=status.HTTP_400_BAD_REQUEST)


        serializer = Truck_scanTableDetailSerializer(queryset, many=True)
        return Response(serializer.data, status=status.HTTP_200_OK)

# for email and save pdf in buget

class MinioUploadView(APIView):
    parser_classes = [MultiPartParser]
 
    def post(self, request):
        file_obj = request.FILES.get('file')
        if not file_obj:
            return Response({"error": "No file provided"}, status=status.HTTP_400_BAD_REQUEST)
 
        s3 = boto3.client(
            's3',
            endpoint_url=f"http{'s' if settings.MINIO_SECURE else ''}://{settings.MINIO_ENDPOINT}",
            aws_access_key_id=settings.MINIO_ACCESS_KEY,
            aws_secret_access_key=settings.MINIO_SECRET_KEY,
        )
 
        try:
            s3.upload_fileobj(
                Fileobj=file_obj,
                Bucket=settings.MINIO_BUCKET_NAME,
                Key=file_obj.name,
                ExtraArgs={'ACL': 'public-read'}
            )
        except Exception as e:
            return Response({"error": str(e)}, status=500)
 
        file_url = f"http://{settings.MINIO_ENDPOINT}/{settings.MINIO_BUCKET_NAME}/{file_obj.name}"
        return Response({"url": file_url}, status=201)

@csrf_exempt
def send_mail_with_pdf(request):
    if request.method != 'POST':
        return JsonResponse({'status': 'error', 'message': 'Only POST requests are allowed'}, status=405)

    try:
        recipient_email = request.GET.get('email')
        if not recipient_email:
            return JsonResponse({'status': 'error', 'message': 'Missing "email" in URL'}, status=400)

        salesmanname = request.POST.get('salesmanname')
        if not salesmanname:
            return JsonResponse({'status': 'error', 'message': 'Missing "salesmanname" parameter'}, status=400)

        if 'pdf' not in request.FILES:
            return JsonResponse({'status': 'error', 'message': 'No PDF file uploaded'}, status=400)

        pdf_file = request.FILES['pdf']
        pdf_data = pdf_file.read()  # Read once and reuse

        message_body = (
            f"Dear {salesmanname},\n\n"
            "We would like to inform you that your request has been successfully completed and delivered to the customer.\n\n"
            "The attached PDF is provided for your reference.\n"
            "For any further assistance or queries, you may reach out to the Concern team.\n\n"
            "Best regards,\n"
            "Concern Team\n"
        )

        # Step 1: Send Email (Django)
        email_msg = EmailMessage(
            subject='Dispatch Confirmation with Attachment',
            body=message_body,
            from_email=settings.DEFAULT_FROM_EMAIL,
            to=[recipient_email],
        )
        email_msg.attach(pdf_file.name, pdf_data, pdf_file.content_type)
        email_msg.send()

        # Step 2: Rebuild full MIME for IMAP "Sent Items"
        mime_msg = MIMEMultipart()
        mime_msg['From'] = settings.DEFAULT_FROM_EMAIL
        mime_msg['To'] = recipient_email
        mime_msg['Subject'] = 'Dispatch Confirmation with Attachment'
        mime_msg.attach(MIMEText(message_body, 'plain'))

        # Attach PDF
        part = MIMEBase('application', 'pdf')
        part.set_payload(pdf_data)
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', f'attachment; filename="{pdf_file.name}"')
        mime_msg.attach(part)

        # Step 3: Save to Sent Items using IMAP
        imap = imaplib.IMAP4_SSL(settings.EMAIL_HOST, 993)
        imap.login(settings.EMAIL_HOST_USER, settings.EMAIL_HOST_PASSWORD)

        # Try multiple folder names if needed
        for sent_folder in ['Sent', 'Sent Items', '"Sent Items"', '"INBOX.Sent"', '[Gmail]/Sent Mail']:
            try:
                status, _ = imap.select(sent_folder)
                if status == 'OK':
                    break
            except:
                continue
        else:
            return JsonResponse({'status': 'error', 'message': 'Could not find Sent folder on IMAP server'})

        imap.append(sent_folder, '', imaplib.Time2Internaldate(time.time()), mime_msg.as_bytes())
        imap.logout()

        return JsonResponse({'status': 'success', 'message': 'Email sent and saved with attachment to Sent folder!'})

    except Exception as e:
        return JsonResponse({'status': 'error', 'message': str(e)})

def get_employee_address(request, empno):
    try:
        with connection.cursor() as cursor:
            query = """
                SELECT  EMPLOYEE_NUMBER, EMAIL_ADDRESS, EFFECTIVE_END_DATE
                FROM [BUYP].[dbo].[ALJE_EMPLOYEE_BUYP]
                WHERE EMPLOYEE_NUMBER = %s
                ORDER BY ABS(DATEDIFF(DAY, GETDATE(), EFFECTIVE_END_DATE)); 
            """
            cursor.execute(query, [empno])
            row = cursor.fetchone()

            if row:
                data = {
                    'EMPLOYEE_NUMBER': row[0],
                    'EMAIL_ADDRESS': row[1],
                    'EFFECTIVE_END_DATE': row[2]
                }
                return JsonResponse({'status': 'success','data': data})
            else:
                return JsonResponse({'status': 'error', 'message': 'Employee not found'}, status=404)

    except Exception as e:
        return JsonResponse({'status': 'error', 'message': str(e)}, status=500)

# class WMS_SoftwareVersionView(viewsets.ViewSet):
#     def list(self, request, softwarename=None):
#         if softwarename:
#             version = WMS_SoftwareVersionModels.objects.filter(App_Name=softwarename).values_list('Current_Version', flat=True).first()
#             if version:
#                 return Response({"version": version})  # Return a single string instead of a list
#             return Response({"error": "No version found for this software"}, status=404)
#         return Response({"error": "Software name is required"}, status=400)

class WMS_SoftwareVersionView(viewsets.ViewSet):
    def list(self, request, softwarename=None):
        checkstatus = request.query_params.get('checkstatus', None)

        if not softwarename:
            return Response({"error": "Software name is required"}, status=400)

        try:
            # Normal version column
            if checkstatus != "mobileapp":
                version = WMS_SoftwareVersionModels.objects.filter(
                    App_Name=softwarename
                ).values_list('Current_Version', flat=True).first()
            # Mobile app version column
            else:
                version = WMS_SoftwareVersionModels.objects.filter(
                    App_Name=softwarename
                ).values_list('MobileApp_Version', flat=True).first()

            if version:
                return Response({"version": version})
            else:
                return Response(
                    {"error": "No version found for this software"}, status=404
                )

        except Exception as e:
            return Response({"error": str(e)}, status=500)
    
class GetPlayStoreWarningMsgView(APIView):
    """
    Fetch app version details based only on 'status'.
    If Play_store_Warning is 'yes', return 'yes' and Running_time.
    Otherwise, return 'no'.
    """

    def get(self, request):


        try:
            # ✅ Query the table safely using parameterized SQL
            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT TOP 1 
                        App_Name,
                        Current_Version,
                        MobileApp_Version,
                        Play_store_Warning,
                        Running_time
                    FROM WMS_VERSION_CONTROLE_TBL
                    ORDER BY id DESC
                """, )

                row = cursor.fetchone()

            # ✅ Handle results properly
            if row:
                (
                    App_Name,
                    Current_Version,
                    MobileApp_Version,
                    Play_store_Warning,
                    Running_time
                ) = row

                # ✅ Build the response
                if Play_store_Warning and Play_store_Warning.lower() == 'yes':
                    data = {
                        "App_Name": App_Name,
                        "Play_store_Warning": "yes",
                        "Running_time": Running_time
                    }
                else:
                    data = {
                        "App_Name": App_Name,
                        "Play_store_Warning": "no"
                    }

                return Response(data, status=status.HTTP_200_OK)

            else:
                return Response(
                    {"message": "No records found for the given status."},
                    status=status.HTTP_404_NOT_FOUND
                )

        except Exception as e:
            # ✅ Catch and show server errors safely
            return Response(
                {"error": str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
        
class CompareScanDat_for_noproductcodeView(APIView):
    def get(self, request, req_id, customername, customersite):
        # Step 1: Filter Pickman, Truck, and Dispatch data based on base fields
        pickman_data = Pickman_ScanModels.objects.filter(
            REQ_ID=req_id,
            CUSTOMER_NUMBER=customername,
            CUSTOMER_SITE_ID=customersite,
            PRODUCT_CODE='00',
        ).exclude(SERIAL_NO='null' ).exclude(FLAG__in=['SR', 'R'])

        truck_data = Truck_scanModels.objects.filter(
            REQ_ID=req_id,
            CUSTOMER_NUMBER=customername,
            CUSTOMER_SITE_ID=customersite,
            PRODUCT_CODE='00',
        ).exclude(SERIAL_NO='null')

        dispatch_data = ToGetGenerateDispatch.objects.filter(
            req_no=req_id,
            Customer_no=customername,
            Customer_Site=customersite,
            Product_code='00',
        ).exclude(Serial_No='null')

        # Debug logs (you can remove later)
        # print(f"Pickman Data Count: {pickman_data.count()}")
        # print(f"Truck Data Count: {truck_data.count()}")
        # print(f"Dispatch Data Count: {dispatch_data.count()}")

        # Step 2: Exclude Pickman rows that exist in Truck with matching SERIAL_NO
        truck_serial_nos = truck_data.values_list('SERIAL_NO', flat=True)
        pickman_filtered_data = pickman_data.exclude(SERIAL_NO__in=truck_serial_nos)

        # print(f"After excluding truck data, remaining Pickman: {pickman_filtered_data.count()}")

        # Step 3: Exclude Pickman rows that exist in Dispatch with matching Serial_No
        dispatch_serial_nos = dispatch_data.values_list('Serial_No', flat=True)
        pickman_only_data = pickman_filtered_data.exclude(SERIAL_NO__in=dispatch_serial_nos)

        # print(f"After excluding dispatch data, final Pickman only: {pickman_only_data.count()}")

        # Step 4: Final response
        if pickman_only_data.exists():
            serializer = Pickman_ScanModelsserializers(pickman_only_data, many=True)
            return Response({
                'status': 'success',
                'message': 'Data found only in Pickman Scan (not in Truck or Dispatch)',
                'data': serializer.data
            }, status=status.HTTP_200_OK)
        elif pickman_data.exists() and (truck_data.exists() or dispatch_data.exists()):
            return Response({
                'status': 'info',
                'message': 'All Pickman data exists in either Truck or Dispatch.'
            }, status=status.HTTP_200_OK)
        else:
            return Response({
                'status': 'error',
                'message': 'No matching data found.'
            }, status=status.HTTP_404_NOT_FOUND)

class CompareScanDat_for_noserialnoView(APIView):
    def get(self, request, req_id, customername, customersite):
        # Step 1: Filter Pickman, Truck, and Dispatch data based on base fields
        pickman_data = Pickman_ScanModels.objects.filter(
            REQ_ID=req_id,
            CUSTOMER_NUMBER=customername,
            CUSTOMER_SITE_ID=customersite,  
            SERIAL_NO='null'
        ).exclude(PRODUCT_CODE='00').exclude(FLAG__in=['SR', 'R'])

        truck_data = Truck_scanModels.objects.filter(
            REQ_ID=req_id,
            CUSTOMER_NUMBER=customername,
            CUSTOMER_SITE_ID=customersite,
            SERIAL_NO='null'
        ).exclude(PRODUCT_CODE='00')

        dispatch_data = ToGetGenerateDispatch.objects.filter(
            req_no=req_id,
            Customer_no=customername,
            Customer_Site=customersite,
            Serial_No='null'
        ).exclude(Product_code='00')

        # Debug logs (you can remove later)
        # print(f"Pickman Data Count: {pickman_data.count()}")
        # print(f"Truck Data Count: {truck_data.count()}")
        # print(f"Dispatch Data Count: {dispatch_data.count()}")

        # Step 2: Exclude Pickman rows that already exist in Truck (by CUSTOMER_TRX_ID)
        truck_trx_ids = truck_data.values_list('CUSTOMER_TRX_ID', flat=True)
        pickman_only_data = pickman_data.exclude(CUSTOMER_TRX_ID__in=truck_trx_ids)

        # print(f"After excluding truck data, remaining Pickman: {pickman_only_data.count()}")

        # Step 3: Exclude Pickman rows that already exist in Dispatch (by Customer_trx_id)
        dispatch_trx_ids = dispatch_data.values_list('Customer_trx_id', flat=True)
        pickman_only_data = pickman_only_data.exclude(CUSTOMER_TRX_ID__in=dispatch_trx_ids)

        # print(f"After excluding dispatch data, final Pickman only: {pickman_only_data.count()}")

        # Step 4: Final response
        if pickman_only_data.exists():
            serializer = Pickman_ScanModelsserializers(pickman_only_data, many=True)
            return Response({
                'status': 'success',
                'message': 'Data found only in Pickman Scan (not in Truck or Dispatch)',
                'data': serializer.data
            }, status=status.HTTP_200_OK)

        elif pickman_data.exists() and truck_data.exists():
            return Response({
                'status': 'info',
                'message': 'There is no bypass data for this customer.'
            }, status=status.HTTP_200_OK)

        else:
            return Response({
                'status': 'error',
                'message': 'No matching data found.'
            }, status=status.HTTP_404_NOT_FOUND)

class Deliver_noserialno_bypassesView(APIView):
    def get(self, request, dispatch_id):
        data = Truck_scanModels.objects.filter(DISPATCH_ID=dispatch_id)

        # 1. No Serial No but valid Product Code
        noserial_data = data.filter(SERIAL_NO='null').exclude(PRODUCT_CODE='00')
        noserial_qty = noserial_data.aggregate(total_qty=Sum('TRUCK_SEND_QTY'))['total_qty'] or 0

        # 2. Bypass Products: Product Code == '00' and Serial No is null
        bypass_data = data.filter(PRODUCT_CODE='00', SERIAL_NO='null')
        bypass_qty = bypass_data.aggregate(total_qty=Sum('TRUCK_SEND_QTY'))['total_qty'] or 0

        # 3. No Product Code: Product Code == '00' and Serial No is NOT null
        no_productcode_data = data.filter(PRODUCT_CODE='00').exclude(SERIAL_NO='null')
        no_productcode_qty = no_productcode_data.aggregate(total_qty=Sum('TRUCK_SEND_QTY'))['total_qty'] or 0

        messages = []
        if noserial_qty > 0:
            messages.append(f"This dispatch ID has No Serial No items with total quantity: {noserial_qty}")
        if bypass_qty > 0:
            messages.append(f"This dispatch ID has Bypass Products (no serial no) with total quantity: {bypass_qty}")
        if no_productcode_qty > 0:
            messages.append(f"This dispatch ID has No Product Code items (with serial no) with total quantity: {no_productcode_qty}")
        if noserial_qty == 0 and bypass_qty == 0 and no_productcode_qty == 0:
            messages.append("This dispatch ID has no No Serial No items, no Bypass Products, and no No Product Code items.")

        return Response({
            'dispatch_id': dispatch_id,
            'no_serial_qty': noserial_qty,
            'bypass_qty': bypass_qty,
            'no_productcode_qty': no_productcode_qty,
            'messages': messages,
        })
    
class CompareScanDat_for_bypassView(APIView):
    def get(self, request, req_id, customername, customersite):
        # Step 1: Filter Pickman, Truck, and Dispatch data based on base fields
        pickman_data = Pickman_ScanModels.objects.filter(
            REQ_ID=req_id,
            CUSTOMER_NUMBER=customername,
            CUSTOMER_SITE_ID=customersite,
            PRODUCT_CODE='00',
            SERIAL_NO='null'
        ).exclude(FLAG__in=['SR', 'R'])

        truck_data = Truck_scanModels.objects.filter(
            REQ_ID=req_id,
            CUSTOMER_NUMBER=customername,
            CUSTOMER_SITE_ID=customersite,
            PRODUCT_CODE='00',
            SERIAL_NO='null'
        ).exclude(FLAG__in=['SR', 'R'])

        dispatch_data = ToGetGenerateDispatch.objects.filter(
            req_no=req_id,
            Customer_no=customername,
            Customer_Site=customersite,
            Product_code='00',
            Serial_No='null'
        )

        # Debug logs (you can remove later)
        # print(f"Pickman Data Count: {pickman_data.count()}")
        # print(f"Truck Data Count: {truck_data.count()}")
        # print(f"Dispatch Data Count: {dispatch_data.count()}")

        # Step 2: Exclude Pickman rows that already exist in Truck (by CUSTOMER_TRX_ID)
        truck_trx_ids = truck_data.values_list('CUSTOMER_TRX_ID', flat=True)
        pickman_only_data = pickman_data.exclude(CUSTOMER_TRX_ID__in=truck_trx_ids)

        # print(f"After excluding truck data, remaining Pickman: {pickman_only_data.count()}")

        # Step 3: Exclude Pickman rows that already exist in Dispatch (by Customer_trx_id)
        dispatch_trx_ids = dispatch_data.values_list('Customer_trx_id', flat=True)
        pickman_only_data = pickman_only_data.exclude(CUSTOMER_TRX_ID__in=dispatch_trx_ids)

        # print(f"After excluding dispatch data, final Pickman only: {pickman_only_data.count()}")

        # Step 4: Final response
        if pickman_only_data.exists():
            serializer = Pickman_ScanModelsserializers(pickman_only_data, many=True)
            return Response({
                'status': 'success',
                'message': 'Data found only in Pickman Scan (not in Truck or Dispatch)',
                'data': serializer.data
            }, status=status.HTTP_200_OK)

        elif pickman_data.exists() and truck_data.exists():
            return Response({
                'status': 'info',
                'message': 'There is no bypass data for this customer.'
            }, status=status.HTTP_200_OK)

        else:
            return Response({
                'status': 'error',
                'message': 'No matching data found.'
            }, status=status.HTTP_404_NOT_FOUND)

class SaveInvoiceReturnHistory(APIView):
    def post(self, request):
        try:
            data = request.data
            # Extract fields from request
            invoice_return_id = data.get("INVOICE_RETURN_ID")
            date = data.get("DATE")
            org_id = data.get("ORG_ID")
            org_name = data.get("ORG_NAME")
            salesman_no = data.get("SALESMANO_NO")           
            manager_name = data.get("MANAGER_NAME")
            manager_no = data.get("MANAGER_NO")
            customer_number = data.get("CUSTOMER_NUMBER")
            customer_name = data.get("CUSTOMER_NAME")
            customer_site_id = data.get("CUSTOMER_SITE_ID")
            invoice_number = data.get("INVOICE_NUMBER")
            customer_trx_id = data.get("CUSTOMER_TRX_ID")
            customer_trx_line_id = data.get("CUSTOMER_TRX_LINE_ID")
            line_number = data.get("LINE_NUMBER")
            
            undel_id = data.get("UNDEL_ID")
            item_code = data.get("ITEM_CODE")
            item_description = data.get("ITEM_DESCRIPTION")
            tot_quantity = data.get("TOT_QUANTITY")
            dispatched_qty = data.get("DISPATCHED_QTY")
            returned_qty = data.get("RETURNED_QTY")
            flag_status = data.get("FLAG_STATUS")
            # remarks = data.get("REMARKS")
            remarks = data.get("REMARKS") if "REMARKS" in data else None

            # Fetch SALESMAN_NAME using the SALESMANO_NO
            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT TOP 1 NAME 
                    FROM [BUYP].[BUYP].[ALJE_SALESREP] 
                    WHERE SALESREP_NUMBER = %s
                """, [salesman_no])
                result = cursor.fetchone()
                salesman_name = result[0] if result else ''  # Default to empty string if not found

            # Insert all data including SALESMAN_NAME
            with connection.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO WHR_INVOICE_RETURN_HISTORY_TBL (
                        INVOICE_RETURN_ID, DATE, ORG_ID, ORG_NAME,
                        MANAGER_NO, MANAGER_NAME, SALESMANO_NO, SALESMAN_NAME,
                        CUSTOMER_NUMBER, CUSTOMER_NAME, CUSTOMER_SITE_ID,
                        INVOICE_NUMBER, CUSTOMER_TRX_ID, CUSTOMER_TRX_LINE_ID,
                        LINE_NUMBER, UNDEL_ID, ITEM_CODE, ITEM_DESCRIPTION,
                        TOT_QUANTITY, DISPATCHED_QTY, RETURNED_QTY, FLAG_STATUS,REMARKS
                    ) VALUES (
                        %s, %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s, %s,
                        %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s, %s, %s,%s
                    )
                """, [
                    invoice_return_id, date, org_id, org_name,
                    manager_no, manager_name, salesman_no, salesman_name,
                    customer_number, customer_name, customer_site_id,
                    invoice_number, customer_trx_id, customer_trx_line_id,
                    line_number, undel_id, item_code, item_description,
                    tot_quantity, dispatched_qty, returned_qty, flag_status,remarks
                ])

            return Response({"message": "Data inserted successfully."}, status=status.HTTP_201_CREATED)

        except Exception as e:
            return Response({"error": str(e)}, status=status.HTTP_400_BAD_REQUEST)

class PickedManInsertView(APIView):
    def post(self, request):
        data = request.data

        try:
            # Parse and convert fields to appropriate data types
            fields_to_int = [
                'CUSTOMER_TRX_ID', 'CUSTOMER_TRX_LINE_ID', 'PICKED_QTY',
                'DISPATCHED_QTY', 'BALANCE_QTY', 'TOT_QUANTITY',
                'LINE_NUMBER', 'CUSTOMER_SITE_ID', 'CUSTOMER_NUMBER',
                'ORG_ID', 'SALESMAN_NO', 'MANAGER_NO', 'PICKMAN_NO'
            ]
            for field in fields_to_int:
                data[field] = int(data.get(field, 0))

            # Parse datetime fields
            dt_fields = ['DATE', 'INVOICE_DATE', 'CREATION_DATE', 'LAST_UPDATE_DATE']
            for field in dt_fields:
                if field in data and data[field]:
                    data[field] = datetime.fromisoformat(data[field])
                else:
                    data[field] = None

            # Add missing fields with default values if not present
            data['CREATED_IP'] = data.get('CREATED_IP', '127.0.0.1')
            data['CREATED_MAC'] = data.get('CREATED_MAC', '00:00:00:00:00:00')
            data['LAST_UPDATED_BY'] = data.get('LAST_UPDATED_BY', data.get('CREATED_BY', 'System'))
            data['LAST_UPDATE_IP'] = data.get('LAST_UPDATE_IP', '127.0.0.1')

            with connection.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO WHR_PICKED_MAN (
                        PICK_ID, REQ_ID, DATE, ASSIGN_PICKMAN, PHYSICAL_WAREHOUSE,
                        ORG_ID, ORG_NAME, SALESMAN_NO, SALESMAN_NAME,
                        MANAGER_NO, MANAGER_NAME, PICKMAN_NO, PICKMAN_NAME,
                        CUSTOMER_NUMBER, CUSTOMER_NAME, CUSTOMER_SITE_ID,
                        INVOICE_DATE, INVOICE_NUMBER, LINE_NUMBER,
                        INVENTORY_ITEM_ID, ITEM_DESCRIPTION,
                        CUSTOMER_TRX_ID, CUSTOMER_TRX_LINE_ID,
                        TOT_QUANTITY, DISPATCHED_QTY, BALANCE_QTY,
                        PICKED_QTY, PRODUCT_CODE, SERIAL_NO,
                        CREATION_DATE, CREATED_BY, CREATED_IP, CREATED_MAC,
                        LAST_UPDATE_DATE, LAST_UPDATED_BY, LAST_UPDATE_IP,
                        FLAG, UNDEL_ID
                    )
                    VALUES (
                        %(PICK_ID)s, %(REQ_ID)s, %(DATE)s, %(ASSIGN_PICKMAN)s, %(PHYSICAL_WAREHOUSE)s,
                        %(ORG_ID)s, %(ORG_NAME)s, %(SALESMAN_NO)s, %(SALESMAN_NAME)s,
                        %(MANAGER_NO)s, %(MANAGER_NAME)s, %(PICKMAN_NO)s, %(PICKMAN_NAME)s,
                        %(CUSTOMER_NUMBER)s, %(CUSTOMER_NAME)s, %(CUSTOMER_SITE_ID)s,
                        %(INVOICE_DATE)s, %(INVOICE_NUMBER)s, %(LINE_NUMBER)s,
                        %(INVENTORY_ITEM_ID)s, %(ITEM_DESCRIPTION)s,
                        %(CUSTOMER_TRX_ID)s, %(CUSTOMER_TRX_LINE_ID)s,
                        %(TOT_QUANTITY)s, %(DISPATCHED_QTY)s, %(BALANCE_QTY)s,
                        %(PICKED_QTY)s, %(PRODUCT_CODE)s, %(SERIAL_NO)s,
                        %(CREATION_DATE)s, %(CREATED_BY)s, %(CREATED_IP)s, %(CREATED_MAC)s,
                        %(LAST_UPDATE_DATE)s, %(LAST_UPDATED_BY)s, %(LAST_UPDATE_IP)s,
                        %(FLAG)s, %(UNDEL_ID)s
                    )
                """, data)

            return Response({'message': 'Data inserted successfully'}, status=status.HTTP_201_CREATED)

        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_400_BAD_REQUEST)


# class UpdateDispatchQtyView(APIView):
#     def get(self, request, id, qty):
#         try:
#             with connection.cursor() as cursor:
#                 # Step 1: Get current values
#                 cursor.execute("""
#                     SELECT DISPATCHED_QTY, SCANNED_QTY, PICKED_QTY
#                     FROM WHR_DISPATCH_REQUEST 
#                     WHERE ID = %s
#                 """, [id])
#                 row = cursor.fetchone()

#                 if not row:
#                     return Response({"error": "Record not found"}, status=status.HTTP_404_NOT_FOUND)

#                 dispatched_qty = row[0] or 0
#                 scanned_qty = row[1] or 0
#                 picked_qty = row[2] or 0

#                 # Step 2: Calculate new_qty but do not exceed picked_qty
#                 new_qty = scanned_qty + int(qty)
#                 if new_qty > picked_qty:
#                     new_qty = picked_qty  # cap at picked_qty

#                 # Step 3: Determine status
#                 status_value = 'Finished' if new_qty == picked_qty else 'pending'

#                 # Step 4: Update record
#                 cursor.execute("""
#                     UPDATE WHR_DISPATCH_REQUEST 
#                     SET SCANNED_QTY = %s, STATUS = %s 
#                     WHERE ID = %s
#                 """, [new_qty, status_value, id])

#             # Step 5: Response with picked_qty also
#             return Response({
#                 "message": "SCANNED_QTY and STATUS updated successfully",
#                 "new_qty": new_qty,
#                 "picked_qty": picked_qty,
#                 "status": status_value
#             }, status=status.HTTP_200_OK)

#         except Exception as e:
#             return Response({"error": str(e)}, status=status.HTTP_400_BAD_REQUEST)

# ===============================
# 1️⃣ UPDATED: UpdateDispatchQtyView
# ===============================
class UpdateDispatchQtyView:
    """
    Updates SCANNED_QTY and STATUS in WHR_DISPATCH_REQUEST 
    based on rows in WHR_PICKED_MAN.
    """

    @staticmethod
    def update_qty(req_id, pick_id, undel_id, invoice_number=None, inventory_item_id=None):
        try:
            with transaction.atomic():
                with connection.cursor() as cursor:

                    # 1️⃣ Fetch the dispatch request record
                    cursor.execute("""
                        SELECT ID, PICKED_QTY, SCANNED_QTY
                        FROM WHR_DISPATCH_REQUEST
                        WHERE REQ_ID = %s
                          AND PICK_ID = %s
                          AND LTRIM(RTRIM(CONVERT(varchar(200), UNDEL_ID))) 
                                = LTRIM(RTRIM(CONVERT(varchar(200), %s)))
                    """, [req_id, pick_id, undel_id])

                    row = cursor.fetchone()
                    if not row:
                        return {"error": "Matching dispatch request not found"}

                    dispatch_id, picked_qty, scanned_qty = row
                    dispatched_qty = float(picked_qty or 0)

                    # 2️⃣ Count scanned rows in WHR_PICKED_MAN
                    cursor.execute("""
                        SELECT COUNT(*)
                        FROM WHR_PICKED_MAN
                        WHERE REQ_ID = %s
                          AND PICK_ID = %s
                          AND LTRIM(RTRIM(CONVERT(varchar(200), UNDEL_ID))) 
                                = LTRIM(RTRIM(CONVERT(varchar(200), %s)))
                    """, [req_id, pick_id, undel_id])

                    pickedman_count = int(cursor.fetchone()[0] or 0)

                    # 3️⃣ Determine new status
                    new_scanned_qty = pickedman_count
                    status_value = "Finished" if pickedman_count == dispatched_qty else "pending"

                    # 4️⃣ Update the dispatch request
                    cursor.execute("""
                        UPDATE WHR_DISPATCH_REQUEST
                        SET SCANNED_QTY = %s,
                            STATUS = %s
                        WHERE ID = %s
                    """, [new_scanned_qty, status_value, dispatch_id])

            return {
                "message": "SCANNED_QTY & STATUS updated successfully",
                "req_id": req_id,
                "pick_id": pick_id,
                "undel_id": undel_id,
                "dispatched_qty": dispatched_qty,
                "pickedman_row_count": pickedman_count,
                "scanned_qty_set": new_scanned_qty,
                "status_set": status_value
            }

        except Exception as e:
            return {"error": str(e)}


# class UpdateTruckAndPickedManWhileScanView(APIView):
#     def post(self, request):
#         try:
#             # Extract data from request
#             reqid = request.data.get("reqid")
#             pickid = request.data.get("pickid")
#             invoiceno = request.data.get("invoiceno")
#             itemcode = request.data.get("itemcode")
#             description = request.data.get("description")
#             product_code = request.data.get("Product Code")
#             serial_no = request.data.get("Serial No")

#             if not all([reqid, pickid, invoiceno, itemcode, description, product_code, serial_no]):
#                 return Response({"error": "Missing required fields."}, status=status.HTTP_400_BAD_REQUEST)

#             with connection.cursor() as cursor:

#                 # --- Update WHR_SAVE_TRUCK_DETAILS_TBL ---
#                 cursor.execute("""
#                     SELECT TOP 1 id
#                     FROM WHR_SAVE_TRUCK_DETAILS_TBL
#                     WHERE req_no = %s AND pick_id = %s AND invoice_no = %s
#                     AND Item_code = %s AND Item_detailas = %s
#                     AND Product_code = 'empty' AND Serial_No = 'empty'
#                 """, [reqid, pickid, invoiceno, itemcode, description])
#                 truck_row = cursor.fetchone()

#                 if truck_row:
#                     truck_id = truck_row[0]
#                     cursor.execute("""
#                         UPDATE WHR_SAVE_TRUCK_DETAILS_TBL
#                         SET Product_code = %s, Serial_No = %s
#                         WHERE id = %s
#                     """, [product_code, serial_no, truck_id])
#                 else:
#                     return Response({"error": "No matching record found in WHR_SAVE_TRUCK_DETAILS_TBL"}, status=status.HTTP_404_NOT_FOUND)

#                 # --- Update WHR_PICKED_MAN ---
#                 cursor.execute("""
#                     SELECT TOP 1 id 
#                     FROM WHR_PICKED_MAN
#                     WHERE REQ_ID = %s AND PICK_ID = %s AND INVOICE_NUMBER = %s 
#                     AND INVENTORY_ITEM_ID = %s AND ITEM_DESCRIPTION = %s 
#                     AND PRODUCT_CODE = 'empty' AND SERIAL_NO = 'empty'
#                 """, [reqid, pickid, invoiceno, itemcode, description])
#                 pick_row = cursor.fetchone()

#                 if pick_row:
#                     pickman_id = pick_row[0]
#                     cursor.execute("""
#                         UPDATE WHR_PICKED_MAN
#                         SET PRODUCT_CODE = %s, SERIAL_NO = %s
#                         WHERE id = %s
#                     """, [product_code, serial_no, pickman_id])
#                 else:
#                     return Response({"error": "No matching record found in WHR_PICKED_MAN"}, status=status.HTTP_404_NOT_FOUND)

#             return Response({"message": "Records updated successfully"}, status=status.HTTP_200_OK)

#         except Exception as e:
#             return Response({"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class UpdateTruckAndPickedManWhileScanView(APIView):
    def post(self, request):
        try:
            # Extract common fields
            reqid = request.data.get("reqid")
            pickid = request.data.get("pickid")
            invoiceno = request.data.get("invoiceno")
            itemcode = request.data.get("itemcode")
            description = request.data.get("description")
            tabledata = request.data.get("tabledata", [])

            if not all([reqid, pickid, invoiceno, itemcode, description]) or not tabledata:
                return Response({"error": "Missing required fields."}, status=status.HTTP_400_BAD_REQUEST)

            updated_records = []

            with transaction.atomic():
                with connection.cursor() as cursor:
                    for entry in tabledata:
                        product_code = entry.get("Product Code")
                        serial_no = entry.get("Serial No")

                        if not product_code or not serial_no:
                            continue  # skip invalid entry

                        # --- Update WHR_SAVE_TRUCK_DETAILS_TBL ---
                        cursor.execute("""
                            SELECT TOP 1 id
                            FROM WHR_SAVE_TRUCK_DETAILS_TBL
                            WHERE req_no = %s AND pick_id = %s AND invoice_no = %s
                            AND Item_code = %s AND Item_detailas = %s
                            AND Product_code = 'empty' AND Serial_No = 'empty'
                        """, [reqid, pickid, invoiceno, itemcode, description])
                        truck_row = cursor.fetchone()

                        if truck_row:
                            truck_id = truck_row[0]
                            cursor.execute("""
                                UPDATE WHR_SAVE_TRUCK_DETAILS_TBL
                                SET Product_code = %s, Serial_No = %s
                                WHERE id = %s
                            """, [product_code, serial_no, truck_id])
                        else:
                            return Response(
                                {"error": f"No matching record found in WHR_SAVE_TRUCK_DETAILS_TBL for Product={product_code}, Serial={serial_no}"},
                                status=status.HTTP_404_NOT_FOUND
                            )

                        # --- Update WHR_PICKED_MAN ---
                        cursor.execute("""
                            SELECT TOP 1 id 
                            FROM WHR_PICKED_MAN
                            WHERE REQ_ID = %s AND PICK_ID = %s AND INVOICE_NUMBER = %s 
                            AND INVENTORY_ITEM_ID = %s AND ITEM_DESCRIPTION = %s 
                            AND PRODUCT_CODE = 'empty' AND SERIAL_NO = 'empty'
                        """, [reqid, pickid, invoiceno, itemcode, description])
                        pick_row = cursor.fetchone()

                        if pick_row:
                            pickman_id = pick_row[0]
                            cursor.execute("""
                                UPDATE WHR_PICKED_MAN
                                SET PRODUCT_CODE = %s, SERIAL_NO = %s
                                WHERE id = %s
                            """, [product_code, serial_no, pickman_id])
                        else:
                            return Response(
                                {"error": f"No matching record found in WHR_PICKED_MAN for Product={product_code}, Serial={serial_no}"},
                                status=status.HTTP_404_NOT_FOUND
                            )

                        updated_records.append({"Product Code": product_code, "Serial No": serial_no})

            return Response(
                {"message": "Records updated successfully", "updated": updated_records},
                status=status.HTTP_200_OK
            )

        except Exception as e:
            return Response({"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@csrf_exempt
def update_emp_role(request):
    if request.method == 'POST':
        try:
            # Parse request body
            data = json.loads(request.body.decode('utf-8'))
            employee_id = data.get('employee_id')
            emp_role = data.get('emp_role')

            if not employee_id:
                return JsonResponse({'status': 'error', 'message': 'employee_id is required.'}, status=400)

            # Perform update using raw SQL
            with connection.cursor() as cursor:
                cursor.execute("""
                    UPDATE [BUYP].[dbo].[WHR_USER_MANAGEMENT]
                    SET EMP_ROLE = %s
                    WHERE EMPLOYEE_ID = %s
                """, [emp_role, employee_id])

            return JsonResponse({'status': 'success', 'message': 'Role updated successfully.'})

        except Exception as e:
            return JsonResponse({'status': 'error', 'message': str(e)}, status=500)

    else:
        return JsonResponse({'status': 'error', 'message': 'Only POST method allowed.'}, status=405)

class AssignPickmanView(APIView):
    def post(self, request):
        pickid = request.data.get('pickid')
        reqno = request.data.get('reqno')
        pickman_name = request.data.get('pickman_name')

        if not pickid or not reqno or not pickman_name:
            return Response({'error': 'Missing required fields'}, status=status.HTTP_400_BAD_REQUEST)

        with connection.cursor() as cursor:
            # Check if already scanned
            cursor.execute("""
                SELECT 1 FROM WHR_PICKED_MAN
                WHERE PICK_ID = %s AND REQ_ID = %s
            """, [pickid, reqno])
            result = cursor.fetchone()

            if result:
                return Response({'message': 'Pickman has already started scanning. Cannot assign again.'}, status=status.HTTP_400_BAD_REQUEST)

            # Update dispatch request table
            cursor.execute("""
                UPDATE WHR_DISPATCH_REQUEST
                SET ASSIGN_PICKMAN = %s
                WHERE PICK_ID = %s AND REQ_ID = %s
            """, [pickman_name, pickid, reqno])

        return Response({'message': 'Pickman assigned successfully'}, status=status.HTTP_200_OK)

# Setup logger for error tracking
logger = logging.getLogger(__name__)

# class GroupedTruckScanDetailsView(APIView):
#     def get(self, request):
#         try:
#             # Get query parameters
#             from_date_str = request.query_params.get('from_date')
#             to_date_str = request.query_params.get('to_date')
#             undel_id_param = request.query_params.get('undel_id')
#             customer_no_param = request.query_params.get('customer_no')
#             salesman_name_param = request.query_params.get('salesman_name')

#             if not from_date_str or not to_date_str:
#                 return Response(
#                     {"error": "from_date and to_date query parameters are required in YYYY-MM-DD format."},
#                     status=status.HTTP_400_BAD_REQUEST
#                 )

#             from_date = parse_date(from_date_str)
#             to_date = parse_date(to_date_str)

#             if from_date is None or to_date is None:
#                 return Response(
#                     {"error": "Invalid date format. Use YYYY-MM-DD."},
#                     status=status.HTTP_400_BAD_REQUEST
#                 )

#             to_date_plus_one = to_date + timedelta(days=1)

#             # # Base query
#             # query = """
#             #     SELECT
#             #     d.DISPATCH_ID,
#             #     d.CUSTOMER_NUMBER,
#             #     d.PHYSICAL_WAREHOUSE,
#             #     d.CUSTOMER_NAME,
#             #     d.[DATE]                       AS RECORD_DATE,
#             #     uagg.INVOICE_DATE,
#             #     uagg.QUANTITY,
#             #     d.DELIVERY_DATE,
#             #     d.SALESMAN_NO,
#             #     d.SALESMAN_NAME,
#             #     d.UNDEL_ID,
#             #     d.INVOICE_NO,
#             #     d.ITEM_CODE,
#             #     d.ITEM_DETAILS,
#             #     d.TRANSPORTER_NAME,
#             #     d.DRIVER_NAME,
#             #     d.VEHICLE_NO,
#             #     d.LOADING_CHARGES,
#             #     d.TRANSPORT_CHARGES,
#             #     d.MISC_CHARGES,
#             #     d.REMARKS,
#             #     d.DELIVERYADDRESS,
#             #     COUNT(*)                       AS TOTAL_TRUCK_QTY
#             # FROM BUYP.dbo.WHR_TRUCK_SCAN_DETAILS d
#             # LEFT JOIN (
#             #     -- one row per UNDEL_ID to avoid duplicating d rows
#             #     SELECT UNDEL_ID,
#             #         MAX(INVOICE_DATE) AS INVOICE_DATE,
#             #         QUANTITY     AS QUANTITY
#             #     FROM BUYP.XXALJE_UNDELIVERED_DATA_BUYP1
#             #     GROUP BY UNDEL_ID,QUANTITY
#             # ) uagg
#             # ON d.UNDEL_ID = uagg.UNDEL_ID
#             # WHERE d.FLAG != 'R'
#             #       AND d.[DATE] >= %s AND d.[DATE] < %s
#             # """


#             # Base query
#             query = """
#                    SELECT
#                     d.DISPATCH_ID,
#                     d.CUSTOMER_NUMBER,
#                     d.PHYSICAL_WAREHOUSE,
#                     d.CUSTOMER_NAME,
#                     d.[DATE]AS RECORD_DATE,
#                     uagg.INVOICE_DATE AS INVOICE_DATE,
#                     uagg.QUANTITY,
#                     d.DELIVERY_DATE,
#                     d.SALESMAN_NO,
#                     d.SALESMAN_NAME,
#                     d.UNDEL_ID,
#                     d.INVOICE_NO,
#                     d.ITEM_CODE,
#                     d.ITEM_DETAILS,
#                     d.TRANSPORTER_NAME,
#                     d.DRIVER_NAME,
#                     d.VEHICLE_NO,
#                     d.LOADING_CHARGES,
#                     d.TRANSPORT_CHARGES,
#                     d.MISC_CHARGES,
#                     d.REMARKS,
#                     d.DELIVERYADDRESS,
#                     COUNT(*)                       AS TOTAL_TRUCK_QTY
#                 FROM BUYP.dbo.WHR_TRUCK_SCAN_DETAILS d
#                 LEFT JOIN (
#                     -- one row per UNDEL_ID to avoid duplicating d rows
#                     SELECT UNDEL_ID,
#                         MAX(INVOICE_DATE) AS INVOICE_DATE,
#                         QUANTITY     AS QUANTITY
#                     FROM BUYP.XXALJE_UNDELIVERED_DATA_BUYP1
#                     GROUP BY UNDEL_ID,QUANTITY
#                 ) uagg
#                 ON d.UNDEL_ID = uagg.UNDEL_ID
#                 WHERE d.FLAG != 'R'
#                   AND d.[DATE] >= %s AND d.[DATE] < %s
#             """



#             # Parameters list for query
#             params = [from_date, to_date_plus_one]

#             # Add optional filters
#             if undel_id_param:
#                 query += " AND d.UNDEL_ID = %s"
#                 params.append(undel_id_param)

#             if customer_no_param:
#                 query += " AND d.CUSTOMER_NUMBER = %s"
#                 params.append(customer_no_param)

#             if salesman_name_param:
#                 query += " AND d.SALESMAN_NAME = %s"
#                 params.append(salesman_name_param)

#             # Grouping
#             query += """
#                 GROUP BY
#                     d.DISPATCH_ID,
#                     d.CUSTOMER_NUMBER,
#                     d.PHYSICAL_WAREHOUSE,
#                     d.CUSTOMER_NAME,
#                     d.[DATE],
#                     uagg.INVOICE_DATE,uagg.QUANTITY,
#                     d.DELIVERY_DATE,
#                     d.SALESMAN_NO,
#                     d.SALESMAN_NAME,
#                     d.UNDEL_ID,
#                     d.INVOICE_NO,
#                     d.ITEM_CODE,
#                     d.ITEM_DETAILS,
#                     d.TRANSPORTER_NAME,
#                     d.DRIVER_NAME,
#                     d.VEHICLE_NO,
#                     d.LOADING_CHARGES,
#                     d.TRANSPORT_CHARGES,
#                     d.MISC_CHARGES,
#                     d.REMARKS,
#                     d.DELIVERYADDRESS
#                 ORDER BY d.CUSTOMER_NUMBER, d.UNDEL_ID, d.DELIVERY_DATE;
#             """

#             with connection.cursor() as cursor:
#                 cursor.execute(query, params)
#                 rows = cursor.fetchall()
#                 columns = [col[0] for col in cursor.description]

#             grouped_data = {}

#             for row in rows:
#                 record = dict(zip(columns, row))
#                 cust_no = record['CUSTOMER_NUMBER']
#                 undel_id = record['UNDEL_ID']
#                 item_code = record['ITEM_CODE']
#                 dispatch_id = record['DISPATCH_ID']
#                 truck_qty = float(record['TOTAL_TRUCK_QTY'])

#                 # Fetch item cost
#                 with connection.cursor() as cursor2:
#                     cursor2.execute("""
#                         SELECT QUANTITY, AMOUNT
#                         FROM BUYP.XXALJE_UNDELIVERED_DATA_BUYP1
#                         WHERE UNDEL_ID = %s
#                     """, [undel_id])
#                     result = cursor2.fetchone()

#                 item_cost = 0.0
#                 if result:
#                     qty, amount = result
#                     if qty and qty != 0:
#                         item_cost = float(amount) / float(qty)

#                 if cust_no not in grouped_data:
#                     grouped_data[cust_no] = {
#                         'customer_name': record['CUSTOMER_NAME'],
#                         'customer_number': cust_no,
#                         'region': record['PHYSICAL_WAREHOUSE'],
#                         'delivery_date': record['DELIVERY_DATE'],
#                         'record_date': record['RECORD_DATE'],
#                         'invoice_date': record['INVOICE_DATE'],
#                         'salesman_name': record['SALESMAN_NAME'],
#                         'total_amount': 0.0,
#                         'dispatches': {}
#                     }

#                 dispatch_key = f"{undel_id}_{item_code}"
#                 if dispatch_key not in grouped_data[cust_no]['dispatches']:
#                     grouped_data[cust_no]['dispatches'][dispatch_key] = {
#                         'dispatch_id': dispatch_id,
#                         'customer_name': record['CUSTOMER_NAME'],
#                         'dispatch_date': record['RECORD_DATE'],
#                         'delivery_date': record['DELIVERY_DATE'],
#                         'customer_number': cust_no,
#                         'salesman_no': record['SALESMAN_NO'],
#                         'salesman_name': record['SALESMAN_NAME'],
#                         'undel_id': undel_id,
#                         'region': record['PHYSICAL_WAREHOUSE'],
#                         'invoice_no': record['INVOICE_NO'],
#                         'truck_send_qty': 0.0,
#                         'item_code': item_code,
#                         'invoice_date': record['INVOICE_DATE'],
#                         'quantity': record['QUANTITY'],
#                         'item_details': record['ITEM_DETAILS'],
#                         'transporter_name': record['TRANSPORTER_NAME'],
#                         'driver_name': record['DRIVER_NAME'],
#                         'vehicle_no': record['VEHICLE_NO'],
#                         'loading_charges': record['LOADING_CHARGES'],
#                         'transport_charges': record['TRANSPORT_CHARGES'],
#                         'misc_charges': record['MISC_CHARGES'],
#                         'remarks': record['REMARKS'] if record['REMARKS'] else "",
#                         'deliveryaddress': record['DELIVERYADDRESS'],
#                         'item_cost': round(item_cost, 2),
#                         'total_cost': 0.0,
#                         'status': "Truck Load Dispatch"
#                     }

#                 grouped_data[cust_no]['dispatches'][dispatch_key]['truck_send_qty'] += truck_qty

#             # Calculate totals
#             for cust_no, cust_data in grouped_data.items():
#                 total_amount = 0.0
#                 for dispatch in cust_data['dispatches'].values():
#                     dispatch['total_cost'] = round(dispatch['truck_send_qty'] * dispatch['item_cost'], 2)
#                     total_amount += dispatch['total_cost']
#                 cust_data['total_amount'] = round(total_amount, 2)
#                 cust_data['dispatches'] = list(cust_data['dispatches'].values())

#             return Response(list(grouped_data.values()), status=status.HTTP_200_OK)

#         except Exception as e:
#             return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

class GroupedTruckScanDetailsView(APIView):
    def get(self, request):
        try:
            # Get query parameters
            from_date_str = request.query_params.get('from_date')
            to_date_str = request.query_params.get('to_date')
            undel_id_param = request.query_params.get('undel_id')
            customer_no_param = request.query_params.get('customer_no')
            salesman_name_param = request.query_params.get('salesman_name')

            if not from_date_str or not to_date_str:
                return Response(
                    {"error": "from_date and to_date query parameters are required in YYYY-MM-DD format."},
                    status=status.HTTP_400_BAD_REQUEST
                )

            from_date = parse_date(from_date_str)
            to_date = parse_date(to_date_str)

            if from_date is None or to_date is None:
                return Response(
                    {"error": "Invalid date format. Use YYYY-MM-DD."},
                    status=status.HTTP_400_BAD_REQUEST
                )

            # to_date_plus_one = to_date + timedelta(days=1)

            # Base query
            query = """
                   SELECT
                    d.DISPATCH_ID,
                    d.CUSTOMER_NUMBER,
                    d.PHYSICAL_WAREHOUSE,
                    d.CUSTOMER_NAME,
                    d.[DATE]AS RECORD_DATE,
                    uagg.INVOICE_DATE AS INVOICE_DATE,
                    uagg.QUANTITY,
                    d.DELIVERY_DATE,
                    d.SALESMAN_NO,
                    d.SALESMAN_NAME,
                    d.UNDEL_ID,
                    d.INVOICE_NO,
                    d.ITEM_CODE,
                    d.ITEM_DETAILS,
                    d.TRANSPORTER_NAME,
                    d.DRIVER_NAME,
                    d.VEHICLE_NO,
                    d.LOADING_CHARGES,
                    d.TRANSPORT_CHARGES,
                    d.MISC_CHARGES,
                    d.REMARKS,
                    d.DELIVERYADDRESS,
                    COUNT(*)                       AS TOTAL_TRUCK_QTY
                FROM BUYP.dbo.WHR_TRUCK_SCAN_DETAILS d
                LEFT JOIN (
                    -- one row per UNDEL_ID to avoid duplicating d rows
                    SELECT UNDEL_ID,
                        MAX(INVOICE_DATE) AS INVOICE_DATE,
                        QUANTITY     AS QUANTITY
                    FROM BUYP.XXALJE_UNDELIVERED_DATA_BUYP1
                    GROUP BY UNDEL_ID,QUANTITY
                ) uagg
                ON d.UNDEL_ID = uagg.UNDEL_ID

                WHERE d.FLAG != 'R'
        AND CAST(d.[DATE] AS DATE) BETWEEN %s AND %s
            """

            # Parameters list for query
            params = [from_date, to_date]

            # Add optional filters
            if undel_id_param:
                query += " AND d.UNDEL_ID = %s"
                params.append(undel_id_param)

            if customer_no_param:
                query += " AND d.CUSTOMER_NUMBER = %s"
                params.append(customer_no_param)

            if salesman_name_param:
                query += " AND d.SALESMAN_NAME = %s"
                params.append(salesman_name_param)

            # Grouping
            query += """
                GROUP BY
                    d.DISPATCH_ID,
                    d.CUSTOMER_NUMBER,
                    d.PHYSICAL_WAREHOUSE,
                    d.CUSTOMER_NAME,
                    d.[DATE],
                    uagg.INVOICE_DATE,uagg.QUANTITY,
                    d.DELIVERY_DATE,
                    d.SALESMAN_NO,
                    d.SALESMAN_NAME,
                    d.UNDEL_ID,
                    d.INVOICE_NO,
                    d.ITEM_CODE,
                    d.ITEM_DETAILS,
                    d.TRANSPORTER_NAME,
                    d.DRIVER_NAME,
                    d.VEHICLE_NO,
                    d.LOADING_CHARGES,
                    d.TRANSPORT_CHARGES,
                    d.MISC_CHARGES,
                    d.REMARKS,
                    d.DELIVERYADDRESS
                ORDER BY d.CUSTOMER_NUMBER, d.UNDEL_ID, d.DELIVERY_DATE;
            """

            with connection.cursor() as cursor:
                cursor.execute(query, params)
                rows = cursor.fetchall()
                columns = [col[0] for col in cursor.description]

            grouped_data = {}

            for row in rows:
                record = dict(zip(columns, row))
                
                dispatch_id = record['DISPATCH_ID']
                cust_no = record['CUSTOMER_NUMBER']
                undel_id = record['UNDEL_ID']
                invoice_no = record['INVOICE_NO']
                item_code = record['ITEM_CODE']
                truck_qty = (record['TOTAL_TRUCK_QTY'])

                # Fetch item cost
                with connection.cursor() as cursor2:
                    cursor2.execute("""
                        SELECT QUANTITY, AMOUNT
                        FROM BUYP.XXALJE_UNDELIVERED_DATA_BUYP1
                        WHERE UNDEL_ID = %s
                    """, [undel_id])
                    result = cursor2.fetchone()

                item_cost = 0.0
                if result:
                    qty, amount = result
                    if qty and qty != 0:
                        item_cost = float(amount) / float(qty)

                if cust_no not in grouped_data:
                    grouped_data[cust_no] = {
                        'customer_name': record['CUSTOMER_NAME'],
                        'customer_number': cust_no,
                        'region': record['PHYSICAL_WAREHOUSE'],
                        'delivery_date': record['DELIVERY_DATE'],
                        'record_date': record['RECORD_DATE'],
                        'invoice_date': record['INVOICE_DATE'],
                        'salesman_name': record['SALESMAN_NAME'],
                        'total_amount': 0.0,
                        'total_vat': 0.0,
                        'total_net_amount': 0.0,
                        'total_quantity': 0.0,   # ✅ added field
                        'dispatches': {}
                    }

                dispatch_key = f"{dispatch_id}_{undel_id}_{invoice_no}_{item_code}"
                if dispatch_key not in grouped_data[cust_no]['dispatches']:
                    grouped_data[cust_no]['dispatches'][dispatch_key] = {
                        'dispatch_id': dispatch_id,
                        'customer_name': record['CUSTOMER_NAME'],
                        'dispatch_date': record['RECORD_DATE'],
                        'delivery_date': record['DELIVERY_DATE'],
                        'customer_number': cust_no,
                        'salesman_no': record['SALESMAN_NO'],
                        'salesman_name': record['SALESMAN_NAME'],
                        'undel_id': undel_id,
                        'region': record['PHYSICAL_WAREHOUSE'],
                        'invoice_no': record['INVOICE_NO'],
                        'item_code': item_code,
                        'invoice_date': record['INVOICE_DATE'],
                        'quantity': record['QUANTITY'],
                        'item_details': record['ITEM_DETAILS'],
                        'transporter_name': record['TRANSPORTER_NAME'],
                        'truck_send_qty': truck_qty,

                        'driver_name': record['DRIVER_NAME'],
                        'vehicle_no': record['VEHICLE_NO'],
                        'loading_charges': record['LOADING_CHARGES'],
                        'transport_charges': record['TRANSPORT_CHARGES'],
                        'misc_charges': record['MISC_CHARGES'],
                        'remarks': record['REMARKS'] if record['REMARKS'] else "",
                        'deliveryaddress': record['DELIVERYADDRESS'],
                        'item_cost': round(item_cost, 2),
                        'total_cost': 0.0,
                        'vat_amount': 0.0,
                        'net_amount': 0.0,
                        'status': "Truck Load Dispatch"
                    }

            # Calculate totals
            for cust_no, cust_data in grouped_data.items():
                total_amount = 0.0
                total_vat = 0.0
                total_net = 0.0
                total_qty = 0.0
                for dispatch in cust_data['dispatches'].values():
                    dispatch['total_cost'] = round(dispatch['truck_send_qty'] * dispatch['item_cost'], 2)
                    dispatch['vat_amount'] = round(dispatch['total_cost'] * 0.15, 2)   # ✅ 15% VAT
                    dispatch['net_amount'] = round(dispatch['total_cost'] + dispatch['vat_amount'], 2)  # ✅ Net

                    total_amount += dispatch['total_cost']
                    total_vat += dispatch['vat_amount']
                    total_net += dispatch['net_amount']
                    total_qty += dispatch['truck_send_qty']   # ✅ add qty

                cust_data['total_amount'] = round(total_amount, 2)
                cust_data['total_vat'] = round(total_vat, 2)
                cust_data['total_net_amount'] = round(total_net, 2)
                cust_data['total_quantity'] = round(total_qty, 2)   # ✅ added total qty
                cust_data['dispatches'] = list(cust_data['dispatches'].values())

            return Response(list(grouped_data.values()), status=status.HTTP_200_OK)

        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

class GetSalesmanNameView(APIView):
    def get(self, request):
        try:
            # Get salesman_no from query params
            salesman_no = request.query_params.get("salesman_no")

            if not salesman_no:
                return Response(
                    {"error": "salesman_no query parameter is required."},
                    status=status.HTTP_400_BAD_REQUEST
                )

            query = """
                SELECT TOP 1 SALESMAN_NAME
                FROM [BUYP].[BUYP].[XXALJE_UNDELIVERED_DATA_BUYP1]
                WHERE SALESMAN_NO = %s
            """

            with connection.cursor() as cursor:
                cursor.execute(query, [salesman_no])
                row = cursor.fetchone()

            if row:
                return Response(
                    {"salesman_no": salesman_no, "salesman_name": row[0]},
                    status=status.HTTP_200_OK
                )
            else:
                return Response(
                    {"error": f"No salesman found with SALESMAN_NO = {salesman_no}"},
                    status=status.HTTP_404_NOT_FOUND
                )

        except Exception as e:
            return Response(
                {"error": str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )



class UndeliveredDataViewSet(ViewSet):
    def list(self, request):
        try:
            # ✅ Get filter params
            salesmanno_param = request.query_params.get("salesmanno")
            customerno_param = request.query_params.get("customerno")
            undelid_param = request.query_params.get("undelid")
            invoiceno_param = request.query_params.get("invoiceno")
            status_param = request.query_params.get("status", "overall").lower()

            # ✅ Build WHERE clauses (parameterized for speed & security)
            filters = []
            params = []

            if salesmanno_param:
                salesmannos = [s.strip() for s in salesmanno_param.split(",") if s.strip().isdigit()]
                if salesmannos:
                    placeholders = ",".join(["%s"] * len(salesmannos))
                    filters.append(f"ud.SALESMAN_NO IN ({placeholders})")
                    params.extend(salesmannos)

            if customerno_param:
                customernos = [c.strip() for c in customerno_param.split(",") if c.strip().isdigit()]
                if customernos:
                    placeholders = ",".join(["%s"] * len(customernos))
                    filters.append(f"ud.CUSTOMER_NUMBER IN ({placeholders})")
                    params.extend(customernos)

            if undelid_param:
                undelids = [u.strip() for u in undelid_param.split(",") if u.strip().isdigit()]
                if undelids:
                    placeholders = ",".join(["%s"] * len(undelids))
                    filters.append(f"ud.UNDEL_ID IN ({placeholders})")
                    params.extend(undelids)

            if invoiceno_param:
                invoicenos = [inv.strip() for inv in invoiceno_param.split(",") if inv.strip()]
                if invoicenos:
                    placeholders = ",".join(["%s"] * len(invoicenos))
                    filters.append(f"ud.INVOICE_NUMBER IN ({placeholders})")
                    params.extend(invoicenos)

            # ✅ Status filter
            if status_param == "pending":
                filters.append("ud.QUANTITY <> ISNULL(ud.DISPATCH_QTY,0)")
            elif status_param == "completed":
                filters.append("ud.QUANTITY = ud.DISPATCH_QTY")

            where_clause = "WHERE " + " AND ".join(filters) if filters else ""

            # ✅ Optimized query (all calculations inside SQL)
            query = f"""
                ;WITH ItemData AS (
                    SELECT 
                        ic.INVENTORY_ITEM_ID,
                        ic.ITEM_CODE,
                        ic.DESCRIPTION,
                        ic.FRANCHISE,
                        ic.FAMILY,
                        ic.CLASS,
                        ic.SUBCLASS,
                        ROW_NUMBER() OVER (PARTITION BY ic.INVENTORY_ITEM_ID ORDER BY ic.ITEM_CODE) AS rn
                    FROM [BUYP].[BUYP].[ALJE_ITEM_CATEGORIES_CPD_V] ic
                ),
                TruckQty AS (
                    SELECT 
                        UNDEL_ID,
                        SUM(ISNULL(TRUCK_SEND_QTY, 0)) AS TRUCK_SEND_QTY
                    FROM WHR_TRUCK_SCAN_DETAILS
                    GROUP BY UNDEL_ID
                )
                SELECT
                    ud.ORG_NAME AS Region,
                    ud.TO_WAREHOUSE AS Warehouse,
                    ISNULL(ud.CUS_LOCATION, 0) AS Location,
                    ud.SALES_CHANNEL AS [Sales Channel],
                    ISNULL(ud.CUSTOMER_NUMBER, 0) AS [Customer Number],
                    ud.CUSTOMER_NAME AS [Customer Name],
                    ISNULL(ud.SALESMAN_NO, 0) AS [Salesman No],
                    ud.SALESMAN_NAME AS [Salesman Name],
                    ud.INVOICE_NUMBER AS [Invoice No],
                    ISNULL(ud.ORG_ID, 0) AS [Organization Code],
                    FORMAT(ud.INVOICE_DATE, 'dd-MMM-yyyy') AS [Invoice Date],
                    ISNULL(ud.LINE_NUMBER, 0) AS [Line No],
                    id.ITEM_CODE,
                    id.DESCRIPTION,
                    id.FRANCHISE,
                    id.FAMILY,
                    id.CLASS,
                    id.SUBCLASS,
                    ud.QUANTITY AS [Invoice Qty],
                    ISNULL(ud.DISPATCH_QTY,0) AS [Dispatch Qty],
                    (ud.QUANTITY - ISNULL(ud.DISPATCH_QTY,0)) AS [Balance Qty],					
                    
                    
                    ISNULL(tq.TRUCK_SEND_QTY, 0) AS [WMS Dispatch Qty],

                    (ISNULL(ud.DISPATCH_QTY, 0) - (ISNULL(tq.TRUCK_SEND_QTY, 0) + ISNULL(ud.RETURN_QUANTITY, 0))) AS [Oracle dispoatch Qty],
                    CASE 
                            WHEN ud.RETURN_FLAG = 'R' THEN 'Y'
                            WHEN ud.RETURN_FLAG IS NULL OR LTRIM(RTRIM(ud.RETURN_FLAG)) = '' THEN 'N'
                            ELSE 'N'
                        END AS [Return Flag],   
                    ISNULL(ud.RETURN_QUANTITY, 0) AS [Return Quantity],
                    CASE WHEN ud.QUANTITY = 0 THEN 0 ELSE ud.AMOUNT/ud.QUANTITY END AS One_Item_Amount,
                    CASE WHEN ud.QUANTITY = 0 THEN 0 ELSE (ud.AMOUNT/ud.QUANTITY)*(ud.QUANTITY - ISNULL(ud.DISPATCH_QTY,0)) END AS Value,
                    DATEDIFF(DAY, ud.INVOICE_DATE, GETDATE()) AS Days,
                    CASE WHEN DATEDIFF(DAY, ud.INVOICE_DATE, GETDATE()) BETWEEN 0 AND 30 THEN (ud.AMOUNT/ud.QUANTITY)*(ud.QUANTITY - ISNULL(ud.DISPATCH_QTY,0)) ELSE 0 END AS [0-30_Days],
                    CASE WHEN DATEDIFF(DAY, ud.INVOICE_DATE, GETDATE()) BETWEEN 31 AND 60 THEN (ud.AMOUNT/ud.QUANTITY)*(ud.QUANTITY - ISNULL(ud.DISPATCH_QTY,0)) ELSE 0 END AS [31-60_Days],
                    CASE WHEN DATEDIFF(DAY, ud.INVOICE_DATE, GETDATE()) BETWEEN 61 AND 90 THEN (ud.AMOUNT/ud.QUANTITY)*(ud.QUANTITY - ISNULL(ud.DISPATCH_QTY,0)) ELSE 0 END AS [61-90_Days],
                    CASE WHEN DATEDIFF(DAY, ud.INVOICE_DATE, GETDATE()) BETWEEN 91 AND 120 THEN (ud.AMOUNT/ud.QUANTITY)*(ud.QUANTITY - ISNULL(ud.DISPATCH_QTY,0)) ELSE 0 END AS [91-120_Days],
                    CASE WHEN DATEDIFF(DAY, ud.INVOICE_DATE, GETDATE()) BETWEEN 121 AND 180 THEN (ud.AMOUNT/ud.QUANTITY)*(ud.QUANTITY - ISNULL(ud.DISPATCH_QTY,0)) ELSE 0 END AS [121-180_Days],
                    CASE WHEN DATEDIFF(DAY, ud.INVOICE_DATE, GETDATE()) BETWEEN 181 AND 360 THEN (ud.AMOUNT/ud.QUANTITY)*(ud.QUANTITY - ISNULL(ud.DISPATCH_QTY,0)) ELSE 0 END AS [181-360_Days],
                    CASE WHEN DATEDIFF(DAY, ud.INVOICE_DATE, GETDATE()) > 360 THEN (ud.AMOUNT/ud.QUANTITY)*(ud.QUANTITY - ISNULL(ud.DISPATCH_QTY,0)) ELSE 0 END AS [360+_Days]
                FROM [BUYP].[BUYP].[XXALJE_UNDELIVERED_DATA_BUYP1] ud
                LEFT JOIN ItemData id 
                    ON ud.INVENTORY_ITEM_ID = id.INVENTORY_ITEM_ID AND id.rn = 1
           LEFT JOIN TruckQty tq
    ON ud.UNDEL_ID = tq.UNDEL_ID 
                {where_clause}
            """

            # ✅ Execute with params (safe & fast)
            with connection.cursor() as cursor:
                cursor.execute(query, params)
                columns = [col[0] for col in cursor.description]
                rows = cursor.fetchall()

            # Convert to list of dicts
            results = [dict(zip(columns, row)) for row in rows]

            # ✅ Pagination (100,000/page)
            paginator = PageNumberPagination()
            paginator.page_size = 100000
            paginated_data = paginator.paginate_queryset(results, request)
            return paginator.get_paginated_response(paginated_data)

        except Exception as e:
            return Response({"error": str(e)}, status=500)


# class InterORGReportView(ViewSet):
#     def list(self, request):
#         try:
#             shipmentnum_param = request.query_params.get("shipmentnum", None)
#             shipmentlineid_param = request.query_params.get("shipmentlineid", None)
#             from_date = request.query_params.get("from_date", None)
#             to_date = request.query_params.get("to_date", None)

#             with connection.cursor() as cursor:
#                 base_query = """
#                     SELECT 
#                         SHIPMENT_HEADER_ID,
#                         SHIPMENT_LINE_ID,
#                         LINE_NUM,
#                         SH_CREATION_DATE,
#                         SH_CREATED_BY,
#                         FROM_ORGN_ID,
#                         FROM_ORGN_CODE,
#                         FROM_ORGN_NAME,
#                         SHIPMENT_NUM,
#                         RECEIPT_NUM,
#                         SHIPPED_DATE,
#                         TO_ORGN_ID,
#                         TO_ORGN_CODE,
#                         TO_ORGN_NAME,
#                         SYS_QUANTITY_SHIPPED,
#                         SYS_QUANTITY_RECEIVED,
#                         UNIT_OF_MEASURE,
#                         ITEM_ID,
#                         ITEM_CODE,
#                         DESCRIPTION,
#                         FRANCHISE,
#                         FAMILY,
#                         CLASS,
#                         SUBCLASS,
#                         SYS_SHIPMENT_LINE_STATUS,
#                         CREATED_BY,
#                         CREATED_DATE,
#                         LAST_UPDATE_DATE,
#                         LAST_UPDATED_BY,
#                         PHY_QUANTITY_SHIPPED,
#                         PHY_QUANTITY_RECEIVED,
#                         PHY_SHIPMENT_LINE_STATUS,
#                         CANCEL_LINE_FLAG,
#                         ATTRIBUTE1,
#                         ATTRIBUTE2,
#                         ATTRIBUTE3,
#                         ATTRIBUTE4,
#                         ATTRIBUTE5,
#                         ATTRIBUTE6
#                     FROM BUYP.dbo.XXALJEBYP_INTERORG_TBL
#                     WHERE SYS_QUANTITY_SHIPPED <> ISNULL(PHY_QUANTITY_SHIPPED, 0)
#                 """
#                 params = []

#                 if shipmentnum_param:
#                     base_query += " AND SHIPMENT_NUM = %s"
#                     params.append(shipmentnum_param)

#                 if shipmentlineid_param:
#                     base_query += " AND SHIPMENT_LINE_ID = %s"
#                     params.append(shipmentlineid_param)

#                 if from_date:
#                     base_query += " AND CAST(SHIPPED_DATE AS DATE) >= %s"
#                     params.append(from_date)

#                 if to_date:
#                     base_query += " AND CAST(SHIPPED_DATE AS DATE) <= %s"
#                     params.append(to_date)

#                 cursor.execute(base_query, params)
#                 columns = [col[0] for col in cursor.description]
#                 rows = cursor.fetchall()

#             results = []
#             for row in rows:
#                 row_data = dict(zip(columns, row))

#                 sys_qty = row_data.get("SYS_QUANTITY_SHIPPED") or 0
#                 phy_qty = row_data.get("PHY_QUANTITY_SHIPPED") or 0
#                 diff = sys_qty - phy_qty
#                 balance_qty = int(diff) if float(diff).is_integer() else round(diff, 1)
#                     # Format SHIPPED_DATE as "08 Aug 2025"
#                 shipped_date_raw = row_data.get("SHIPPED_DATE")
#                 shipped_date_formatted = ''
#                 if isinstance(shipped_date_raw, (datetime, )):
#                     shipped_date_formatted = shipped_date_raw.strftime('%d %b %Y')

#                 result_row = {
#                     "FROM_ORGN_CODE": row_data.get("FROM_ORGN_CODE"),
#                     "FROM_ORGN_NAME": row_data.get("FROM_ORGN_NAME"),
#                     "TO_ORGN_CODE": row_data.get("TO_ORGN_CODE"),
#                     "TO_ORGN_NAME": row_data.get("TO_ORGN_NAME"),
#                     "SHIPMENT_NUM": row_data.get("SHIPMENT_NUM"),
#                     "SHIPPED_DATE": shipped_date_formatted,
#                     "LINE_NUM": row_data.get("LINE_NUM"),
#                     "ITEM_CODE": row_data.get("ITEM_CODE"),
#                     "DESCRIPTION": row_data.get("DESCRIPTION"),
#                     "TRANSFER QUANTITY": row_data.get("SYS_QUANTITY_SHIPPED"),
#                     "PHY SHIPPED QUANTITY": row_data.get("PHY_QUANTITY_SHIPPED") or 0,
#                     "BALANCE_QUANTITY_SHIPPED": balance_qty,
#                 }
#                 results.append(result_row)

#             paginator = TruckResultsPagination()
#             # paginator.page_size = 500
#             paginated_data = paginator.paginate_queryset(results, request)
#             return paginator.get_paginated_response(paginated_data)

#         except Exception as e:
#             return Response({"error": str(e)}, status=500)

class InterORGReportView(ViewSet):
    def list(self, request):
        try:
            shipmentnum_param = request.query_params.get("shipmentnum", None)
            shipmentlineid_param = request.query_params.get("shipmentlineid", None)
            from_date = request.query_params.get("from_date", None)
            to_date = request.query_params.get("to_date", None)
 
            page_number = int(request.query_params.get("page", 1))
            page_size = int(request.query_params.get("page_size", 1000000))
 
            with connection.cursor() as cursor:
                base_query = """
                    SELECT
                        FROM_ORGN_CODE,
                        FROM_ORGN_NAME,
                        TO_ORGN_CODE,
                        TO_ORGN_NAME,
                        SHIPMENT_NUM,
                        CONVERT(VARCHAR(11), SHIPPED_DATE, 106) AS SHIPPED_DATE,
                        LINE_NUM,
                        ITEM_CODE,
                        DESCRIPTION,
                        SYS_QUANTITY_SHIPPED,
                        ISNULL(PHY_QUANTITY_SHIPPED, 0) AS PHY_QUANTITY_SHIPPED,
                        SYS_QUANTITY_SHIPPED - ISNULL(PHY_QUANTITY_SHIPPED, 0) AS BALANCE_QUANTITY_SHIPPED
                    FROM BUYP.dbo.XXALJEBYP_INTERORG_TBL
                    WHERE SYS_QUANTITY_SHIPPED <> ISNULL(PHY_QUANTITY_SHIPPED, 0)
                """
                params = []
 
                if shipmentnum_param:
                    base_query += " AND SHIPMENT_NUM = %s"
                    params.append(shipmentnum_param)
 
                if shipmentlineid_param:
                    base_query += " AND SHIPMENT_LINE_ID = %s"
                    params.append(shipmentlineid_param)
 
                if from_date:
                    base_query += " AND CAST(SHIPPED_DATE AS DATE) >= %s"
                    params.append(from_date)
 
                if to_date:
                    base_query += " AND CAST(SHIPPED_DATE AS DATE) <= %s"
                    params.append(to_date)
 
                # Apply SQL pagination
                offset = (page_number - 1) * page_size
                base_query += f" ORDER BY SHIPPED_DATE OFFSET {offset} ROWS FETCH NEXT {page_size} ROWS ONLY"
 
                cursor.execute(base_query, params)
                columns = [col[0] for col in cursor.description]
                rows = cursor.fetchall()
 
            # Convert to dict directly
            results = [dict(zip(columns, row)) for row in rows]
 
            return Response({
                "page": page_number,
                "page_size": page_size,
                "results": results
            })
 
        except Exception as e:
            return Response({"error": str(e)}, status=500)



class InterORGReportCompletedView(ViewSet):
    def list(self, request):
        try:
            shipmentnum_param = request.query_params.get("shipmentnum", None)
            shipmentlineid_param = request.query_params.get("shipmentlineid", None)
 
            with connection.cursor() as cursor:
                # ✅ Only fetch required columns (smaller data transfer, faster)
                base_query = """
                    SELECT
                        FROM_ORGN_CODE,
                        FROM_ORGN_NAME,
                        TO_ORGN_CODE,
                        TO_ORGN_NAME,
                        SHIPMENT_NUM,
                        LINE_NUM,
                        ITEM_CODE,
                        DESCRIPTION,
                        SYS_QUANTITY_SHIPPED,
                        PHY_QUANTITY_SHIPPED
                    FROM BUYP.dbo.XXALJEBYP_INTERORG_TBL
                    WHERE SYS_QUANTITY_SHIPPED = PHY_QUANTITY_SHIPPED
                """
 
                params = []
                if shipmentnum_param:
                    base_query += " AND SHIPMENT_NUM = %s"
                    params.append(shipmentnum_param)
                if shipmentlineid_param:
                    base_query += " AND SHIPMENT_LINE_ID = %s"
                    params.append(shipmentlineid_param)
 
                # ✅ Use efficient fetchall
                cursor.execute(base_query, params)
                columns = [col[0] for col in cursor.description]
                rows = cursor.fetchall()
 
            # ✅ Map results (lighter response dict)
            results = []
            for row in rows:
                row_data = dict(zip(columns, row))
                results.append({
                    "FROM_ORGN_CODE": row_data.get("FROM_ORGN_CODE"),
                    "FROM_ORGN_NAME": row_data.get("FROM_ORGN_NAME"),
                    "TO_ORGN_CODE": row_data.get("TO_ORGN_CODE"),
                    "TO_ORGN_NAME": row_data.get("TO_ORGN_NAME"),
                    "SHIPMENT_NUM": row_data.get("SHIPMENT_NUM"),
                    "LINE_NUM": row_data.get("LINE_NUM"),
                    "ITEM_CODE": row_data.get("ITEM_CODE"),
                    "DESCRIPTION": row_data.get("DESCRIPTION"),
                    "TRANSFER QUANTITY": row_data.get("SYS_QUANTITY_SHIPPED"),
                    "PHY SHIPPED QUANTITY": row_data.get("PHY_QUANTITY_SHIPPED"),
                })
 
            # ✅ Paginate (never dump thousands of rows at once)
            paginator = PageNumberPagination()
            paginator.page_size = 10000
            paginated_data = paginator.paginate_queryset(results, request)
            return paginator.get_paginated_response(paginated_data)
 
        except Exception as e:
            return Response({"error": str(e)}, status=500)

# class InterORG_Shipment_transferdView(ViewSet):
#     def list(self, request):
#         try:
#             shipmentnum_param = request.query_params.get("shipmentnum", None)
#             shipmentlineid_param = request.query_params.get("shipmentlineid", None)
#             from_date_param = request.query_params.get("from_date", None)
#             to_date_param = request.query_params.get("to_date", None)

#             with connection.cursor() as cursor:
#                 base_query = """
#                         WITH DispatchCalc AS (
#                             SELECT 
#                                 D.ID,
#                                 D.SHIPMENT_ID,
#                                 D.WAREHOUSE_NAME,
#                                 D.TO_WAREHOUSE_NAME,
#                                 D.SALESMANNO,
#                                 D.SALESMANAME,
#                                 D.DATE,
#                                 D.TRANSPORTER_NAME,
#                                 D.DRIVER_NAME,
#                                 D.DRIVER_MOBILENO,
#                                 D.VEHICLE_NO,
#                                 D.TRUCK_DIMENSION,
#                                 D.LOADING_CHARGES,
#                                 D.TRANSPORT_CHARGES,
#                                 D.MISC_CHARGES,
#                                 D.DELIVERYADDRESS,
#                                 D.SHIPMENT_HEADER_ID,
#                                 D.SHIPMENT_LINE_ID,
#                                 D.LINE_NUM,
#                                 D.CREATION_DATE,
#                                 D.CREATED_BY,
#                                 D.ORGANIZATION_ID,
#                                 D.ORGANIZATION_CODE,
#                                 D.ORGANIZATION_NAME,
#                                 D.SHIPMENT_NUM,
#                                 D.SHIPPED_DATE,
#                                 D.RECEIPT_NUM,    
#                                 D.TO_ORGN_CODE,
#                                 D.TO_ORGN_NAME,
#                                 I.INVENTORY_ITEM_ID,
#                                 D.ITEM_ID,      
#                                 I.DESCRIPTION AS ITEM_DESCRIPTION,
#                                 D.DESCRIPTION AS DISPATCH_DESCRIPTION, 
#                                 D.QUANTITY_SHIPPED,
#                                 D.QUANTITY_PROGRESS,
#                                 D.ACTIVE_STATUS,
#                                 D.REMARKS,

#                                 -- ✅ Existing received qty (before this row)
#                                 ISNULL(
#                                     SUM(D.QUANTITY_PROGRESS) OVER (
#                                         PARTITION BY D.SHIPMENT_LINE_ID
#                                         ORDER BY D.ID
#                                         ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
#                                     ),0
#                                 ) AS EXISTING_RECEIVED_QTY,

#                                 -- ✅ Cumulative received qty up to this row
#                                 SUM(D.QUANTITY_PROGRESS) OVER (
#                                     PARTITION BY D.SHIPMENT_LINE_ID
#                                     ORDER BY D.ID
#                                     ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
#                                 ) AS CUMULATIVE_RECEIVED_QTY,

#                                 -- ✅ Remain Balance = Shipped - Cumulative Received
#                                 D.QUANTITY_SHIPPED 
#                                 - SUM(D.QUANTITY_PROGRESS) OVER (
#                                         PARTITION BY D.SHIPMENT_LINE_ID
#                                         ORDER BY D.ID
#                                         ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
#                                     ) AS REMAIN_BALANCE
#                             FROM BUYP.dbo.WHR_SHIMENT_DISPATCH D
#                             LEFT JOIN BUYP.BUYP.ALJE_ITEM_CATEGORIES_CPD_V I
#                                 ON D.ITEM_ID = I.ITEM_CODE
#                         )
#                         SELECT *
#                         FROM DispatchCalc
                        
#                 """
           

#                 conditions = []
#                 params = []

#                 if shipmentnum_param:
#                     conditions.append("SHIPMENT_NUM = %s")
#                     params.append(shipmentnum_param)

#                 if shipmentlineid_param:
#                     conditions.append("SHIPMENT_LINE_ID = %s")
#                     params.append(shipmentlineid_param)

#                 if from_date_param and to_date_param:
#                     try:
#                         from_date = datetime.strptime(from_date_param, "%Y-%m-%d").date()
#                         to_date = datetime.strptime(to_date_param, "%Y-%m-%d").date()
#                         conditions.append("CAST(DATE AS DATE) BETWEEN %s AND %s")
#                         params.append(from_date)
#                         params.append(to_date)
#                     except ValueError:
#                         return Response(
#                             {"error": "Invalid date format. Use YYYY-MM-DD."}, status=400
#                         )

#                 if conditions:
#                     base_query += " WHERE " + " AND ".join(conditions)

#                 base_query += " ORDER BY SHIPMENT_LINE_ID, ID;"

#                 cursor.execute(base_query, params)
#                 columns = [col[0] for col in cursor.description]
#                 rows = cursor.fetchall()

#             grouped_rows = defaultdict(list)
#             for row in rows:
#                 row_dict = dict(zip(columns, row))
#                 grouped_rows[row_dict["SHIPMENT_LINE_ID"]].append(row_dict)

#             results = []
#             for shipment_line_id, group in grouped_rows.items():
#                 if not group:
#                     continue

#                 first_row = group[0]
#                 quantity_received = first_row.get("QUANTITY_RECEIVED") or 0

#                 # Add Opening Balance only if QUANTITY_RECEIVED > 0
#                 # if quantity_received > 0:
#                     # opening_balance_row = {
#                     #     "DISPATCH NUM": "Opening Balance",
#                     #     "DISPATCH DATE": (
#                     #         first_row.get("DATE").strftime("%d-%b-%Y")
#                     #         if first_row.get("DATE")
#                     #         else datetime.now().strftime("%d-%b-%Y")
#                     #     ),
#                     #     "FROM_ORG_CODE": first_row.get("ORGANIZATION_CODE"),
#                     #     "FROM_ORG_NAME": first_row.get("ORGANIZATION_NAME"),
#                     #     "TO_ORGN_CODE": first_row.get("TO_ORGN_CODE"),
#                     #     "TO_ORGN_NAME": first_row.get("TO_ORGN_NAME"),
#                     #     "NOTES": "Oracle Dispatch",
#                     #     "SHIPMENT_NUM": first_row.get("SHIPMENT_NUM"),
#                     #     "SHIPPED_DATE": (
#                     #         first_row.get("SHIPPED_DATE").strftime("%d-%b-%Y")
#                     #         if first_row.get("SHIPPED_DATE")
#                     #         else datetime.now().strftime("%d-%b-%Y")
#                     #     ),
#                     #     "INVENTORY_ITEM_ID": first_row.get("INVENTORY_ITEM_ID"),
#                     #     "ITEM CODE": first_row.get("ITEM_ID"),
#                     #     "DESCRIPTION": first_row.get("DISPATCH_DESCRIPTION"),
#                     #     "QTY DISPATCH": first_row.get("QUANTITY_SHIPPED") or 0,
#                     #     "TOTAL QTY": quantity_received,
#                     # }
#                     # results.append(opening_balance_row)

#                 for row_data in group:
#                     result_row = {
#                         "DISPATCH NUM": row_data.get("SHIPMENT_ID"),
#                         "DISPATCH DATE": (
#                             row_data.get("DATE").strftime("%d-%b-%Y")
#                             if row_data.get("DATE")
#                             else datetime.now().strftime("%d-%b-%Y")
#                         ),
#                         "FROM_ORG_CODE": row_data.get("ORGANIZATION_CODE"),
#                         "FROM_ORG_NAME": row_data.get("ORGANIZATION_NAME"),
#                         "TO_ORGN_CODE": row_data.get("TO_ORGN_CODE"),
#                         "TO_ORGN_NAME": row_data.get("TO_ORGN_NAME"),
#                         "NOTES/REMARKS":row_data.get("REMARKS"),
#                         "SHIPMENT_NUM": row_data.get("SHIPMENT_NUM"),
#                         # "SHIPMENT_LINE_ID": row_data.get("SHIPMENT_LINE_ID"),
#                         "SHIPPED_DATE": (
#                             row_data.get("SHIPPED_DATE").strftime("%d-%b-%Y")
#                             if row_data.get("SHIPPED_DATE")
#                             else datetime.now().strftime("%d-%b-%Y")
#                         ),
#                         "INVENTORY_ITEM_ID": row_data.get("INVENTORY_ITEM_ID"),
#                         "ITEM CODE": row_data.get("ITEM_ID"),
#                         "DESCRIPTION": row_data.get("DISPATCH_DESCRIPTION"),
#                         "TOTAL QUANTITY": row_data.get("QUANTITY_SHIPPED") or 0,
#                         "DISPATCH QTY": row_data.get("QUANTITY_PROGRESS") or 0,
#                         "EXISTING RECEIVED QTY": row_data.get("EXISTING_RECEIVED_QTY") or 0,
#                         "REMAINING QTY": row_data.get("REMAIN_BALANCE") or 0,
#                     }
#                     results.append(result_row)

#             paginator = TruckResultsPagination()
#             # paginator.page_size = 500
#             paginated_data = paginator.paginate_queryset(results, request)
#             return paginator.get_paginated_response(paginated_data)

#         except Exception as e:
#             return Response({"error": str(e)}, status=500)

from datetime import datetime
from rest_framework.viewsets import ViewSet
from rest_framework.response import Response
from django.db import connection


from datetime import datetime
from rest_framework.response import Response
from rest_framework.viewsets import ViewSet
from django.db import connection
from datetime import datetime
from rest_framework.response import Response
from rest_framework.viewsets import ViewSet
from django.db import connection

class InterORG_Shipment_transferdView(ViewSet):

    def list(self, request):
        try:
            shipmentnum = request.query_params.get("shipmentnum")
            shipmentlineid = request.query_params.get("shipmentlineid")
            from_date_param = request.query_params.get("from_date")
            to_date_param = request.query_params.get("to_date")

            last_id = request.query_params.get("last_id")
            page_size = int(request.query_params.get("page_size", 100000))
            page_size = min(max(page_size, 1), 500000)

            include_count = request.query_params.get("include_count", "false").lower() == "true"

            where_clauses = ["1=1"]
            params = []

            if shipmentnum:
                where_clauses.append("D.SHIPMENT_NUM = %s")
                params.append(shipmentnum)

            if shipmentlineid:
                where_clauses.append("D.SHIPMENT_LINE_ID = %s")
                params.append(shipmentlineid)

            if from_date_param and to_date_param:
                from_date = datetime.strptime(from_date_param, "%Y-%m-%d").date()
                to_date = datetime.strptime(to_date_param, "%Y-%m-%d").date()
                where_clauses.append("CAST(D.DATE AS DATE) BETWEEN %s AND %s")
                params.extend([from_date, to_date])

            where_sql = " AND ".join(where_clauses)

            # ✅ Correct SQL
            sql = f"""
                WITH CTE AS (
                    SELECT
                        D.ID,
                        D.SHIPMENT_ID,
                        D.ORGANIZATION_CODE,
                        D.ORGANIZATION_NAME,
                        D.TO_ORGN_CODE,
                        D.TO_ORGN_NAME,
                        D.REMARKS,
                        D.SHIPMENT_NUM,
                        D.SHIPPED_DATE,
                        I.INVENTORY_ITEM_ID,
                        D.ITEM_ID,
                        D.DESCRIPTION AS DISPATCH_DESCRIPTION,
                        D.QUANTITY_SHIPPED,
                        D.QUANTITY_PROGRESS,

                        ISNULL(
                            FIRST_VALUE(D.QUANTITY_RECEIVED) OVER (
                                PARTITION BY D.SHIPMENT_LINE_ID
                                ORDER BY D.ID
                            ), 0
                        ) AS EXISTING_FIRST_RECEIVED,

                        ISNULL(
                            SUM(D.QUANTITY_PROGRESS) OVER (
                                PARTITION BY D.SHIPMENT_LINE_ID
                                ORDER BY D.ID
                                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                            ), 0
                        ) AS SUM_PROGRESS_BEFORE,

                        D.DATE AS DISPATCH_DATE,

                        ROW_NUMBER() OVER (
                            PARTITION BY D.SHIPMENT_LINE_ID
                            ORDER BY D.ID DESC
                        ) AS rn
                    FROM BUYP.dbo.WHR_SHIMENT_DISPATCH D WITH (NOLOCK)
                    LEFT JOIN BUYP.BUYP.ALJE_ITEM_CATEGORIES_CPD_V I
                        ON D.ITEM_ID = I.ITEM_CODE
                    WHERE {where_sql}
                )
                SELECT *
                FROM CTE
                WHERE rn = 1
            """

            # ✅ Keyset pagination (correct place)
            if last_id:
                sql += " AND ID > %s"
                params.append(int(last_id))

            sql += """
                ORDER BY ID ASC
                OFFSET 0 ROWS FETCH NEXT %s ROWS ONLY
            """
            params.append(page_size)

            with connection.cursor() as cursor:
                cursor.execute(sql, params)
                columns = [c[0] for c in cursor.description]
                rows = cursor.fetchall()

            results = []
            for row in rows:
                r = dict(zip(columns, row))
                received = (r["EXISTING_FIRST_RECEIVED"] or 0) + (r["SUM_PROGRESS_BEFORE"] or 0)
                remaining = (r["QUANTITY_SHIPPED"] or 0) - (received + (r["QUANTITY_PROGRESS"] or 0))

                results.append({
                    "DISPATCH_NUM": r["SHIPMENT_ID"],
                    "DISPATCH_DATE": r["DISPATCH_DATE"].strftime("%d-%b-%Y") if r["DISPATCH_DATE"] else None,
                    "FROM_ORG_CODE": r["ORGANIZATION_CODE"],
                    "FROM_ORG_NAME": r["ORGANIZATION_NAME"],
                    "TO_ORGN_CODE": r["TO_ORGN_CODE"],
                    "TO_ORGN_NAME": r["TO_ORGN_NAME"],
                    "REMARKS": r["REMARKS"],
                    "SHIPMENT_NUM": r["SHIPMENT_NUM"],
                    "ITEM_CODE": r["ITEM_ID"],
                    "DESCRIPTION": r["DISPATCH_DESCRIPTION"],
                    "DISPATCH_QTY": r["QUANTITY_PROGRESS"] or 0,
                    "REMAINING_QTY": remaining
                })

            response = {"page_size": page_size, "results": results}

            if include_count:
                count_sql = f"""
                    SELECT COUNT(DISTINCT D.SHIPMENT_LINE_ID)
                    FROM BUYP.dbo.WHR_SHIMENT_DISPATCH D WITH (NOLOCK)
                    WHERE {where_sql}
                """
                with connection.cursor() as cursor:
                    cursor.execute(count_sql, params[:-1])
                    response["count"] = cursor.fetchone()[0]

            return Response(response)

        except Exception as e:
            return Response({"error": str(e)}, status=500)


class InvoiceReports_CreateDispatch(ViewSet):
    def list(self, request):
        try:
            today = datetime.today().date()

            # ----------------------------
            # Get query parameters
            # ----------------------------
            from_date_str = request.query_params.get("from_date")
            to_date_str = request.query_params.get("to_date")
            undel_id_filter = request.query_params.get("undel_id")
            salesman_no_filter = request.query_params.get("salesman_no")
            customer_no_filter = request.query_params.get("customer_no")
            invoice_no_filter = request.query_params.get("invoice_no")
            dispatch_id_filter = request.query_params.get("dispatch_id")
            filter_type = request.query_params.get("filter_type", "").strip().lower()

            # ----------------------------
            # Parse dates
            # ----------------------------
            from_date = datetime.strptime(from_date_str, "%Y-%m-%d") if from_date_str else None
            to_date = datetime.strptime(to_date_str, "%Y-%m-%d") if to_date_str else None
            if to_date:
                # include full day
                to_date = to_date + timedelta(days=1) - timedelta(seconds=1)

            # ----------------------------
            # Optimized SQL with JOIN
            # ----------------------------
            sql = """
                SELECT 
                    t.DISPATCH_ID,
                    t.UNDEL_ID,
                    t.MANAGER_NO,
                    t.MANAGER_NAME,
                    t.ORG_NAME,
                    t.PHYSICAL_WAREHOUSE,
                    t.CUSTOMER_NUMBER,
                    t.CUSTOMER_NAME,
                    t.CUSTOMER_SITE_ID,
                    t.SALESMAN_NO,
                    t.SALESMAN_NAME,
                    t.INVOICE_NO,
                    t.ORG_ID,
                    t.[DATE],
                    t.LINE_NO,
                    t.ITEM_CODE,
                    t.ITEM_DETAILS,
                    t.TRUCK_SEND_QTY,
                    t.CREATION_DATE,
                    u.QUANTITY,
                    u.AMOUNT
                FROM [BUYP].[dbo].[WHR_TRUCK_SCAN_DETAILS] t
                LEFT JOIN [BUYP].[BUYP].[XXALJE_UNDELIVERED_DATA_BUYP1] u
                    ON t.UNDEL_ID = u.UNDEL_ID
                WHERE t.FLAG != 'R'
            """
            params = []

            if undel_id_filter:
                sql += " AND t.UNDEL_ID = %s"
                params.append(undel_id_filter)

            if dispatch_id_filter:
                sql += " AND t.DISPATCH_ID = %s"
                params.append(dispatch_id_filter)

            if salesman_no_filter:
                sql += " AND LOWER(t.SALESMAN_NO) = %s"
                params.append(salesman_no_filter.lower())

            if customer_no_filter:
                sql += " AND t.CUSTOMER_NUMBER = %s"
                params.append(customer_no_filter)

            if invoice_no_filter:
                sql += " AND t.INVOICE_NO = %s"
                params.append(invoice_no_filter)

            if from_date and to_date:
                sql += " AND t.[DATE] BETWEEN %s AND %s"
                params.extend([from_date, to_date])
            elif from_date:
                sql += " AND t.[DATE] >= %s"
                params.append(from_date)
            elif to_date:
                sql += " AND t.[DATE] <= %s"
                params.append(to_date)

            # ----------------------------
            # Execute single optimized query
            # ----------------------------
            with connection.cursor() as cursor:
                cursor.execute(sql, params)
                columns = [col[0] for col in cursor.description]
                rows = cursor.fetchall()

            if not rows:
                return Response({"message": "No data found for given filters"}, status=200)

            results = []
            truck_data_by_undel = {}

            # Group data by UNDEL_ID
            for row in rows:
                data = dict(zip(columns, row))
                undel_id = data.get("UNDEL_ID")
                if not undel_id:
                    continue
                truck_data_by_undel.setdefault(undel_id, []).append(data)

            # Process results
            for undel_id, entries in truck_data_by_undel.items():
                qty = entries[0].get("QUANTITY") or 0
                amount = entries[0].get("AMOUNT") or 0
                item_cost = round(amount / qty, 2) if qty > 0 else 0.0

                total_dispatched_qty = sum(entry.get("TRUCK_SEND_QTY") or 0 for entry in entries)
                final_value = round(item_cost * total_dispatched_qty, 2)

                latest_entry = entries[-1]
                creation_date = latest_entry.get("CREATION_DATE")
                if isinstance(creation_date, datetime):
                    creation_date = creation_date.date()

                days = (today - creation_date).days if creation_date else None

                # Aging buckets
                buckets = {
                    "0-30_Days": 0, "31-60_Days": 0, "61-90_Days": 0, "91-120_Days": 0,
                    "121-150_Days": 0, "151-180_Days": 0, "181-360_Days": 0, "360+_Days": 0
                }

                invoice_date_val = latest_entry.get("DATE")
                formatted_invoice_date = ""
                if invoice_date_val:
                    if isinstance(invoice_date_val, datetime):
                        formatted_invoice_date = invoice_date_val.strftime("%d-%b-%Y")
                    elif isinstance(invoice_date_val, str):
                        try:
                            formatted_invoice_date = datetime.strptime(
                                invoice_date_val, "%Y-%m-%dT%H:%M:%S"
                            ).strftime("%d-%b-%Y")
                        except ValueError:
                            try:
                                formatted_invoice_date = datetime.strptime(
                                    invoice_date_val, "%Y-%m-%d"
                                ).strftime("%d-%b-%Y")
                            except Exception:
                                formatted_invoice_date = invoice_date_val

                if days is not None:
                    value = total_dispatched_qty if filter_type == "qtywise" else final_value
                    if 0 <= days <= 30: buckets["0-30_Days"] = value
                    elif 31 <= days <= 60: buckets["31-60_Days"] = value
                    elif 61 <= days <= 90: buckets["61-90_Days"] = value
                    elif 91 <= days <= 120: buckets["91-120_Days"] = value
                    elif 121 <= days <= 150: buckets["121-150_Days"] = value
                    elif 151 <= days <= 180: buckets["151-180_Days"] = value
                    elif 181 <= days <= 360: buckets["181-360_Days"] = value
                    elif days > 360: buckets["360+_Days"] = value

                results.append({
                    "Dispatch Id": latest_entry.get("DISPATCH_ID"),
                    "WHR SuperUser No": latest_entry.get("MANAGER_NO"),
                    "WHR SuperUser Name": latest_entry.get("MANAGER_NAME"),
                    "Region": latest_entry.get("ORG_NAME"),
                    "Warehosue": latest_entry.get("PHYSICAL_WAREHOUSE"),
                    "Customer No": latest_entry.get("CUSTOMER_NUMBER"),
                    "Customer Name": latest_entry.get("CUSTOMER_NAME"),
                    "Customer site id": latest_entry.get("CUSTOMER_SITE_ID"),
                    "Salesman No": latest_entry.get("SALESMAN_NO"),
                    "Salesman Name": latest_entry.get("SALESMAN_NAME"),
                    "Invoice Number": int(latest_entry.get("INVOICE_NO") or 0),
                    "Organization Code": latest_entry.get("ORG_ID"),
                    "Invoice Date": formatted_invoice_date,
                    "Line Number": latest_entry.get("LINE_NO"),
                    "Item Code": latest_entry.get("ITEM_CODE"),
                    "Desription": latest_entry.get("ITEM_DETAILS"),
                    "Invoice Qty": qty,
                    "Dispatch Qty": total_dispatched_qty,
                    "Sales Value": item_cost,
                    "Total Value": final_value,
                    "Days": days,
                    **buckets
                })

            # ----------------------------
            # Pagination
            # ----------------------------
            paginator = PageNumberPagination()
            paginator.page_size = 100000
            paginated_data = paginator.paginate_queryset(results, request)
            return paginator.get_paginated_response(paginated_data)

        except Exception as e:
            return Response({"error": str(e)}, status=500)


from datetime import datetime, timedelta
from rest_framework.viewsets import ViewSet
from rest_framework.response import Response
from rest_framework.pagination import PageNumberPagination
from django.db import connection


class Return_Dispatch_Report_view(ViewSet):
    def list(self, request):
        try:
            # ----------------------------
            # Get filter parameters
            # ----------------------------
            from_date_str = request.query_params.get("from_date")
            to_date_str = request.query_params.get("to_date")
            undel_id_filter = request.query_params.get("undel_id")
            salesman_no_filter = request.query_params.get("salesman_no")
            customer_no_filter = request.query_params.get("customer_no")
            invoice_no_filter = request.query_params.get("invoice_no")
            dispatch_id = request.query_params.get("dispatch_id")
            flag_status_filter = request.query_params.get("flag_status")

            # ----------------------------
            # Parse dates safely
            # ----------------------------
            from_date = datetime.strptime(from_date_str, "%Y-%m-%d") if from_date_str else None
            to_date = datetime.strptime(to_date_str, "%Y-%m-%d") if to_date_str else None
            if to_date:
                to_date = to_date + timedelta(days=1) - timedelta(seconds=1)

            # ----------------------------
            # Base query (using LEFT JOIN for completeness)
            # ----------------------------
            sql = """
           SELECT 
                r.INVOICE_RETURN_ID AS [Inv Return Id],
                r.[DATE] AS [Date],
                r.ORG_ID AS [Organization Code],
                r.ORG_NAME AS [Region],
                r.MANAGER_NO AS [WHR SuperUser No],
                r.MANAGER_NAME AS [WHR SuperUser Name],
                r.SALESMANO_NO AS [Salesman No],
                r.SALESMAN_NAME AS [Salesman Name],
                r.CUSTOMER_NUMBER AS [Customer No],
                r.CUSTOMER_NAME AS [Customer Name],
                r.CUSTOMER_SITE_ID AS [Customer Site Id],
                r.INVOICE_NUMBER AS [Invoice Number],
                r.LINE_NUMBER AS [Line Number],
                r.ITEM_CODE AS [Item Code],
                r.ITEM_DESCRIPTION AS [Item Description],
                r.TOT_QUANTITY AS [Invoice Qty],
                r.RETURNED_QTY AS [Return Qty],
                r.REMARKS AS [Remarks]
            FROM [BUYP].[dbo].[WHR_INVOICE_RETURN_HISTORY_TBL] r
            WHERE 1 = 1 

            """

            params = []

            # ----------------------------
            # Dynamic filters
            # ----------------------------
            if undel_id_filter:
                sql += " AND r.UNDEL_ID = %s"
                params.append(undel_id_filter)

            if salesman_no_filter:
                sql += " AND r.SALESMANO_NO = %s"
                params.append(salesman_no_filter)

            if customer_no_filter:
                sql += " AND r.CUSTOMER_NUMBER = %s"
                params.append(customer_no_filter)

            if invoice_no_filter:
                sql += " AND r.INVOICE_NUMBER = %s"
                params.append(invoice_no_filter)

            if dispatch_id:
                sql += " AND r.INVOICE_RETURN_ID = %s"
                params.append(dispatch_id)

            if flag_status_filter:
                sql += " AND r.FLAG_STATUS = %s"
                params.append(flag_status_filter)

            if from_date and to_date:
                sql += " AND r.[DATE] BETWEEN %s AND %s"
                params.extend([from_date, to_date])
            elif from_date:
                sql += " AND r.[DATE] >= %s"
                params.append(from_date)
            elif to_date:
                sql += " AND r.[DATE] <= %s"
                params.append(to_date)

            sql += " ORDER BY r.[DATE] DESC"

            # ----------------------------
            # Execute query safely
            # ----------------------------
            with connection.cursor() as cursor:
                cursor.execute(sql, params)
                columns = [col[0] for col in cursor.description]
                rows = cursor.fetchall()

            # Convert to list of dicts
            results = [dict(zip(columns, row)) for row in rows]

            # ----------------------------
            # Pagination (50,000 rows/page)
            # ----------------------------
            paginator = PageNumberPagination()
            paginator.page_size = 100000
            paginated_data = paginator.paginate_queryset(results, request)


            return paginator.get_paginated_response(paginated_data)

        except Exception as e:
            return Response({"error": str(e)}, status=500)


# class InvoiceReports_CreateDispatch(ViewSet):
#     def list(self, request):
#         try:
#             today = datetime.today().date()

#             # ----------------------------
#             # Get query parameters
#             # ----------------------------
#             from_date_str = request.query_params.get("from_date")
#             to_date_str = request.query_params.get("to_date")
#             undel_id_filter = request.query_params.get("undel_id")
#             salesman_no_filter = request.query_params.get("salesman_no")
#             customer_no_filter = request.query_params.get("customer_no")
#             invoice_no_filter = request.query_params.get("invoice_no")
#             dispatch_id_filter = request.query_params.get("dispatch_id")
#             filter_type = request.query_params.get("filter_type", "").strip().lower()

#             # ----------------------------
#             # Parse dates
#             # ----------------------------
#             from_date = datetime.strptime(from_date_str, "%Y-%m-%d") if from_date_str else None
#             to_date = datetime.strptime(to_date_str, "%Y-%m-%d") if to_date_str else None
#             if to_date:
#                 # include full day
#                 to_date = to_date + timedelta(days=1) - timedelta(seconds=1)

#             # ----------------------------
#             # Build base SQL query
#             # ----------------------------
#             sql = "SELECT * FROM [BUYP].[dbo].[WHR_TRUCK_SCAN_DETAILS] WHERE FLAG != 'R'"
#             params = []

#             if undel_id_filter:
#                 sql += " AND UNDEL_ID = %s"
#                 params.append(undel_id_filter)

#             if dispatch_id_filter:
#                 sql += " AND DISPATCH_ID = %s"
#                 params.append(dispatch_id_filter)

#             if salesman_no_filter:
#                 sql += " AND LOWER(SALESMAN_NO) = %s"
#                 params.append(salesman_no_filter.lower())

#             if customer_no_filter:
#                 sql += " AND CUSTOMER_NUMBER = %s"
#                 params.append(customer_no_filter)

#             if invoice_no_filter:
#                 sql += " AND INVOICE_NO = %s"
#                 params.append(invoice_no_filter)

#             if from_date and to_date:
#                 sql += " AND [DATE] BETWEEN %s AND %s"
#                 params.extend([from_date, to_date])
#             elif from_date:
#                 sql += " AND [DATE] >= %s"
#                 params.append(from_date)
#             elif to_date:
#                 sql += " AND [DATE] <= %s"
#                 params.append(to_date)

#             # ----------------------------
#             # Execute query
#             # ----------------------------
#             with connection.cursor() as cursor:
#                 cursor.execute(sql, params)
#                 columns = [col[0] for col in cursor.description]
#                 rows = cursor.fetchall()

#             if not rows:
#                 return Response({"message": "No data found for given filters"}, status=200)

#             truck_data_by_undel = {}
#             for row in rows:
#                 data = dict(zip(columns, row))
#                 undel_id = data.get("UNDEL_ID")
#                 if not undel_id:
#                     continue
#                 truck_data_by_undel.setdefault(undel_id, []).append(data)

#             results = []

#             # ----------------------------
#             # Process results
#             # ----------------------------
#             for undel_id, entries in truck_data_by_undel.items():
#                 with connection.cursor() as cursor2:
#                     cursor2.execute("""
#                         SELECT QUANTITY, AMOUNT
#                         FROM [BUYP].[BUYP].[XXALJE_UNDELIVERED_DATA_BUYP1]
#                         WHERE UNDEL_ID = %s
#                     """, [undel_id])
#                     fetched = cursor2.fetchone()

#                 if not fetched:
#                     continue

#                 qty, amount = fetched
#                 qty = qty or 0
#                 amount = amount or 0
#                 item_cost = round(amount / qty, 2) if qty > 0 else 0.0

#                 total_dispatched_qty = sum(entry.get("TRUCK_SEND_QTY") or 0 for entry in entries)
#                 final_value = round(item_cost * total_dispatched_qty, 2)

#                 latest_entry = entries[-1]
#                 creation_date = latest_entry.get("CREATION_DATE")
#                 if isinstance(creation_date, datetime):
#                     creation_date = creation_date.date()

#                 days = (today - creation_date).days if creation_date else None

#                 buckets = {
#                     "0-30_Days": 0, "31-60_Days": 0, "61-90_Days": 0, "91-120_Days": 0,
#                     "121-150_Days": 0, "151-180_Days": 0, "181-360_Days": 0, "360+_Days": 0
#                 }
#                 invoice_date_val = latest_entry.get("DATE")
#                 formatted_invoice_date = ""

#                 if invoice_date_val:
#                     if isinstance(invoice_date_val, datetime):
#                         formatted_invoice_date = invoice_date_val.strftime("%d-%b-%Y")
#                     elif isinstance(invoice_date_val, str):
#                         try:
#                             formatted_invoice_date = datetime.strptime(invoice_date_val, "%Y-%m-%dT%H:%M:%S").strftime("%d-%b-%Y")
#                         except ValueError:
#                             try:
#                                 # fallback for plain date string without time
#                                 formatted_invoice_date = datetime.strptime(invoice_date_val, "%Y-%m-%d").strftime("%d-%b-%Y")
#                             except Exception:
#                                 formatted_invoice_date = invoice_date_val  # keep as is if unrecognized
#                 if days is not None:
#                     value = total_dispatched_qty if filter_type == "qtywise" else final_value
#                     if 0 <= days <= 30: buckets["0-30_Days"] = value
#                     elif 31 <= days <= 60: buckets["31-60_Days"] = value
#                     elif 61 <= days <= 90: buckets["61-90_Days"] = value
#                     elif 91 <= days <= 120: buckets["91-120_Days"] = value
#                     elif 121 <= days <= 150: buckets["121-150_Days"] = value
#                     elif 151 <= days <= 180: buckets["151-180_Days"] = value
#                     elif 181 <= days <= 360: buckets["181-360_Days"] = value
#                     elif days > 360: buckets["360+_Days"] = value

#                 result_row = {
#                     "Dispatch Id": latest_entry.get("DISPATCH_ID"),
#                     # "UNDEL_ID": undel_id,
#                     "WHR SuperUser No": latest_entry.get("MANAGER_NO"),
#                     "WHR SuperUser Name": latest_entry.get("MANAGER_NAME"),
#                     "Region": latest_entry.get("ORG_NAME"),
#                     "Warehosue": latest_entry.get("PHYSICAL_WAREHOUSE"),
#                     "Customer No": latest_entry.get("CUSTOMER_NUMBER"),
#                     "Customer Name": latest_entry.get("CUSTOMER_NAME"),
#                     "Customer site id": latest_entry.get("CUSTOMER_SITE_ID"),
#                     "Salesman No": latest_entry.get("SALESMAN_NO"),
#                     "Salesman Name": latest_entry.get("SALESMAN_NAME"),
#                     "Invoice Number": int(latest_entry.get("INVOICE_NO") or 0),

#                     "Organization Code": latest_entry.get("ORG_ID"),
#                     "Invoice Date": formatted_invoice_date,
#                     "Line Number": latest_entry.get("LINE_NO"),
#                     "Item Code": latest_entry.get("ITEM_CODE"),
#                     "Desription": latest_entry.get("ITEM_DETAILS"),
#                     "Invoice Qty": qty,
#                     "Dispatch Qty": total_dispatched_qty,
#                     "Sales Value": item_cost,
#                     "Total Value": final_value,
#                     "Days": days,
#                     **buckets
#                 }
#                 results.append(result_row)

#             # ----------------------------
#             # Pagination
#             # ----------------------------
#             paginator = PageNumberPagination()
#             paginator.page_size = 20
#             paginated_data = paginator.paginate_queryset(results, request)
#             return paginator.get_paginated_response(paginated_data)

#         except Exception as e:
#             return Response({"error": str(e)}, status=500)



# for inbound datas 
class  GenerateDocNoView(APIView):
    def get(self, request, *args, **kwargs):
        # Generate CSRF token
        token = get_token(request)
 
        # Get current year and month
        now = datetime.now()
        year_short = str(now.year)[-2:]  # '25' for 2025
        month = f"{now.month:02d}"      # '05' for May
        prefix = f"DC{year_short}{month}"
 
        # Get latest DOC_NO starting with this prefix
        latest_doc = DocNO_Models.objects.filter(DOC_NO__startswith=prefix).order_by('-id').first()
 
        if latest_doc:
            last_doc_no = latest_doc.DOC_NO
            match = re.match(rf"{prefix}(\d+)", last_doc_no)
            if match:
                last_number = int(match.group(1))
                next_number = last_number + 1
            else:
                next_number = 1
        else:
            next_number = 1
 
        next_doc_no = f"{prefix}{next_number:03d}"
 
        # Save to DB
        DocNO_Models.objects.create(DOC_NO=next_doc_no, TOKEN=token)
 
        return Response({
            "DOC_NO": next_doc_no,
            "TOKEN": token
        }, status=status.HTTP_200_OK)
 
class DocNoView(APIView):
    def get(self, request, *args, **kwargs):
        # Get current year and month
        now = datetime.now()
        year_short = str(now.year)[-2:]  # '25'
        month = f"{now.month:02d}"      # '05'
        prefix = f"DC{year_short}{month}"
 
        # Default DOC_NO if no records
        default_doc_no = f"{prefix}000"
 
        # Get latest entry from table
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT TOP 1 [id], [DOC_NO], [TOKEN]
                FROM [BUYP].[dbo].[WHR_INBOUND_DOCNO]
                ORDER BY [id] DESC;
            """)
            result = cursor.fetchone()
 
        # Determine DOC_NO to return
        if result and result[1]:
            doc_no = result[1]
        else:
            doc_no = default_doc_no
 
        return Response({'DOC_NO': doc_no})

def get_pending_po(request):
    po_number = request.GET.get('po_number')  # GET param: ?po_number=8403

    if not po_number:
        return JsonResponse({'error': 'po_number is required'}, status=400)

    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT *
            FROM [BUYP].[dbo].[XXALJEBUYP_PENDING_PO]
            WHERE PO_NUMBER = %s
        """, [po_number])

        columns = [col[0] for col in cursor.description]
        data = [dict(zip(columns, row)) for row in cursor.fetchall()]

    return JsonResponse(data, safe=False)

class SaveShipmentView(APIView):
    @method_decorator(csrf_exempt, name='dispatch')
    def post(self, request, *args, **kwargs):
        try:
            data = request.data
            # print("Received data:", data)

            # Required field check
            required_fields = [
                "DOC_NO", "SUPPLIER", "HOUSE_BL", "CONTAINER_COUNT",
                "CLEARANCE_DATE", "SUPPLIER_NAME", "SUPPLIER_NO", "BL_DATE",
                "LC_NO", "BAYAN_DATE", "ETD_DATE", "POD", "POL", "BILL_NO",
                "MASTER_BL", "ETA_DATE", "LINE_NAME", "BILL_DUE_DATE",
                "INVOICE_NO1", "INVOICE_NO2", "INVOICE_NO3",
                "INVOICE_1VALUE", "INVOICE_2VALUE", "INVOICE_3VALUE",
                "TERM_1PAYMENT", "TERM_2PAYMENT", "TERM_3PAYMENT",
                "INCOTERM1", "INCOTERM2", "INCOTERM3"
            ]
            for field in required_fields:
                if field not in data:
                    return Response({"status": "error", "message": f"Missing field: {field}"}, status=status.HTTP_400_BAD_REQUEST)

            def parse_int(value):
                if value is None or value == "":
                    return None
                try:
                    return int(value)
                except (ValueError, TypeError):
                    raise ValueError(f"Invalid integer for field CONTAINER_COUNT: {value}")

            def parse_date(value):
                if value is None or value == "":
                    return None
                for fmt in ('%Y-%m-%d', '%d-%b-%Y', '%d-%B-%Y'):
                    try:
                        return datetime.strptime(value, fmt).date()
                    except ValueError:
                        continue
                raise ValueError(f"Invalid date format: {value}. Expected formats: YYYY-MM-DD or DD-MMM-YYYY.")

            values = [
                data.get("DOC_NO"),
                data.get("SUPPLIER"),
                data.get("HOUSE_BL"),
                parse_int(data.get("CONTAINER_COUNT")),
                parse_date(data.get("CLEARANCE_DATE")),
                data.get("SUPPLIER_NAME"),
                data.get("SUPPLIER_NO"),
                parse_date(data.get("BL_DATE")),
                data.get("LC_NO"),
                parse_date(data.get("BAYAN_DATE")),
                parse_date(data.get("ETD_DATE")),
                data.get("POD"),
                data.get("POL"),
                data.get("BILL_NO"),
                data.get("MASTER_BL"),
                parse_date(data.get("ETA_DATE")),
                data.get("LINE_NAME"),
                parse_date(data.get("BILL_DUE_DATE")),
                data.get("INVOICE_NO1"),
                data.get("INVOICE_NO2"),
                data.get("INVOICE_NO3"),
                data.get("INVOICE_1VALUE"),
                data.get("INVOICE_2VALUE"),
                data.get("INVOICE_3VALUE"),
                data.get("TERM_1PAYMENT"),
                data.get("TERM_2PAYMENT"),
                data.get("TERM_3PAYMENT"),
                data.get("INCOTERM1"),
                data.get("INCOTERM2"),
                data.get("INCOTERM3"),
            ]

            # print("Values being inserted:", values)

            with connection.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO [BUYP].[dbo].[WHR_INBOUND_SHIPMENT_HEADER] (
                        DOC_NO, SUPPLIER, HOUSE_BL, CONTAINER_COUNT, CLEARANCE_DATE,
                        SUPPLIER_NAME, SUPPLIER_NO, BL_DATE, LC_NO, BAYAN_DATE,
                        ETD_DATE, POD, POL, BILL_NO, MASTER_BL, ETA_DATE,
                        LINE_NAME, BILL_DUE_DATE, INVOICE_NO1, INVOICE_NO2, INVOICE_NO3,
                        INVOICE_1VALUE, INVOICE_2VALUE, INVOICE_3VALUE,
                        TERM_1PAYMENT, TERM_2PAYMENT, TERM_3PAYMENT,
                        INCOTERM1, INCOTERM2, INCOTERM3
                    ) VALUES (
                       %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                       %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                """, values)

            return Response({"status": "success"}, status=status.HTTP_200_OK)

        except ValueError as ve:
            return Response({"status": "error", "message": str(ve)}, status=status.HTTP_400_BAD_REQUEST)

        except Exception as e:
            import traceback
            # print("❌ Shipment save failed:", str(e))
            # print(traceback.format_exc())
            return Response({"status": "error", "message": "Internal server error."}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

class SaveContainerInfoView(APIView):
    @method_decorator(csrf_exempt, name='dispatch')
    def post(self, request, *args, **kwargs):
        try:
            data = request.data
            # print("📦 Received Container Data:", data)

            required_fields = [ "DOC_NO", "CONTAINER_NO", "SIZE"]
            for field in required_fields:
                if field not in data or data[field] is None:
                    return Response({"status": "error", "message": f"Missing field: {field}"}, status=status.HTTP_400_BAD_REQUEST)

            values = [
                data.get("DOC_NO"),
                data.get("CONTAINER_NO"),
                data.get("SIZE")
            ]

            with connection.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO [BUYP].[dbo].[WHR_INBOUND_CONTAINER_INFO] (
                        DOC_NO, CONTAINER_NO, SIZE
                    ) VALUES (%s, %s, %s)
                """, values)

            return Response({"status": "success", "message": "Container info saved successfully."}, status=status.HTTP_200_OK)

        except Exception as e:
            import traceback
            # print("❌ Error saving container info:", str(e))
            # print(traceback.format_exc())
            return Response({"status": "error", "message": "Internal server error."}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

class SaveProductInfoView(APIView):
    def post(self, request, *args, **kwargs):
        try:
            product_list = request.data
            # print("📦 Received Product List:", product_list)

            required_fields = [
                "DOC_NO", "PO_NUMBER", "FRANCHISE", "FAMILY", "CLASS",
                "SUBCLASS", "ITEM_CODE", "PO_QTY", "REC_QTY", 
                "BALANCE_QTY", "SHIPPED_QTY", "CONTAINER_NO"
            ]

            # Validate all products
            for idx, data in enumerate(product_list):
                for field in required_fields:
                    if not data.get(field) and data.get(field) != 0:
                        return Response({
                            "status": "error",
                            "message": f"Missing or empty field: {field} in product #{idx + 1}"
                        }, status=status.HTTP_400_BAD_REQUEST)

            with connection.cursor() as cursor:
                for data in product_list:
                    # Convert fields to correct types
                    po_qty = float(data.get("PO_QTY"))
                    rec_qty = float(data.get("REC_QTY"))
                    balance_qty = float(data.get("BALANCE_QTY"))
                    shipped_qty = float(data.get("SHIPPED_QTY"))
                    container_no = data.get("CONTAINER_NO") or ""  # string

                    values = [
                        data.get("DOC_NO"),
                        data.get("PO_NUMBER"),
                        data.get("FRANCHISE"),
                        data.get("FAMILY"),
                        data.get("CLASS"),
                        data.get("SUBCLASS"),
                        data.get("ITEM_CODE"),
                        po_qty,
                        rec_qty,
                        balance_qty,
                        shipped_qty,
                        container_no
                    ]

                    cursor.execute("""
                        INSERT INTO [BUYP].[dbo].[WHR_INBOUND_PRODUCT_INFO] (
                            DOC_NO, PO_NUMBER, FRANCHISE, FAMILY, CLASS, 
                            SUBCLASS, ITEM_CODE, PO_QTY, REC_QTY, 
                            BALANCE_QTY, SHIPPED_QTY, CONTAINER_NO
                        )  VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, values)

            return Response({
                "status": "success",
                "message": "All product info saved successfully."
            }, status=status.HTTP_201_CREATED)

        except Exception as e:
            import traceback
            # print("❌ Error saving product info:", str(e))
            # print(traceback.format_exc())
            return Response({
                "status": "error",
                "message": "Internal server error."
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

def get_expense_cat(request):
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT  [EXPENSE_CAT]
            FROM [BUYP].[dbo].[WHR_INBOUND_EXPENSE_CAT]
        """)
        rows = [row[0] for row in cursor.fetchall()]
    return JsonResponse({"expense_categories": rows})

def get_names_by_expense_cat(request, cat):
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT [NAME]
            FROM [BUYP].[dbo].[WHR_INBOUND_EXPENSE_CAT_NAME]
            WHERE [EXPENSE_CAT] = %s
        """, [cat])
        rows = [row[0] for row in cursor.fetchall()]
    return JsonResponse({"category": cat, "names": rows})

@method_decorator(csrf_exempt, name='dispatch')
class SaveExpenseView(APIView):
    def post(self, request, *args, **kwargs):
        try:
            data = request.data
            # print("📥 Received Expense Data:", data)

            expenses = data.get("expense_data")
            if not expenses or not isinstance(expenses, list):
                return Response({
                    "status": "error",
                    "message": "Invalid or missing 'expense_data'. Must be a list."
                }, status=status.HTTP_400_BAD_REQUEST)

            with connection.cursor() as cursor:
                for expense in expenses:
                    required_fields = ["BAYAN_NO", "EXPENSE_CAT", "NAME", "AMOUNT"]
                    for field in required_fields:
                        if field not in expense or expense[field] in [None, ""]:
                            return Response({
                                "status": "error",
                                "message": f"Missing or empty field: {field}"
                            }, status=status.HTTP_400_BAD_REQUEST)

                    try:
                        amount = float(expense["AMOUNT"])
                    except (TypeError, ValueError):
                        return Response({
                            "status": "error",
                            "message": "AMOUNT must be a valid number"
                        }, status=status.HTTP_400_BAD_REQUEST)

                    cursor.execute("""
                        INSERT INTO [BUYP].[dbo].[WHR_INBOUND_EXPENSE_DETAILS] (
                            BAYAN_NO, EXPENSE_CAT, NAME, AMOUNT
                        ) VALUES (%s, %s, %s, %s)
                    """, [
                        expense["BAYAN_NO"],
                        expense["EXPENSE_CAT"],
                        expense["NAME"],
                        amount
                    ])

            return Response({
                "status": "success",
                "message": "All expenses saved successfully."
            }, status=status.HTTP_200_OK)

        except Exception as e:
            import traceback
            # print("❌ Error saving expenses:", str(e))
            # print(traceback.format_exc())
            return Response({
                "status": "error",
                "message": "Internal server error."
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

# For Dashboard Salesman
  
def getPendingInvoice_for_salesman(request, salesman_name):
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT
                INVOICE_NUMBER,
                SUM(ISNULL(QUANTITY, 0)) AS Total_Quantity,
                SUM(ISNULL(DISPATCH_QTY, 0)) AS Total_Dispatch_Qty,
                SUM(ISNULL(RETURN_QTY, 0)) AS Total_Return_Qty,
                -- Calculate Dispatch + Return as Final_Request_Qty0
                SUM(ISNULL(DISPATCH_QTY, 0)) + SUM(ISNULL(RETURN_QTY, 0)) AS Final_Request_Qty,
                -- Calculate Quantity - (Dispatch + Return) as Final_Qty
                SUM(ISNULL(QUANTITY, 0)) - (SUM(ISNULL(DISPATCH_QTY, 0)) + SUM(ISNULL(RETURN_QTY, 0))) AS Final_Qty
            FROM
                [BUYP].[BUYP].[XXALJE_UNDELIVERED_DATA_BUYP1]
            WHERE
                SALESMAN_NO = %s
            GROUP BY
                INVOICE_NUMBER
            HAVING
                SUM(ISNULL(QUANTITY, 0)) - (SUM(ISNULL(DISPATCH_QTY, 0)) + SUM(ISNULL(RETURN_QTY, 0))) > 0
            ORDER BY
                INVOICE_NUMBER;
        """, [salesman_name])
       
        # Fetch all results
        results = cursor.fetchall()
       
        # Prepare the response data
        pending_invoices = []
        for row in results:
            pending_invoices.append({
                "invoice_number": row[0],
                "total_quantity": row[1],
                "total_dispatch_qty": row[2],
                "total_return_qty": row[3],
                "final_request_qty": row[4],
                "final_qty": row[5],
            })
       
        # Count of pending invoices
        pending_invoice_count = len(pending_invoices)
 
    return JsonResponse({
        "salesman_name": salesman_name,
        "pendingInvoice_count": pending_invoice_count,
        "pending_invoices": pending_invoices
    })

def getcompleted_dispatches_for_salesman(request, salesman_name):
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT COUNT(DISTINCT REQ_ID)
            FROM BUYP.dbo.WHR_CREATE_DISPATCH
            WHERE TRUCK_SCAN_QTY = 0 AND SALESMAN_NAME = %s  
        """, [salesman_name])
        count = cursor.fetchone()[0]
 
    return JsonResponse({
        "salesman_name": salesman_name,
        "unscanned_invoice_count": count
    })

def get_On_Progress_dispatches_for_salesman(request, salesman_name):
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT COUNT(DISTINCT REQ_ID)
            FROM BUYP.dbo.WHR_CREATE_DISPATCH
            WHERE TRUCK_SCAN_QTY != 0 AND SALESMAN_NAME = %s 
        """, [salesman_name])
        count = cursor.fetchone()[0]
 
    return JsonResponse({
        "salesman_name": salesman_name,
        "scanned_invoice_count": count
    })
 
def undelivered_customer_count(request, salesman_name):
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT COUNT(DISTINCT CUSTOMER_NAME)
            FROM BUYP.BUYP.XXALJE_UNDELIVERED_DATA_BUYP1
            WHERE SALESMAN_NAME = %s
        """, [salesman_name])
        customer_count = cursor.fetchone()[0]
 
    return JsonResponse({
        "salesman_name": salesman_name,
        "customer_count": customer_count
    })

# For Dashboard Manager

def warehouse_dispatch_count(request, warehouse_name):
    with connection.cursor() as cursor:
        cursor.execute("""
           SELECT COUNT(DISTINCT REQ_ID)
            FROM BUYP.dbo.WHR_CREATE_DISPATCH
            WHERE PHYSICAL_WAREHOUSE = %s
            AND TRUCK_SCAN_QTY != 0 ;
        """, [warehouse_name])
       
        dispatch_count_manager = cursor.fetchone()[0]

    return JsonResponse({
        "warehouse_name": warehouse_name,
        "dispatch_invoice_count": dispatch_count_manager
    })
 
class FilteredPendingDispatchRequestCount(APIView):
    def get(self, request, physical_warehouse):
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT COUNT(DISTINCT REQ_ID)
                FROM WHR_DISPATCH_REQUEST
                WHERE STATUS = 'pending'
                  AND PHYSICAL_WAREHOUSE = %s and FLAG != 'OU'
            """, [physical_warehouse])
            count = cursor.fetchone()[0]

        return Response({"count": count}, status=status.HTTP_200_OK)

@csrf_exempt
def get_LivestageCountView_by_warehouse(request, warehouse_name):
    with connection.cursor() as cursor:
        cursor.execute("""
             SELECT 
                    COUNT(*) AS MISMATCHED_PICK_COUNT
                FROM (
                    SELECT 
                        w.PICK_ID
                    FROM 
                        WHR_PICKED_MAN AS w
                    LEFT JOIN 
                        WHR_TRUCK_SCAN_DETAILS AS t
                        ON w.PICK_ID = t.PICK_ID
                    WHERE 
                        w.PHYSICAL_WAREHOUSE =%s
                        AND w.FLAG != 'SR' AND w.FLAG != 'OU'
                    GROUP BY 
                        w.PICK_ID
                    HAVING 
                        COUNT(w.PICKED_QTY) != ISNULL(SUM(t.TRUCK_SEND_QTY), 0)
                ) AS subquery
                WHERE NOT EXISTS (
                    SELECT 1 
                    FROM WHR_SAVE_TRUCK_DETAILS_TBL s 
                    WHERE s.pick_id = subquery.PICK_ID 
                    AND s.SCAN_STATUS = 'Request for delivery'
                )
        """, [warehouse_name])

        row = cursor.fetchone()
        invoice_count = row[0] if row else 0

    return JsonResponse({'invoice_count': invoice_count})

@csrf_exempt
def get_Dispatch_RequestCount_warehouse(request, warehouse_name):
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT COUNT(DISTINCT w.pick_id) AS InvoiceCount
            FROM [WHR_SAVE_TRUCK_DETAILS_TBL] AS w
            JOIN WHR_PICKED_MAN AS p ON w.Item_code = p.INVENTORY_ITEM_ID
            WHERE p.PHYSICAL_WAREHOUSE = %s AND SCAN_STATUS = 'Request for Delivery'
        """, [warehouse_name])
        
        row = cursor.fetchone()
        DisReq_count = row[0] if row else 0

    return JsonResponse({'DisReq_count': DisReq_count})

def delivered_count(request, warehouse_name):
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT COUNT(DISTINCT DISPATCH_ID)
            FROM BUYP.dbo.WHR_TRUCK_SCAN_DETAILS
            WHERE ORG_NAME = %s AND FLAG !='R' and FLAG!='OU'
        """, [warehouse_name])
       
        delivered_count = cursor.fetchone()[0]

    return JsonResponse({
        "warehouse_name": warehouse_name,
        "delivered_invoice_count": delivered_count
    })

def ReturnInvoice_count(request,warehouse_name):
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT COUNT(DISTINCT INVOICE_RETURN_ID)
            FROM WHR_INVOICE_RETURN_HISTORY_TBL           
            WHERE ORG_NAME = %s 
        """, [warehouse_name])
        
        ReturnInvoice_count = cursor.fetchone()[0]

    return JsonResponse({
        "ReturnInvoice_count": ReturnInvoice_count
    })

@csrf_exempt
def get_InterORG_count(request):
    warehouse_name = request.GET.get('WAREHOUSE_NAME')  # or use POST.get if it's sent via POST

    if not warehouse_name:
        return JsonResponse({
            "error": "WAREHOUSE_NAME parameter is required."
        }, status=400)

    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT COUNT(DISTINCT SHIPMENT_ID)
            FROM [BUYP].[dbo].[WHR_SHIMENT_DISPATCH]
            WHERE WAREHOUSE_NAME = %s
        """, [warehouse_name])
        InterORG_count = cursor.fetchone()[0]

    return JsonResponse({
        "InterORG_count": InterORG_count
    })

# For Dashboard Pickman

class FilteredPendingPickCount(APIView):
    def get(self, request, pickman_name):
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT COUNT(DISTINCT PICK_ID)
                FROM WHR_DISPATCH_REQUEST
                WHERE STATUS = 'pending'
                  AND ASSIGN_PICKMAN = %s  and FLAG != 'OU'
            """, [pickman_name])
            count = cursor.fetchone()[0]

        return Response({"pending_pick_count": count}, status=status.HTTP_200_OK)

class PickCompleted_Count(APIView):
    def get(self, request, pickman_name):
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT COUNT(DISTINCT PICK_ID)
                  FROM WHR_DISPATCH_REQUEST
                     WHERE ASSIGN_PICKMAN = %s and SCANNED_QTY !='0'  and PICKED_QTY = SCANNED_QTY and FLAG != 'OU'
            """, [pickman_name])
            count = cursor.fetchone()[0]

        return Response({"pickComplete_count": count}, status=status.HTTP_200_OK)

class StageReturn_Count(APIView):
    def get(self, request, pickman_name):
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT COUNT(DISTINCT PICK_ID)
                FROM WHR_PICKED_MAN
                WHERE PICKMAN_NAME = %s and FLAG = 'SR'
            """, [pickman_name])
            count = cursor.fetchone()[0]

        return Response({"stagereturn_count": count}, status=status.HTTP_200_OK)

# For Dashboard - Chart SALESMAN

def get_weekly_dispatches_for_salesman(request, salesman_name):
    salesman_name = unquote(salesman_name)
 
    # Set week range from Sunday to Saturday
    today = datetime.today()
    start_of_week = today - timedelta(days=today.weekday() + 1) if today.weekday() != 6 else today
    end_of_week = start_of_week + timedelta(days=6)
 
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT
                DATENAME(WEEKDAY, CREATION_DATE) AS day_name,
                COUNT(DISTINCT REQ_ID) AS dispatch_count
            FROM BUYP.dbo.WHR_CREATE_DISPATCH
            WHERE
                SALESMAN_NAME = %s AND
                CAST(CREATION_DATE AS DATE) BETWEEN %s AND %s
            GROUP BY DATENAME(WEEKDAY, CREATION_DATE)
        """, [salesman_name, start_of_week.date(), end_of_week.date()])
 
        rows = cursor.fetchall()
 
    # Map: Sunday = 0 through Saturday = 6
    week_days_map = {
        "Sunday": 0,
        "Monday": 1,
        "Tuesday": 2,
        "Wednesday": 3,
        "Thursday": 4,
        "Friday": 5,
        "Saturday": 6,
    }
 
    # Initialize dispatch count for all 7 days
    result = [0] * 7
 
    for day_name, count in rows:
        index = week_days_map.get(day_name)
        if index is not None:
            result[index] = count
 
    return JsonResponse({
        "salesman_name": salesman_name,
        "weekly_dispatches": result  # [Sun, Mon, Tue, ..., Sat]
    })

# For Dashboard - Chart Manager

def get_weekly_delivered_count(request, warehouse_name):
    warehouse_name = unquote(warehouse_name)
 
    # Get current week's Sunday and Saturday
    today = datetime.today()
    start_of_week = today - timedelta(days=today.weekday() + 1) if today.weekday() != 6 else today
    end_of_week = start_of_week + timedelta(days=6)
 
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT
                DATENAME(WEEKDAY, DATE) AS day_name,
                COUNT(DISTINCT DISPATCH_ID) AS delivered_count
            FROM BUYP.dbo.WHR_TRUCK_SCAN_DETAILS
            WHERE
                ORG_NAME = %s AND
                CAST(DATE AS DATE) BETWEEN %s AND %s
            GROUP BY DATENAME(WEEKDAY, DATE)
        """, [warehouse_name, start_of_week.date(), end_of_week.date()])
 
        rows = cursor.fetchall()
 
    # Map: Sunday=0, ..., Saturday=6
    week_days_map = {
        "Sunday": 0,
        "Monday": 1,
        "Tuesday": 2,
        "Wednesday": 3,
        "Thursday": 4,
        "Friday": 5,
        "Saturday": 6,
    }
 
    # Initialize all 7 days with 0
    result = [0] * 7
 
    for day_name, count in rows:
        index = week_days_map.get(day_name)
        if index is not None:
            result[index] = count
 
    return JsonResponse({
        "warehouse_name": warehouse_name,
        "weekly_delivered_invoices": result  # [Sun, Mon, Tue, ..., Sat]
    })

# For Dashboard - Chart Pickman
 
def get_weekly_picked_count(request, pickman_name):
    pickman_name = unquote(pickman_name)
 
    # Get current week's Sunday and Saturday
    today = datetime.today()
    start_of_week = today - timedelta(days=today.weekday() + 1) if today.weekday() != 6 else today
    end_of_week = start_of_week + timedelta(days=6)
 
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT
                DATENAME(WEEKDAY, DATE) AS day_name,
                COUNT(DISTINCT PICK_ID) AS picked_count
            FROM BUYP.dbo.WHR_PICKED_MAN
            WHERE
                PICKMAN_NAME = %s AND
                CAST(DATE AS DATE) BETWEEN %s AND %s
            GROUP BY DATENAME(WEEKDAY, DATE)
        """, [pickman_name, start_of_week.date(), end_of_week.date()])
 
        rows = cursor.fetchall()
 
    # Map: Sunday=0, ..., Saturday=6
    week_days_map = {
        "Sunday": 0,
        "Monday": 1,
        "Tuesday": 2,
        "Wednesday": 3,
        "Thursday": 4,
        "Friday": 5,
        "Saturday": 6,
    }
 
    # Initialize all 7 days with 0
    result = [0] * 7
 
    for day_name, count in rows:
        index = week_days_map.get(day_name)
        if index is not None:
            result[index] = count
 
    return JsonResponse({
        "pickman_name": pickman_name,
        "weekly_picked": result  # [Sun, Mon, Tue, ..., Sat]
    })

def picked_and_truck_count_view(request):
    req_id = request.GET.get('req_id')
    
    invoice_number = request.GET.get('invoice_number')
    customer_number = request.GET.get('customer_number')
    customer_site_id = request.GET.get('customer_site_id')
    
    inventory_item_id = request.GET.get('inventory_item_id')  # corresponds to ITEM_CODE in WHR_RETURN_DISPATCH

    if not all([req_id, invoice_number, customer_number, customer_site_id, inventory_item_id]):
        return JsonResponse({'error': 'Missing required parameters'}, status=400)

    with connection.cursor() as cursor:
        # Query 1: total picked qty from WHR_PICKED_MAN
        cursor.execute("""
            SELECT SUM(ISNULL(PICKED_QTY, 0)) AS total_picked_qty
            FROM [BUYP].[dbo].[WHR_PICKED_MAN]
            WHERE [REQ_ID] = %s 
                       
              AND [INVOICE_NUMBER] = %s 
              AND [CUSTOMER_NUMBER] = %s 
              AND [CUSTOMER_SITE_ID] = %s 
              AND [INVENTORY_ITEM_ID] = %s
              AND [FLAG] != 'R'AND [FLAG] != 'SR'
        """, [req_id, invoice_number, customer_number, customer_site_id, inventory_item_id])
        picked_row = cursor.fetchone()

        # Query 2: total truck qty from WHR_TRUCK_SCAN_DETAILS
        cursor.execute("""
            SELECT SUM(ISNULL(TRUCK_SEND_QTY, 0)) AS total_truck_qty
            FROM [BUYP].[dbo].[WHR_TRUCK_SCAN_DETAILS]
            WHERE [REQ_ID] = %s 
              AND [INVOICE_NO] = %s 
              AND [CUSTOMER_NUMBER] = %s 
              AND [CUSTOMER_SITE_ID] = %s 
              AND [ITEM_CODE] = %s
              AND [FLAG] != 'R' AND [FLAG] != 'SR'
        """, [req_id, invoice_number, customer_number, customer_site_id, inventory_item_id])
        truck_row = cursor.fetchone()

        # Query 3: total count of rows from WHR_RETURN_DISPATCH
        cursor.execute("""
            SELECT COUNT(*) 
            FROM [BUYP].[dbo].[WHR_RETURN_DISPATCH]
            WHERE [REQ_ID] = %s
              AND [INVOICE_NO] = %s
              AND [CUSTOMER_NUMBER] = %s
              AND [CUSTOMER_SITE_ID] = %s
              AND [ITEM_CODE] = %s
              AND [RE_ASSIGN_STATUS] != 'Re-Assign-Finished'
        """, [req_id, invoice_number, customer_number, customer_site_id, inventory_item_id])
        count_row = cursor.fetchone()

    result = {
        'total_picked_qty': int(picked_row[0]) if picked_row[0] is not None else 0,
        'total_truck_qty': int(truck_row[0]) if truck_row[0] is not None else 0,
        'total_return_qty': int(count_row[0]) if count_row[0] is not None else 0,
    }

    return JsonResponse(result)

@csrf_exempt
def add_transaction_detail(request):
    if request.method == 'POST':
        try:
            data = json.loads(request.body)

            with connection.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO WHR_TRANSACTION_DETAILS (
                        UNDEL_ID, TRANSACTION_DATE, CUSTOMER_TRX_ID, CUSTOMER_TRX_LINE_ID, ITEM_ID, LINE_NO,
                        QTY, SOURCE, TRANSACTION_TYPE, DISPATCH_ID, REFERENCE1, REFERENCE2, REFERENCE3,
                        REFERENCE4, CREATION_DATE, CREATED_BY, CREATED_IP, CREATED_MAC, LAST_UPDATE_DATE,
                        LAST_UPDATED_BY, LAST_UPDATE_IP, LAST_UPDATE_MAC, FLAG
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s
                    )
                """, [
                    data.get("UNDEL_ID"),
                    data.get("TRANSACTION_DATE"),
                    data.get("CUSTOMER_TRX_ID"),
                    data.get("CUSTOMER_TRX_LINE_ID"),
                    data.get("ITEM_ID"),
                    data.get("LINE_NO"),
                    data.get("QTY"),
                    data.get("SOURCE"),
                    data.get("TRANSACTION_TYPE"),
                    data.get("DISPATCH_ID"),
                    data.get("REFERENCE1"),
                    data.get("REFERENCE2"),
                    data.get("REFERENCE3"),
                    data.get("REFERENCE4"),
                    datetime.now(),  # CREATION_DATE
                    data.get("CREATED_BY"),
                    data.get("CREATED_IP"),
                    data.get("CREATED_MAC"),
                    datetime.now(),  # LAST_UPDATE_DATE
                    data.get("LAST_UPDATED_BY"),
                    data.get("LAST_UPDATE_IP"),
                    data.get("LAST_UPDATE_MAC"),
                    data.get("FLAG")
                ])

            return JsonResponse({'status': 'success', 'message': 'Transaction detail added successfully'})

        except Exception as e:
            return JsonResponse({'status': 'error', 'message': str(e)})

    return JsonResponse({'status': 'error', 'message': 'Only POST requests allowed'})

def check_product_serial_duplicate(request):
    product_code = request.GET.get('product_code')
    serial_no = request.GET.get('serial_no')

    if not product_code or not serial_no:
        return JsonResponse({'error': 'Missing product_code or serial_no'}, status=400)

    query_1 = """
        SELECT COUNT(*) 
        FROM [BUYP].[dbo].[WHR_PICKED_MAN]
        WHERE [PRODUCT_CODE] = %s AND [SERIAL_NO] = %s
    """
    query_2 = """
        SELECT COUNT(*) 
        FROM [BUYP].[dbo].[WHR_TRUCK_SCAN_DETAILS]
        WHERE [PRODUCT_CODE] = %s AND [SERIAL_NO] = %s
    """

    with connection.cursor() as cursor:
        # Check in WHR_PICKED_MAN
        cursor.execute(query_1, [product_code, serial_no])
        picked_count = cursor.fetchone()[0]

        # Check in WHR_TRUCK_SCAN_DETAILS
        cursor.execute(query_2, [product_code, serial_no])
        scan_count = cursor.fetchone()[0]

    # If found in either table
    is_duplicate = picked_count > 0 or scan_count > 0

    return JsonResponse({'is_duplicate': is_duplicate})

@csrf_exempt
def check_serial_and_fetch_data(request):
    if request.method != 'POST':
        return JsonResponse({'error': 'Only POST method is allowed'}, status=405)

    try:
        body = json.loads(request.body.decode('utf-8'))
        reqno = body.get('reqno')
        pickid = body.get('pickid')
        productcode = body.get('productcode')
        serialno = body.get('serialno')

        if not all([reqno, pickid, productcode, serialno]):
            return JsonResponse({'error': 'Missing required fields'}, status=400)

        # 1. Check in WHR_TRUCK_SCAN_DETAILS
        check_query = """
            SELECT 1 
            FROM [BUYP].[dbo].[WHR_TRUCK_SCAN_DETAILS]
            WHERE [REQ_ID] = %s AND [PICK_ID] = %s AND [PRODUCT_CODE] = %s AND [SERIAL_NO] = %s
        """

        with connection.cursor() as cursor:
            cursor.execute(check_query, [reqno, pickid, productcode, serialno])
            result = cursor.fetchone()

            if result:
                return JsonResponse({
                    'message': 'This Product Code and Serial No are already being tracked.',
                    'already_tracked': True
                })

        # 2. If not found, check in WHR_PICKED_MAN and return matching rows
        fetch_query = """
            SELECT *
            FROM [BUYP].[dbo].[WHR_PICKED_MAN]
            WHERE [REQ_ID] = %s AND [PICK_ID] = %s AND [PRODUCT_CODE] = %s AND [SERIAL_NO] = %s
        """

        with connection.cursor() as cursor:
            cursor.execute(fetch_query, [reqno, pickid, productcode, serialno])
            columns = [col[0] for col in cursor.description]
            rows = cursor.fetchall()

            data = [dict(zip(columns, row)) for row in rows]

        if not data:
            return JsonResponse({'message': 'No matching data found for the entered product code and serial number in Pickman_scan.', 'data': []})
        else:
            return JsonResponse({'message': 'Data retrieved successfully.', 'data': data})

    except json.JSONDecodeError:
        return JsonResponse({'error': 'Invalid JSON'}, status=400)
    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)

@csrf_exempt
def update_Oracle_dispatch_flag(request, reqno):
    if request.method == 'POST':
        try:
            with connection.cursor() as cursor:
                # Update FLAG in WHR_CREATE_DISPATCH and set TRUCK_SCAN_QTY = 0
                cursor.execute("""
                    UPDATE WHR_CREATE_DISPATCH
                    SET FLAG = 'OU', TRUCK_SCAN_QTY = 0
                    WHERE REQ_ID = %s
                """, [reqno])

                # Update FLAG in WHR_DISPATCH_REQUEST
                cursor.execute("""
                    UPDATE WHR_DISPATCH_REQUEST
                    SET FLAG = 'OU'
                    WHERE REQ_ID = %s
                """, [reqno])

                # Update FLAG in WHR_PICKED_MAN
                cursor.execute("""
                    UPDATE WHR_PICKED_MAN
                    SET FLAG = 'OU'
                    WHERE REQ_ID = %s
                """, [reqno])

            return JsonResponse({'status': 'success', 'message': f'Updated records for REQ_ID = {reqno}'})
        except Exception as e:
            return JsonResponse({'status': 'error', 'message': str(e)}, status=500)
    else:
        return JsonResponse({'status': 'error', 'message': 'Only POST method allowed'}, status=405)
 

@csrf_exempt
def Reverse_update_Oracle_dispatch_flag(request, reqno):
    if request.method == 'POST':
        try:
            with connection.cursor() as cursor:
                # Set FLAG = 'A' and TRUCK_SCAN_QTY = DISPATCHED_QTY in WHR_CREATE_DISPATCH
                cursor.execute("""
                    UPDATE WHR_CREATE_DISPATCH
                    SET FLAG = 'A',
                        TRUCK_SCAN_QTY = DISPATCHED_QTY
                    WHERE REQ_ID = %s
                """, [reqno])

                # Set FLAG = 'A' in WHR_DISPATCH_REQUEST
                cursor.execute("""
                    UPDATE WHR_DISPATCH_REQUEST
                    SET FLAG = 'A'
                    WHERE REQ_ID = %s
                """, [reqno])

                # Set FLAG = 'A' in WHR_PICKED_MAN
                cursor.execute("""
                    UPDATE WHR_PICKED_MAN
                    SET FLAG = 'A'
                    WHERE REQ_ID = %s
                """, [reqno])

            return JsonResponse({'status': 'success', 'message': f'Updated records for REQ_ID = {reqno}'})
        except Exception as e:
            return JsonResponse({'status': 'error', 'message': str(e)}, status=500)
    else:
        return JsonResponse({'status': 'error', 'message': 'Only POST method allowed'}, status=405)
    
# Version
 
class APIVersionView(APIView):
    def get(self, request):
        return Response({
            "version": API_VERSION,
            "date": RELEASE_DATE
        })
    
# Monitoring views 

from django.shortcuts import render, redirect, get_object_or_404
from django.core.paginator import Paginator
from django.utils.dateparse import parse_datetime
from django.db.models import Q
from django.urls import resolve, Resolver404
from django.views.decorators.csrf import csrf_exempt
from django.utils.timezone import make_aware
import pytz

# from .models import RequestLog

# def log_view(request):
#     start = request.GET.get('start')
#     filters = Q()

#     if start:
#         start_dt = parse_datetime(start)
#         if start_dt:
#             saudi_tz = pytz.timezone('Asia/Riyadh')
#             aware_start = make_aware(start_dt, timezone=saudi_tz)
#             filters &= Q(timestamp__gte=aware_start)

#     log_list = RequestLog.objects.filter(filters).order_by('-timestamp')

#     paginator = Paginator(log_list, 50)
#     page_number = request.GET.get('page')
#     page_obj = paginator.get_page(page_number)

#     total_requests = log_list.count()
#     unique_ips = log_list.values_list('ip', flat=True).distinct().count()

#     # Count most accessed views
#     path_counts = {}
#     for log in log_list:
#         try:
#             view_name = resolve(log.path).view_name or resolve(log.path).url_name
#         except Resolver404:
#             view_name = "Unknown"
#         path_counts[view_name] = path_counts.get(view_name, 0) + 1

#     sorted_views = sorted(path_counts.items(), key=lambda x: x[1], reverse=True)[:10]

#     return render(request, 'log_monitor.html', {
#         'page_obj': page_obj,
#         'total_requests': total_requests,
#         'unique_ips': unique_ips,
#         'sorted_views': sorted_views,
#         'start': start or "",
#     })

# @csrf_exempt
# def delete_filtered_logs(request):
#     if request.method == 'POST':
#         start = request.POST.get('start')
#         if start:
#             start_dt = parse_datetime(start)
#             if start_dt:
#                 saudi_tz = pytz.timezone('Asia/Riyadh')
#                 aware_start = make_aware(start_dt, timezone=saudi_tz)
#                 RequestLog.objects.filter(timestamp__gte=aware_start).delete()
#     return redirect('log_monitor')

# @csrf_exempt
# def delete_all_logs(request):
#     if request.method == 'POST':
#         RequestLog.objects.all().delete()
#     return redirect('log_monitor')

# def log_detail(request, pk):
#     req = get_object_or_404(RequestLog, pk=pk)
#     return render(request, 'log_.html', {'req': req})
    
    
    

# 🔹 Helper: Fetch Dispatch Request Data
def get_filtered_dispatch_data(req_id, warehouse_name):
    queryset = WHRCreateDispatch.objects.filter(
        REQ_ID=req_id,
        PHYSICAL_WAREHOUSE__iexact=warehouse_name
    ).exclude(FLAG='D')

    result = {}
    for record in queryset:
        if record.REQ_ID not in result:
            result[record.REQ_ID] = {
                "REQ_ID": record.REQ_ID,
                "TO_WAREHOUSE": record.PHYSICAL_WAREHOUSE,
                "ORG_ID": record.ORG_ID,
                "ORG_NAME": record.ORG_NAME,
                "SALESMAN_NO": record.SALESMAN_NO,
                "SALESMAN_NAME": record.SALESMAN_NAME,
                "CUSTOMER_NUMBER": record.CUSTOMER_NUMBER,
                "CUSTOMER_NAME": record.CUSTOMER_NAME,
                "CUSTOMER_SITE_ID": record.CUSTOMER_SITE_ID,
                "INVOICE_DATE": record.INVOICE_DATE,
                "INVOICE_NUMBER": record.INVOICE_NUMBER,
                "DELIVERY_DATE": record.DELIVERY_DATE,
                "DELIVERYADDRESS": record.DELIVERYADDRESS,
                "TABLE_DETAILS": []
            }

        detail = {
            "ID": record.id,
            "UNDEL_ID": record.UNDEL_ID,
            "INVOICE_NUMBER": record.INVOICE_NUMBER,
            "CUSTOMER_TRX_ID": record.CUSTOMER_TRX_ID,
            "CUSTOMER_TRX_LINE_ID": record.CUSTOMER_TRX_LINE_ID,
            "LINE_NUMBER": record.LINE_NUMBER,
            "INVENTORY_ITEM_ID": record.INVENTORY_ITEM_ID,
            "ITEM_DESCRIPTION": record.ITEM_DESCRIPTION,
            "TOT_QUANTITY": record.TOT_QUANTITY,
            "DISPATCHED_QTY": record.DISPATCHED_QTY,
            "DISPATCHED_BY_MANAGER": record.DISPATCHED_BY_MANAGER,
            "BALANCE_QTY": record.BALANCE_QTY,
        }
        result[record.REQ_ID]["TABLE_DETAILS"].append(detail)

    return list(result.values())


# 🔹 Helper: Generate Quick Bill ID
def generate_quickbill_id(request):
    token = get_token(request)
    now = datetime.now()
    prefix = f"QKB{str(now.year)[-2:]}{now.month:02d}"

    last_id = QUICK_BILL_ID_Models.objects.filter(
        QUICK_BILL_ID__startswith=prefix
    ).order_by("-id").first()

    if last_id:
        match = re.match(rf"{prefix}(\d+)$", last_id.QUICK_BILL_ID)
        next_number = int(match.group(1)) + 1 if match else 1
    else:
        next_number = 1

    new_id = f"{prefix}{next_number}"
    QUICK_BILL_ID_Models.objects.create(QUICK_BILL_ID=new_id, TOCKEN=token)

    return new_id, token


# # 🔹 Main Save Dispatch Request API
# @csrf_exempt
# def save_dispatch_request(request):
#     if request.method != "POST":
#         return JsonResponse({"status": "error", "message": "Only POST allowed"}, status=405)

#     try:
#         body = json.loads(request.body.decode("utf-8"))
#         req_id = body.get("REQ_ID")
#         warehouse_name = body.get("TO_WAREHOUSE")
#         manager_no = body.get("MANAGER_NO")
#         manager_name = body.get("MANAGER_NAME")

#         if not req_id or not warehouse_name:
#             return JsonResponse({"status": "error", "message": "REQ_ID and TO_WAREHOUSE required"}, status=400)

#         # Get Dispatch Data
#         dispatch_data = get_filtered_dispatch_data(req_id, warehouse_name)
#         if not dispatch_data:
#             return JsonResponse({"status": "error", "message": "No data found"}, status=404)

#         delivery_id = 1
#         response_summary = []

#         with connection.cursor() as cursor, transaction.atomic():
#             for record in dispatch_data:
#                 req_no = record.get("REQ_ID")
#                 salesman_no = record.get("SALESMAN_NO")
#                 salesman_name = record.get("SALESMAN_NAME")
#                 customer_no = record.get("CUSTOMER_NUMBER")
#                 customer_name = record.get("CUSTOMER_NAME")
#                 customer_site = record.get("CUSTOMER_SITE_ID")

#                 total_pick_qty = 0
#                 pick_id = None

#                 for detail in record.get("TABLE_DETAILS", []):
#                     dispatched_qty = int(detail.get("DISPATCHED_BY_MANAGER", 0))
#                     total_pick_qty += dispatched_qty

#                     invoice_no = detail.get("INVOICE_NUMBER")
#                     item_code = detail.get("INVENTORY_ITEM_ID")
#                     line_no = detail.get("LINE_NUMBER")

#                     # Get Barcode
#                     cursor.execute("""
#                         SELECT TOP 1 ISNULL(PRODUCT_BARCODE,'00')
#                         FROM [BUYP].[BUYP].[ALJE_ITEM_CATEGORIES_CPD_V]
#                         WHERE ITEM_CODE = %s
#                     """, [item_code])
#                     prod_result = cursor.fetchone()
#                     product_code = prod_result[0] if prod_result else "00"

#                     # Existing rows check
#                     cursor.execute("""
#                         SELECT TOP 1 pick_id, COUNT(*) 
#                         FROM [BUYP].[dbo].[WHR_SAVE_TRUCK_DETAILS_TBL]
#                         WHERE dispatch_id=%s AND req_no=%s AND invoice_no=%s
#                           AND Item_code=%s AND line_no=%s
#                         GROUP BY pick_id
#                     """, [delivery_id, req_no, invoice_no, item_code, line_no])
#                     result = cursor.fetchone()

#                     existing_count = result[1] if result else 0
#                     existing_pick_id = result[0] if result else None

#                     if existing_count >= dispatched_qty:
#                         if not pick_id:
#                             pick_id = existing_pick_id
#                         continue

#                     if not pick_id:
#                         pick_id, _ = generate_quickbill_id(request)

#                     rows_to_add = dispatched_qty - existing_count
#                     for _ in range(rows_to_add):
#                         cursor.execute("""
#                             INSERT INTO [BUYP].[dbo].[WHR_SAVE_TRUCK_DETAILS_TBL] (
#                                 dispatch_id, req_no, pick_id,
#                                 salesman_no, salesman_name,
#                                 Customer_no, Customer_name, Customer_Site,
#                                 invoice_no, Customer_trx_id, Customer_trx_line_id, line_no,
#                                 Item_code, Item_detailas, DisReq_Qty, Send_qty,
#                                 Product_code, Serial_No, Udel_id, SCAN_STATUS,
#                                 manager_no, manager_name, loadman_no, loadman_name
#                             )
#                             VALUES (
#                                 %s, %s, %s,
#                                 %s, %s,
#                                 %s, %s, %s,
#                                 %s, %s, %s, %s,
#                                 %s, %s, %s, 1,
#                                 %s, 'QBNSA', %s, 'Request for Delivery',  %s, %s,
#                                 %s, %s
#                             )
#                         """, [
#                             delivery_id, req_no, pick_id,
#                             salesman_no, salesman_name,
#                             customer_no, customer_name, customer_site,
#                             invoice_no, detail.get("CUSTOMER_TRX_ID"),
#                             detail.get("CUSTOMER_TRX_LINE_ID"), line_no,
#                             item_code, detail.get("ITEM_DESCRIPTION"),
#                             detail.get("DISPATCHED_QTY"),
#                             product_code, detail.get("UNDEL_ID"),
#                             manager_no, manager_name,manager_no, manager_name
#                         ])

#                 response_summary.append({
#                     "req_no": req_no,
#                     "pick_id": pick_id,
#                     "customer_no": customer_no,
#                     "customer_name": customer_name,
#                     "customer_site": customer_site,
#                     "pick_qty": total_pick_qty,
#                 })

#             cursor.execute("""
#                 UPDATE [BUYP].[dbo].[WHR_CREATE_DISPATCH]
#                 SET DISPATCHED_BY_MANAGER = 0,
#                     LAST_UPDATED_BY = %s,
#                     COMMERCIAL_NO = %s
#                 WHERE REQ_ID = %s
#             """, [manager_name, manager_no, req_id])

#         return JsonResponse({
#             "status": "success",
#             "message": "Data saved successfully",
#             "data": response_summary
#         })

#     except Exception as e:
#         return JsonResponse({"status": "error", "message": str(e)}, status=500)

# 🔹 Main Save Dispatch Request API

@csrf_exempt
def save_dispatch_request(request):
    if request.method != "POST":
        return JsonResponse({"status": "error", "message": "Only POST allowed"}, status=405)

    try:
        body = json.loads(request.body.decode("utf-8"))

        req_id = body.get("REQ_ID")
        warehouse_name = body.get("TO_WAREHOUSE")
        manager_no = body.get("MANAGER_NO")
        manager_name = body.get("MANAGER_NAME")
        tabledatas = body.get("tabledatas", [])

        if not req_id or not warehouse_name:
            return JsonResponse({"status": "error", "message": "REQ_ID and TO_WAREHOUSE required"}, status=400)

        if not tabledatas:
            return JsonResponse({"status": "error", "message": "tabledatas is empty"}, status=400)

        delivery_id = 1
        total_pick_qty = 0
        pick_id, _ = generate_quickbill_id(request)

        first_row = tabledatas[0]
        customer_no = first_row.get("CUSTOMER_NUMBER")
        customer_name = first_row.get("CUSTOMER_NAME")
        customer_site = first_row.get("CUSTOMER_SITE_ID")

        # =============================================================
        # PROCESS INSIDE TRANSACTION
        # =============================================================
        with transaction.atomic():
            with connection.cursor() as cursor:

                for row in tabledatas:

                    undel_id = row.get("UNDEL_ID")
                    scan_qty = int(row.get("SCAN_QTY", 0))

                    if scan_qty <= 0:
                        continue

                    total_pick_qty += scan_qty

                    invoice_no = row.get("INVOICE_NUMBER")
                    item_code = row.get("INVENTORY_ITEM_ID")
                    line_no = row.get("LINE_NUMBER")
                    salesman_no = row.get("SALESMAN_NO")
                    salesman_name = row.get("SALESMAN_NAME")
                    customer_no = row.get("CUSTOMER_NUMBER")
                    customer_name = row.get("CUSTOMER_NAME")
                    customer_site = row.get("CUSTOMER_SITE_ID")
                    customer_trx_id = row.get("CUSTOMER_TRX_ID")
                    customer_trx_line_id = row.get("CUSTOMER_TRX_LINE_ID")
                    item_desc = row.get("ITEM_DESCRIPTION")
                    dispatched_qty = row.get("DISPATCHED_QTY")

                    # =============================================================
                    # STEP-1 → VALIDATION BEFORE INSERT
                    # =============================================================
                    cursor.execute("""
                        SELECT 
                            d.DISPATCHED_QTY,
                            d.TRUCK_SCAN_QTY,

                            -- FINAL ASSIGNED = SUM PICKED − TRUCK COUNT
                            (
                                SELECT COALESCE(SUM(r.PICKED_QTY), 0)
                                FROM WHR_DISPATCH_REQUEST r
                                WHERE r.REQ_ID = d.REQ_ID AND r.UNDEL_ID = d.UNDEL_ID
                            ) -
                           (
                                SELECT COALESCE(COUNT(*), 0)
                                FROM WHR_TRUCK_SCAN_DETAILS t
                                WHERE t.REQ_ID = d.REQ_ID 
                                AND t.UNDEL_ID = d.UNDEL_ID
                                AND t.PICK_ID IN (
                                        SELECT r2.PICK_ID
                                        FROM WHR_DISPATCH_REQUEST r2
                                        WHERE r2.REQ_ID = d.REQ_ID
                                        AND r2.UNDEL_ID = d.UNDEL_ID
                                )
                            ) AS ASSIGNED_QTY,

                            -- STAGING = saved rows NOT IN dispatch request
                            (
                                SELECT COALESCE(COUNT(*), 0)
                                FROM WHR_SAVE_TRUCK_DETAILS_TBL s
                                WHERE s.req_no = d.REQ_ID
                                  AND s.Udel_id = d.UNDEL_ID
                                  AND s.PICK_ID NOT IN (
                                        SELECT r3.PICK_ID
                                        FROM WHR_DISPATCH_REQUEST r3
                                        WHERE r3.REQ_ID = d.REQ_ID
                                          AND r3.UNDEL_ID = d.UNDEL_ID
                                  )
                            ) AS STAGING_QTY

                        FROM WHR_CREATE_DISPATCH d
                        WHERE d.REQ_ID = %s AND d.UNDEL_ID = %s
                    """, [req_id, undel_id])

                    check = cursor.fetchone()
                    if not check:
                        return JsonResponse({"status": "error", "message": "Invalid REQ_ID or UNDEL_ID"}, status=400)

                    dispatch_qty = check[0]
                    truck_qty = check[1]
                    assigned_qty = check[2]
                    staging_qty = check[3]

                    final_truck_qty = dispatch_qty - truck_qty
                    available_qty = dispatch_qty - (final_truck_qty + assigned_qty + staging_qty)

                    if scan_qty > available_qty:
                        return JsonResponse({
                            "status": "error",
                            "message": f"Scan qty {scan_qty} exceeds available qty {available_qty}"
                        })

                    # =============================================================
                    # STEP-2 → FETCH PRODUCT BARCODE
                    # =============================================================
                    cursor.execute("""
                        SELECT TOP 1 ISNULL(PRODUCT_BARCODE, '00')
                        FROM BUYP.BUYP.ALJE_ITEM_CATEGORIES_CPD_V
                        WHERE ITEM_CODE = %s
                    """, [item_code])

                    prod = cursor.fetchone()
                    product_code = prod[0] if prod else "00"

                    # =============================================================
                    # STEP-3 → INSERT TRUCK DETAILS
                    # =============================================================
                    for _ in range(scan_qty):
                        cursor.execute("""
                            INSERT INTO WHR_SAVE_TRUCK_DETAILS_TBL (
                                dispatch_id, req_no, pick_id,
                                salesman_no, salesman_name,
                                Customer_no, Customer_name, Customer_Site,
                                invoice_no, Customer_trx_id, Customer_trx_line_id, line_no,
                                Item_code, Item_detailas, DisReq_Qty, Send_qty,
                                Product_code, Serial_No, Udel_id, SCAN_STATUS,
                                manager_no, manager_name, loadman_no, loadman_name
                            )
                            VALUES (
                                %s, %s, %s,
                                %s, %s,
                                %s, %s, %s,
                                %s, %s, %s, %s,
                                %s, %s, %s, 1,
                                %s, 'QBNSA', %s, 'Request for Delivery',
                                %s, %s, %s, %s
                            )
                        """, [
                            delivery_id, req_id, pick_id,
                            salesman_no, salesman_name,
                            customer_no, customer_name, customer_site,
                            invoice_no, customer_trx_id, customer_trx_line_id, line_no,
                            item_code, item_desc, dispatched_qty,
                            product_code, undel_id,
                            manager_no, manager_name,
                            manager_no, manager_name
                        ])

                # =============================================================
                # STEP-4 → UPDATE DISPATCHED BY MANAGER (FINAL BALANCE)
                # =============================================================
                cursor.execute("""
                    SELECT
                        d.UNDEL_ID,
                        d.DISPATCHED_QTY,
                        d.TRUCK_SCAN_QTY,

                        -- FINAL ASSIGNED
                        (
                            SELECT COALESCE(SUM(r.PICKED_QTY), 0)
                            FROM WHR_DISPATCH_REQUEST r
                            WHERE r.REQ_ID = d.REQ_ID AND r.UNDEL_ID = d.UNDEL_ID
                        ) -
                        (
                            SELECT COALESCE(COUNT(*), 0)
                            FROM WHR_TRUCK_SCAN_DETAILS t
                            WHERE t.REQ_ID = d.REQ_ID 
                            AND t.UNDEL_ID = d.UNDEL_ID
                            AND t.PICK_ID IN (
                                    SELECT r2.PICK_ID
                                    FROM WHR_DISPATCH_REQUEST r2
                                    WHERE r2.REQ_ID = d.REQ_ID
                                    AND r2.UNDEL_ID = d.UNDEL_ID
                            )
                        ) AS AssignedQty,

                        -- STAGING
                        (
                            SELECT COALESCE(COUNT(*), 0)
                            FROM WHR_SAVE_TRUCK_DETAILS_TBL s
                            WHERE s.req_no = d.REQ_ID
                              AND s.Udel_id = d.UNDEL_ID
                              AND s.PICK_ID NOT IN (
                                    SELECT r3.PICK_ID
                                    FROM WHR_DISPATCH_REQUEST r3
                                    WHERE r3.REQ_ID = d.REQ_ID AND r3.UNDEL_ID = d.UNDEL_ID
                              )
                        ) AS StagingQty

                    FROM WHR_CREATE_DISPATCH d
                    WHERE d.REQ_ID = %s
                """, [req_id])

                update_rows = cursor.fetchall()

                for row in update_rows:
                    undel = row[0]
                    disp_qty = row[1]
                    truck_scan = row[2]
                    assigned = row[3]
                    staged = row[4]

                    final_truck = disp_qty - truck_scan
                    updated_balance = disp_qty - (final_truck + assigned + staged)
                    updated_balance = max(0, updated_balance)

                    cursor.execute("""
                        UPDATE WHR_CREATE_DISPATCH
                        SET DISPATCHED_BY_MANAGER = %s,
                            LAST_UPDATED_BY = %s,
                            COMMERCIAL_NO = %s
                        WHERE REQ_ID = %s AND UNDEL_ID = %s
                    """, [updated_balance, manager_name, manager_no, req_id, undel])

        # =============================================================
        # FINAL SUCCESS RESPONSE
        # =============================================================
        return JsonResponse({
            "status": "success",
            "message": "Data saved successfully",
            "req_no": req_id,
            "pick_id": pick_id,
            "customer_no": customer_no,
            "customer_name": customer_name,
            "customer_site": customer_site,
            "pick_qty": total_pick_qty
        })

    except Exception as e:
        return JsonResponse({"status": "error", "message": str(e)}, status=500)

    
import math
import json
from collections import defaultdict
from datetime import datetime
from django.db import transaction, connections

@csrf_exempt
def truck_scan_view(request):
    if request.method != "POST":
        return JsonResponse({"error": "Only POST allowed"}, status=405)

    try:
        data = json.loads(request.body)

        # ---------------- HEADER FIELDS ----------------
        dispatch_id       = data.get("DISPATCH_ID")
        req_id            = data.get("REQ_ID")
        date              = data.get("DATE")
        warehouse         = data.get("PHYSICAL_WAREHOUSE")
        org_id            = data.get("ORG_ID")
        org_name          = data.get("ORG_NAME")
        staff_no          = data.get("STAFF_NO")
        staff_name        = data.get("STAFF_NAME")
        customer_number   = data.get("CUSTOMER_NUMBER")
        customer_name     = data.get("CUSTOMER_NAME")
        customer_site_id  = data.get("CUSTOMER_SITE_ID")
        transporter_name  = data.get("TRANSPORTER_NAME")
        driver_name       = data.get("DRIVER_NAME")
        driver_mobileno   = data.get("DRIVER_MOBILENO")
        vehicle_no        = data.get("VEHICLE_NO")
        truck_dimension   = data.get("TRUCK_DIMENSION")
        loading_charges   = data.get("LOADING_CHARGES")
        transport_charges = data.get("TRANSPORT_CHARGES")
        misc_charges      = data.get("MISC_CHARGES")
        deliveryaddress   = data.get("DELIVERYADDRESS")
        salesmanremarks   = data.get("SALESMANREMARKS")
        remarks           = data.get("REMARKS")
        creation_date     = data.get("CREATION_DATE")
        created_by        = data.get("CREATED_BY")
        created_ip        = data.get("CREATED_IP")
        created_mac       = data.get("CREATED_MAC")
        last_update_date  = data.get("LAST_UPDATE_DATE")
        last_updated_by   = data.get("LAST_UPDATED_BY")
        last_update_ip    = data.get("LAST_UPDATE_IP")
        flag              = data.get("FLAG")
        delivery_date     = data.get("DELIVERY_DATE")

        tabledata = data.get("tabledata", [])
        inserted = 0
        undel_summary = defaultdict(int)
        update_summary = defaultdict(int)

        # ---------------- INSERT SQL ----------------
        insert_sql = """
            INSERT INTO [BUYP].[dbo].[WHR_TRUCK_SCAN_DETAILS] (
                DISPATCH_ID, REQ_ID, PICK_ID, DATE, PHYSICAL_WAREHOUSE,
                ORG_ID, ORG_NAME, SALESMAN_NO, SALESMAN_NAME,
                MANAGER_NO, MANAGER_NAME, PICKMAN_NO, PICKMAN_NAME,
                STAFF_NO, STAFF_NAME, CUSTOMER_NUMBER, CUSTOMER_NAME,
                CUSTOMER_SITE_ID, LINE_NO, TRANSPORTER_NAME,
                DRIVER_NAME, DRIVER_MOBILENO, VEHICLE_NO, TRUCK_DIMENSION,
                LOADING_CHARGES, TRANSPORT_CHARGES, MISC_CHARGES,
                DELIVERYADDRESS, SALESMANREMARKS, REMARKS, INVOICE_NO,
                CUSTOMER_TRX_ID, CUSTOMER_TRX_LINE_ID, ITEM_CODE, ITEM_DETAILS,
                PRODUCT_CODE, SERIAL_NO, DISREQ_QTY, BALANCE_QTY, TRUCK_SEND_QTY,
                CREATION_DATE, CREATED_BY, CREATED_IP, CREATED_MAC,
                LAST_UPDATE_DATE, LAST_UPDATED_BY, LAST_UPDATE_IP, FLAG,
                DELIVERY_DATE, UNDEL_ID, ATTRIBUTE1
            ) VALUES (
                %s,%s,%s,%s,%s,
                %s,%s,%s,%s,
                %s,%s,%s,%s,
                %s,%s,%s,%s,
                %s,%s,%s,
                %s,%s,%s,%s,
                %s,%s,%s,
                %s,%s,%s,%s,
                %s,%s,%s,%s,
                %s,%s,%s,%s,%s,
                %s,%s,%s,%s,
                %s,%s,%s,%s,
                %s,%s,%s
            )
        """

        CHUNK_SIZE = 1000
        params_chunk = []
        save_deletes = []

        # -----------------------------------------------------
        # ✅ GET EXISTING ATTRIBUTE1 FROM DB FIRST
        # -----------------------------------------------------
        incoming_attrs = [str(r.get("id")) for r in tabledata if r.get("id")]
        existing_attrs = set()

        if incoming_attrs:
            with connections["default"].cursor() as cursor:
                for i in range(0, len(incoming_attrs), 1000):
                    chunk = incoming_attrs[i:i+1000]
                    placeholders = ",".join(["%s"] * len(chunk))
                    cursor.execute(
                        f"SELECT ATTRIBUTE1 FROM WHR_TRUCK_SCAN_DETAILS WHERE ATTRIBUTE1 IN ({placeholders})",
                        chunk
                    )
                    for r in cursor.fetchall():
                        existing_attrs.add(str(r[0]))

        # -----------------------------------------------------
        # ✅ INSERT ONLY NEW ATTRIBUTE1 ROWS
        # -----------------------------------------------------
        for row in tabledata:

            row_attr = str(row.get("id"))
            if not row_attr:
                continue

            # ❌ Already inserted → skip
            if row_attr in existing_attrs:
                continue

            disreqqty = int(row.get("disreqqty") or 0)
            balanceqty = int(row.get("balanceqty") or 0)
            sendqty = int(row.get("sendqty") or 0)

            params_chunk.append((
                dispatch_id, req_id, row.get("pick_id"), date, warehouse,
                org_id, org_name, row.get("salesman_no"), row.get("salesman_name"),
                row.get("manager_no"), row.get("manager_name"), row.get("pickman_no"), row.get("pickman_name"),
                staff_no, staff_name, customer_number, customer_name,
                customer_site_id, row.get("line_no"), transporter_name,
                driver_name, driver_mobileno, vehicle_no, truck_dimension,
                loading_charges, transport_charges, misc_charges,
                deliveryaddress, salesmanremarks, remarks, row.get("invoiceno"),
                row.get("Customer_trx_id"), row.get("Customer_trx_line_id"), row.get("itemcode"), row.get("itemdetails"),
                row.get("Product_code"), row.get("Serial_No"), disreqqty, balanceqty, sendqty,
                creation_date, created_by, created_ip, created_mac,
                last_update_date, last_updated_by, last_update_ip, flag,
                delivery_date, row.get("Udel_id"), row_attr
            ))

            undel_summary[row.get("Udel_id")] += sendqty
            update_summary[(req_id, customer_number, customer_site_id, row.get("itemcode"), row.get("Udel_id"))] += sendqty
            save_deletes.append((req_id, row.get("pick_id")))

            if len(params_chunk) >= CHUNK_SIZE:
                with transaction.atomic():
                    with connections["default"].cursor() as cur:
                        cur.fast_executemany = True
                        cur.executemany(insert_sql, params_chunk)
                inserted += len(params_chunk)
                params_chunk = []

        # final insert
        if params_chunk:
            with transaction.atomic():
                with connections["default"].cursor() as cur:
                    cur.fast_executemany = True
                    cur.executemany(insert_sql, params_chunk)
            inserted += len(params_chunk)

        # -----------------------------------------------------
        # UPDATE WHR_CREATE_DISPATCH (UNCHANGED)
        # -----------------------------------------------------
        with connections["default"].cursor() as cursor:
            update_sql = """
                UPDATE WHR_CREATE_DISPATCH
                SET TRUCK_SCAN_QTY=%s
                WHERE REQ_ID=%s AND CUSTOMER_NUMBER=%s
                      AND CUSTOMER_SITE_ID=%s AND INVENTORY_ITEM_ID=%s
                      AND UNDEL_ID=%s
            """
            update_params = []

            for (req, cusno, cussite, itemcode, undelid), total_qty in update_summary.items():
                cursor.execute("""
                    SELECT TRUCK_SCAN_QTY FROM WHR_CREATE_DISPATCH
                    WHERE REQ_ID=%s AND CUSTOMER_NUMBER=%s
                          AND CUSTOMER_SITE_ID=%s AND INVENTORY_ITEM_ID=%s AND UNDEL_ID=%s
                """, [req, cusno, cussite, itemcode, undelid])

                row = cursor.fetchone()
                if not row:
                    continue

                current = int(row[0] or 0)
                new_qty = max(current - total_qty, 0)
                update_params.append((new_qty, req, cusno, cussite, itemcode, undelid))

            for i in range(0, len(update_params), CHUNK_SIZE):
                chunk = update_params[i:i+CHUNK_SIZE]
                with transaction.atomic():
                    with connections["default"].cursor() as cur:
                        cur.fast_executemany = True
                        cur.executemany(update_sql, chunk)

        # -----------------------------------------------------
        # INSERT TRANSACTION DETAILS (UNCHANGED)
        # -----------------------------------------------------
        trans_insert_sql = """
            INSERT INTO WHR_TRANSACTION_DETAILS (
                UNDEL_ID, TRANSACTION_DATE, CUSTOMER_TRX_ID, CUSTOMER_TRX_LINE_ID,
                ITEM_ID, LINE_NO, QTY, SOURCE, TRANSACTION_TYPE, DISPATCH_ID,
                REFERENCE1, REFERENCE2, REFERENCE3, REFERENCE4,
                CREATION_DATE, CREATED_BY, CREATED_IP, CREATED_MAC,
                LAST_UPDATE_DATE, LAST_UPDATED_BY, LAST_UPDATE_IP, LAST_UPDATE_MAC, FLAG
            ) VALUES (
                %s,%s,%s,%s,
                %s,%s,%s,%s,%s,%s,
                %s,%s,%s,%s,
                %s,%s,%s,%s,
                %s,%s,%s,%s,%s
            )
        """

        trans_params = []
        undel_totals = defaultdict(int)
        undel_meta = {}

        for row in tabledata:
            qty = int(row.get("sendqty") or 0)
            undel_id = row.get("Udel_id")
            if not undel_id or qty == 0:
                continue

            undel_totals[undel_id] += qty
            if undel_id not in undel_meta:
                undel_meta[undel_id] = row

        for undel_id, qty in undel_totals.items():
            r = undel_meta[undel_id]

            trans_params.append((
                undel_id,
                datetime.now(),
                r.get("Customer_trx_id"),
                r.get("Customer_trx_line_id"),
                r.get("itemcode"),
                r.get("line_no"),
                -qty,
                "Truck Delivery dispatch",
                "OUTBOUND",
                dispatch_id,
                None, None, None, None,
                datetime.now(), created_by, created_ip, created_mac,
                datetime.now(), last_updated_by, last_update_ip, None,
                'A'
            ))

            if len(trans_params) >= CHUNK_SIZE:
                with transaction.atomic():
                    with connections["default"].cursor() as cur:
                        cur.fast_executemany = True
                        cur.executemany(trans_insert_sql, trans_params)
                trans_params = []

        if trans_params:
            with transaction.atomic():
                with connections["default"].cursor() as cur:
                    cur.fast_executemany = True
                    cur.executemany(trans_insert_sql, trans_params)

        # -----------------------------------------------------
        # DELETE WHR_SAVE_TRUCK_DETAILS_TBL (UNCHANGED)
        # -----------------------------------------------------
        if save_deletes:
            with connections["default"].cursor() as cursor:
                for i in range(0, len(save_deletes), CHUNK_SIZE):
                    chunk = save_deletes[i:i+CHUNK_SIZE]
                    with transaction.atomic():
                        for reqno, pickid in chunk:
                            cursor.execute("""
                                DELETE FROM WHR_SAVE_TRUCK_DETAILS_TBL
                                WHERE req_no=%s AND pick_id=%s
                                  AND SCAN_STATUS='Request for Delivery'
                            """, [reqno, pickid])

        # -----------------------------------------------------
        # POST CHECK
        # -----------------------------------------------------
        with connections["default"].cursor() as cursor:
            cursor.execute(
                "SELECT COUNT(*) FROM WHR_TRUCK_SCAN_DETAILS WHERE DISPATCH_ID=%s",
                [dispatch_id]
            )
            exists = cursor.fetchone()[0]

        if exists == 0:
            return JsonResponse({"error": f"Dispatch ID {dispatch_id} not found"}, status=400)

        return JsonResponse({
            "message": f"{dispatch_id} truck scan rows processed successfully",
            "inserted": inserted,
            "undel_summary": {str(k): int(v) for k, v in undel_summary.items()}
        }, status=201)

    except Exception as e:
        import traceback, sys
        traceback.print_exc(file=sys.stderr)
        return JsonResponse({"error": str(e)}, status=400)



from collections import defaultdict
from django.db import connection, transaction
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
import json

@csrf_exempt
def insert_delivery_header(request):
    if request.method != "POST":
        return JsonResponse({"error": "Only POST allowed"}, status=405)

    try:
        data = json.loads(request.body)

        # ---- Header / Common fields ----
        dispatch_id       = data.get("DISPATCH_ID")
        req_id            = data.get("REQ_ID")
        date              = data.get("DATE")
        warehouse         = data.get("PHYSICAL_WAREHOUSE")
        org_id            = data.get("ORG_ID")
        org_name          = data.get("ORG_NAME")
        staff_no          = data.get("STAFF_NO")
        staff_name        = data.get("STAFF_NAME")
        customer_number   = data.get("CUSTOMER_NUMBER")
        customer_name     = data.get("CUSTOMER_NAME")
        customer_site_id  = data.get("CUSTOMER_SITE_ID")
        transporter_name  = data.get("TRANSPORTER_NAME")
        driver_name       = data.get("DRIVER_NAME")
        driver_mobileno   = data.get("DRIVER_MOBILENO")
        vehicle_no        = data.get("VEHICLE_NO")
        truck_dimension   = data.get("TRUCK_DIMENSION")
        loading_charges   = data.get("LOADING_CHARGES")
        transport_charges = data.get("TRANSPORT_CHARGES")
        misc_charges      = data.get("MISC_CHARGES")
        deliveryaddress   = data.get("DELIVERYADDRESS")
        salesmanremarks   = data.get("SALESMANREMARKS")
        remarks           = data.get("REMARKS")
        creation_date     = data.get("CREATION_DATE")
        created_by        = data.get("CREATED_BY")
        created_ip        = data.get("CREATED_IP")
        created_mac       = data.get("CREATED_MAC")
        last_update_date  = data.get("LAST_UPDATE_DATE")
        last_updated_by   = data.get("LAST_UPDATED_BY")
        last_update_ip    = data.get("LAST_UPDATE_IP")
        flag              = data.get("FLAG")
        delivery_date     = data.get("DELIVERY_DATE")

        tabledata = data.get("tabledata", [])

        # ---- Group rows by UNDEL_ID ----
        grouped = defaultdict(list)
        for row in tabledata:
            grouped[row.get("Udel_id")].append(row)

        with transaction.atomic():
            with connection.cursor() as cursor:
                for undel_id, rows in grouped.items():
                    # truck_send_qty = number of rows with this undel_id
                    truck_send_qty = len(rows)

                    # representative row (salesman/manager/pickman/etc.)
                    row = rows[0]

                    cursor.execute("""
                        INSERT INTO [BUYP].[dbo].[WHR_DELIVERY_HEADER_TBL] 
                        (
                            DISPATCH_ID, REQ_ID, PICK_ID, DATE, PHYSICAL_WAREHOUSE,
                            ORG_ID, ORG_NAME, SALESMAN_NO, SALESMAN_NAME,
                            MANAGER_NO, MANAGER_NAME, PICKMAN_NO, PICKMAN_NAME,
                            LOAD_NO, LOAD_NAME, SUPERUSER_NO, SUPERUSER_NAME,
                            CUSTOMER_NUMBER, CUSTOMER_NAME, CUSTOMER_SITE_ID, LINE_NO,
                            TRANSPORTER_NAME, DRIVER_NAME, DRIVER_MOBILENO, VEHICLE_NO,
                            TRUCK_DIMENSION, LOADING_CHARGES, TRANSPORT_CHARGES, MISC_CHARGES,
                            DELIVERYADDRESS, SALESMANREMARKS, REMARKS, INVOICE_NO,
                            CUSTOMER_TRX_ID, CUSTOMER_TRX_LINE_ID, ITEM_CODE, ITEM_DETAILS,
                            DISREQ_QTY, CREATION_DATE, CREATED_BY, CREATED_IP, CREATED_MAC,
                            LAST_UPDATE_DATE, LAST_UPDATED_BY, LAST_UPDATE_IP, FLAG,
                            DELIVERY_DATE, DELIVERY_STATUS, UNDEL_ID, SCAN_PATH, TRUCK_SEND_QTY
                        )
                        VALUES (
                            %s,%s,%s,%s,%s,
                            %s,%s,%s,%s,
                            %s,%s,%s,%s,
                            %s,%s,%s,%s,%s,%s,%s,%s,
                            %s,%s,%s,%s,%s,%s,
                            %s,%s,%s,%s,
                            %s,%s,%s,%s,
                            %s,%s,%s,
                            %s,%s,%s,%s,
                            %s,%s,%s,%s,
                            %s,%s,%s,%s,%s
                        )
                    """, [
                        dispatch_id, req_id, row.get("pick_id"), date, warehouse,
                        org_id, org_name, row.get("salesman_no"), row.get("salesman_name"),
                        row.get("manager_no"), row.get("manager_name"), row.get("pickman_no"), row.get("pickman_name"),
                        row.get("loadman_no"), row.get("loadman_name"), staff_no, staff_name,
                        customer_number, customer_name, customer_site_id, row.get("line_no"),
                        transporter_name, driver_name, driver_mobileno, vehicle_no,
                        truck_dimension, loading_charges, transport_charges, misc_charges,
                        deliveryaddress, salesmanremarks, remarks,
                        row.get("invoiceno"),
                        row.get("Customer_trx_id"), row.get("Customer_trx_line_id"),
                        row.get("itemcode"), row.get("itemdetails"), row.get("disreqqty"),
                        creation_date, created_by, created_ip, created_mac,
                        last_update_date, last_updated_by, last_update_ip, flag,
                        delivery_date, "Pending", undel_id, None, truck_send_qty
                    ])

        return JsonResponse({"success": True, "message": "Inserted successfully"})

    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)
    
class GetPickmanDetailsView(View):
    def get(self, request):
        pickman_name = request.GET.get("pickman")
        status_filter = request.GET.get("status")  # pickmanpending / pickmancomplete

        if not pickman_name:
            return JsonResponse({"error": "pickman is required"}, status=400)

        # Run query
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT 
                    ID, PICK_ID, REQ_ID, DATE, ASSIGN_PICKMAN, FLAG,
                    TOT_QUANTITY, PICKED_QTY, SCANNED_QTY, STATUS
                FROM WHR_DISPATCH_REQUEST
                WHERE ASSIGN_PICKMAN = %s
            """, [pickman_name])

            rows = cursor.fetchall()
            columns = [col[0] for col in cursor.description]

        # Convert rows into list of dicts
        data = [dict(zip(columns, row)) for row in rows]

        uniqueData = {}

        for item in data:
            try:
                invoiceQty = float(item.get('TOT_QUANTITY') or 0)
                pickedQty = float(item.get('PICKED_QTY') or 0)
                scanned_qty = float(item.get('SCANNED_QTY') or 0)
                flag = str(item.get('FLAG') or "")
                status = str(item.get('STATUS') or "")
                pickId = str(item.get('PICK_ID') or "")
                reqid = str(item.get('REQ_ID') or "")

                balance_qty = pickedQty - scanned_qty
                uniqueKey = f"{pickId}-{reqid}"

                if uniqueKey in uniqueData:
                    # aggregate quantities
                    uniqueData[uniqueKey]['des_id'] += invoiceQty
                    uniqueData[uniqueKey]['total'] += pickedQty
                    uniqueData[uniqueKey]['scanned_qty'] += scanned_qty
                    uniqueData[uniqueKey]['balance_qty'] += balance_qty

                    # aggregate status
                    if status.lower() == "pending":
                        uniqueData[uniqueKey]['status'] = "pending"
                    elif uniqueData[uniqueKey]['status'] != "pending" and status.lower() == "finished":
                        uniqueData[uniqueKey]['status'] = "Finished"
                else:
                    formatted_date = None
                    if item.get("DATE"):
                        try:
                            formatted_date = datetime.strftime(item["DATE"], "%Y-%m-%d")
                        except Exception:
                            formatted_date = str(item["DATE"])

                    uniqueData[uniqueKey] = {
                        "id": item["ID"],
                        "pickMan_Name": item["ASSIGN_PICKMAN"],
                        "reqid": reqid,
                        "pick_id": pickId,
                        "date": formatted_date,
                        "des_id": invoiceQty,
                        "total": pickedQty,
                        "scanned_qty": scanned_qty,
                        "balance_qty": balance_qty,
                        "flag": flag,
                        "status": status,
                    }
            except Exception:
                continue

        # ---- Final filter conditions ----
        filtered_data = []
        for entry in uniqueData.values():
            if entry['flag'] == 'OU':
                continue  # always ignore "OU"

            if status_filter == "pickmanpending":
                if entry['balance_qty'] != 0.0:
                    filtered_data.append(entry)
            elif status_filter == "pickmancomplete":
                if entry['balance_qty'] == 0.0:
                    filtered_data.append(entry)
            else:
                # default: pending (balance ≠ 0)
                if entry['balance_qty'] != 0.0:
                    filtered_data.append(entry)

        return JsonResponse({"data": filtered_data}, safe=False)


@method_decorator(csrf_exempt, name='dispatch')
class SaveLoginDetailsView(View):
    def post(self, request):
        try:
            body = json.loads(request.body.decode('utf-8'))

            login_id = body.get("LOGIN_ID")
            login_name = body.get("LOGIN_NAME")
            login_mac = body.get("LOGIN_MAC_ADDRESS")

            if not (login_id and login_name and login_mac):
                return JsonResponse({"error": "Missing required fields"}, status=400)

            with connection.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO WHR_LOGIIN_DETAILS (LOGIN_ID, LOGIN_NAME, LOGIN_MAC_ADDRESS)
                    VALUES (%s, %s, %s)
                """, [login_id, login_name, login_mac])

            return JsonResponse({"message": "Login details saved successfully"}, status=201)

        except Exception as e:
            return JsonResponse({"error": str(e)}, status=500)



@csrf_exempt
def Show_button_truck_details(request):
    if request.method != "POST":
        return JsonResponse({"error": "Only POST method allowed"}, status=405)

    try:
        body = json.loads(request.body.decode("utf-8"))
        reqid = body.get("reqid")
        pickid = body.get("pickid")
        loadman_no = body.get("loadman_no")
        loadman_name = body.get("loadman_name")
        dispatch_id = body.get("dispatch_id")

        if not reqid or not pickid:
            return JsonResponse({"error": "Both reqid and pickid are required"}, status=400)

        with connection.cursor() as cursor:
            # Fetch rows from WHR_PICKED_MAN with conditions
            cursor.execute("""
                SELECT [REQ_ID],[PICK_ID],[SALESMAN_NO],[SALESMAN_NAME],
                       [MANAGER_NO],[MANAGER_NAME],[PICKMAN_NO],[PICKMAN_NAME],
                       [CUSTOMER_NUMBER],[CUSTOMER_NAME],[CUSTOMER_SITE_ID],
                       [INVOICE_NUMBER],[CUSTOMER_TRX_ID],[CUSTOMER_TRX_LINE_ID],
                       [LINE_NUMBER],[INVENTORY_ITEM_ID],[ITEM_DESCRIPTION],
                       [TOT_QUANTITY],[DISPATCHED_QTY],[PRODUCT_CODE],
                       [SERIAL_NO],[UNDEL_ID]
                FROM [BUYP].[dbo].[WHR_PICKED_MAN]
                WHERE REQ_ID=%s AND PICK_ID=%s
                AND (PRODUCT_CODE = 'empty')
                AND (SERIAL_NO  = 'empty')
            """, [reqid, pickid])

            rows = cursor.fetchall()

            inserted = 0
            skipped = 0

            for row in rows:
                (req_no, pick_id, salesman_no, salesman_name,
                 manager_no, manager_name, pickman_no, pickman_name,
                 customer_no, customer_name, customer_site,
                 invoice_no, trx_id, trx_line_id,
                 line_no, item_code, item_details,
                 disreq_qty, send_qty, product_code,
                 serial_no, udel_id) = row

                # 1. Count in WHR_PICKED_MAN (how many rows exist for same reqid+pickid+undelid)
                cursor.execute("""
                    SELECT COUNT(*) 
                    FROM [BUYP].[dbo].[WHR_PICKED_MAN]
                    WHERE REQ_ID=%s AND PICK_ID=%s AND UNDEL_ID=%s
                    AND (PRODUCT_CODE='empty') AND (SERIAL_NO='empty')
                """, [req_no, pick_id, udel_id])
                total_pickman = cursor.fetchone()[0]

                # 2. Count already inserted in WHR_SAVE_TRUCK_DETAILS_TBL
                cursor.execute("""
                    SELECT COUNT(*)
                    FROM [BUYP].[dbo].[WHR_SAVE_TRUCK_DETAILS_TBL]
                    WHERE req_no=%s AND pick_id=%s AND Udel_id=%s
                    AND (PRODUCT_CODE='empty') AND (SERIAL_NO='empty')
                """, [req_no, pick_id, udel_id])
                total_saved = cursor.fetchone()[0]

                # 3. Allow insert only if saved < total_pickman
                if total_saved < total_pickman:
                    cursor.execute("""
                        INSERT INTO [BUYP].[dbo].[WHR_SAVE_TRUCK_DETAILS_TBL] 
                        ([dispatch_id],[req_no],[pick_id],[salesman_no],[salesman_name],
                         [manager_no],[manager_name],[pickman_no],[pickman_name],
                         [loadman_no],[loadman_name],
                         [Customer_no],[Customer_name],[Customer_Site],
                         [invoice_no],[Customer_trx_id],[Customer_trx_line_id],
                         [line_no],[Item_code],[Item_detailas],[DisReq_Qty],
                         [Send_qty],[Product_code],[Serial_No],[Udel_id],[SCAN_STATUS])
                        VALUES (%s,%s,%s,%s,%s,
                                %s,%s,%s,%s,
                                %s,%s,
                                %s,%s,%s,
                                %s,%s,%s,
                                %s,%s,%s,%s,
                                %s,%s,%s,%s,%s)
                    """, [
                        dispatch_id, req_no, pick_id, salesman_no, salesman_name,
                        manager_no, manager_name, pickman_no, pickman_name,
                        loadman_no, loadman_name,
                        customer_no, customer_name, customer_site,
                        invoice_no, trx_id, trx_line_id,
                        line_no, item_code, item_details, disreq_qty,
                        '1', product_code, serial_no, udel_id, "PENDING"
                    ])
                    inserted += 1
                else:
                    skipped += 1

        return JsonResponse({
            "message": f"{inserted} rows inserted, {skipped} rows skipped",
            "reqid": reqid,
            "pickid": pickid
        }, status=201)

    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)
    
import json
import urllib.request
from datetime import datetime
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.db import connection, transaction

# zoneinfo is in stdlib for Python 3.9+. If not available, try pytz fallback.
try:
    from zoneinfo import ZoneInfo
except ImportError:
    import pytz
    def ZoneInfo(name):
        return pytz.timezone(name)


def _get_local_time_from_api(timeout_seconds: int = 5):
    """
    Get current local time from worldtimeapi.org based on server IP.
    Works globally (India, Saudi, US, etc.).
    Falls back to server local time if API fails.
    """
    url = "http://worldtimeapi.org/api/ip"
    try:
        with urllib.request.urlopen(url, timeout=timeout_seconds) as resp:
            body = resp.read().decode("utf-8")
            data = json.loads(body)

            tz = data.get("timezone")
            dt_str = data.get("datetime")

            if tz and dt_str:
                try:
                    # Example datetime: "2025-09-12T12:50:00.123456+05:30"
                    dt = datetime.fromisoformat(dt_str)
                    return dt.astimezone(ZoneInfo(tz))
                except Exception:
                    pass

    except Exception as e:
        print(f"WorldTimeAPI fetch failed: {e}")

    # fallback: system local time with timezone
    try:
        return datetime.now().astimezone()
    except Exception:
        return datetime.now()


@csrf_exempt
def check_Insert_update_Status_view(request):
    """
    Insert or update WHM_Check_update_tbl row.
    If (Warehouse + Org_id + Emp_id) exists -> update Version + Last_Updated_date.
    Otherwise insert new row.
    """
    if request.method != "POST":
        return JsonResponse({"error": "Only POST method allowed"}, status=405)

    try:
        data = json.loads(request.body.decode("utf-8"))
    except Exception:
        return JsonResponse({"error": "Invalid JSON"}, status=400)

    warehouse = data.get("warehouse")
    orgid = data.get("orgid")
    empid = data.get("empid")
    empname = data.get("empname")
    version = data.get("version")

    if not all([warehouse, orgid, empid, empname, version]):
        return JsonResponse({
            "error": "warehouse, orgid, empid, empname and version are required"
        }, status=400)

    # Get local time dynamically based on server's location
    local_dt = _get_local_time_from_api()

    # SQL Server prefers naive datetimes (strip tzinfo)
    try:
        creation_dt_naive = local_dt.replace(tzinfo=None)
    except Exception:
        creation_dt_naive = local_dt

    try:
        with transaction.atomic():
            with connection.cursor() as cursor:
                # Check if record exists
                cursor.execute("""
                    SELECT id, Creation_date
                    FROM WHM_Check_update_tbl
                    WHERE Warehouse = %s AND Org_id = %s AND Emp_id = %s
                """, [warehouse, orgid, empid])
                row = cursor.fetchone()

                if row:
                    # Update existing record
                    cursor.execute("""
                        UPDATE WHM_Check_update_tbl
                        SET Version = %s,
                            Last_Updated_date = %s
                        WHERE Warehouse = %s AND Org_id = %s AND Emp_id = %s
                    """, [version, creation_dt_naive, warehouse, orgid, empid])
                    return JsonResponse({"message": "Existing record updated successfully"})
                else:
                    # Insert new record
                    cursor.execute("""
                        INSERT INTO WHM_Check_update_tbl
                        (Warehouse, Org_id, Emp_id, Emp_name, Version, Creation_date)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, [warehouse, orgid, empid, empname, version, creation_dt_naive])
                    return JsonResponse({"message": "New record inserted successfully"})

    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)




class InsertPickedDataToTruckView(APIView):
    def get(self, request, *args, **kwargs):
        reqid = request.GET.get("reqid")
        pickid = request.GET.get("pickid")
        loadman_no = request.GET.get("loadman_no")
        loadman_name = request.GET.get("loadman_name")

        if not reqid or not pickid or not loadman_no or not loadman_name:
            return JsonResponse({"error": "reqid, pickid, loadman_no and loadman_name are required"}, status=400)

        try:
            with transaction.atomic():
                with connection.cursor() as cursor:
                    # Total picked
                    cursor.execute(
                        "SELECT COUNT(*) FROM [BUYP].[dbo].[WHR_PICKED_MAN] WHERE REQ_ID=%s AND PICK_ID=%s",
                        [reqid, pickid],
                    )
                    total_picked = int(cursor.fetchone()[0] or 0)

                    # Already saved before
                    cursor.execute(
                        "SELECT COUNT(*) FROM [BUYP].[dbo].[WHR_SAVE_TRUCK_DETAILS_TBL] WHERE req_no=%s AND pick_id=%s",
                        [reqid, pickid],
                    )
                    total_saved_before = int(cursor.fetchone()[0] or 0)

                    # For each UNDEL_ID
                    cursor.execute(
                        """
                        SELECT pm.UNDEL_ID, COUNT(*) AS picked_count
                        FROM [BUYP].[dbo].[WHR_PICKED_MAN] pm
                        WHERE pm.REQ_ID=%s AND pm.PICK_ID=%s
                        GROUP BY pm.UNDEL_ID
                        """,
                        [reqid, pickid],
                    )
                    undel_rows = cursor.fetchall()

                    undel_stats = []
                    total_inserted = 0

                    for undel_id, picked_count in undel_rows:
                        # insert only rows NOT already in TRUCK_SCAN_DETAILS and NOT already in SAVE_TRUCK
                        insert_sql = """
                        INSERT INTO [BUYP].[dbo].[WHR_SAVE_TRUCK_DETAILS_TBL]
                        (dispatch_id, req_no, pick_id, salesman_no, salesman_name,
                         manager_no, manager_name, pickman_no, pickman_name,
                         loadman_no, loadman_name,
                         Customer_no, Customer_name, Customer_Site,
                         invoice_no, Customer_trx_id, Customer_trx_line_id,
                         line_no, Item_code, Item_detailas,
                         DisReq_Qty, Send_qty, Product_code, Serial_No, Udel_id, SCAN_STATUS)
                        SELECT
                          '1',
                          pm.REQ_ID, pm.PICK_ID, pm.SALESMAN_NO, pm.SALESMAN_NAME,
                          pm.MANAGER_NO, pm.MANAGER_NAME, pm.PICKMAN_NO, pm.PICKMAN_NAME,
                          %s, %s,
                          pm.CUSTOMER_NUMBER, pm.CUSTOMER_NAME, pm.CUSTOMER_SITE_ID,
                          pm.INVOICE_NUMBER, pm.CUSTOMER_TRX_ID, pm.CUSTOMER_TRX_LINE_ID,
                          pm.LINE_NUMBER, pm.INVENTORY_ITEM_ID, pm.ITEM_DESCRIPTION,
                          pm.TOT_QUANTITY, pm.PICKED_QTY, pm.PRODUCT_CODE, pm.SERIAL_NO, pm.UNDEL_ID, %s
                        FROM [BUYP].[dbo].[WHR_PICKED_MAN] pm
                        WHERE pm.REQ_ID=%s AND pm.PICK_ID=%s AND pm.UNDEL_ID=%s
                          -- not already in TRUCK_SCAN_DETAILS
                          AND NOT EXISTS (
                            SELECT 1 FROM [BUYP].[dbo].[WHR_TRUCK_SCAN_DETAILS] ts
                            WHERE ts.REQ_ID = pm.REQ_ID
                              AND ts.PICK_ID = pm.PICK_ID
                              AND ts.UNDEL_ID = pm.UNDEL_ID
                              AND ISNULL(ts.ITEM_CODE,'') = ISNULL(pm.INVENTORY_ITEM_ID,'')
                              AND ISNULL(ts.CUSTOMER_TRX_ID,'') = ISNULL(pm.CUSTOMER_TRX_ID,'')
                              AND ISNULL(ts.CUSTOMER_TRX_LINE_ID,'') = ISNULL(pm.CUSTOMER_TRX_LINE_ID,'')
                              AND ISNULL(ts.LINE_NO,'') = ISNULL(pm.LINE_NUMBER,'')
                              AND ISNULL(ts.SERIAL_NO,'') = ISNULL(pm.SERIAL_NO,'')
                          )
                          -- not already in SAVE_TRUCK
                          AND NOT EXISTS (
                            SELECT 1 FROM [BUYP].[dbo].[WHR_SAVE_TRUCK_DETAILS_TBL] st
                            WHERE st.req_no=pm.REQ_ID
                              AND st.pick_id=pm.PICK_ID
                              AND st.Udel_id=pm.UNDEL_ID
                              AND ISNULL(st.Item_code,'')=ISNULL(pm.INVENTORY_ITEM_ID,'')
                              AND ISNULL(st.Customer_trx_id,'')=ISNULL(pm.CUSTOMER_TRX_ID,'')
                              AND ISNULL(st.Customer_trx_line_id,'')=ISNULL(pm.CUSTOMER_TRX_LINE_ID,'')
                              AND ISNULL(st.line_no,'')=ISNULL(pm.LINE_NUMBER,'')
                              AND ISNULL(st.Serial_No,'')=ISNULL(pm.SERIAL_NO,'')
                          )
                        """
                        cursor.execute(insert_sql, [loadman_no, loadman_name, "PENDING", reqid, pickid, undel_id])
                        rows_inserted = cursor.rowcount if cursor.rowcount != -1 else 0
                        total_inserted += rows_inserted

                        undel_stats.append(
                            {
                                "undel_id": undel_id,
                                "picked_count": picked_count,
                                "inserted": rows_inserted,
                            }
                        )

                    # After insert count
                    cursor.execute(
                        "SELECT COUNT(*) FROM [BUYP].[dbo].[WHR_SAVE_TRUCK_DETAILS_TBL] WHERE req_no=%s AND pick_id=%s",
                        [reqid, pickid],
                    )
                    total_saved_after = int(cursor.fetchone()[0] or 0)

            return JsonResponse(
                {
                    "message": "Process complete",
                    "reqid": reqid,
                    "pickid": pickid,
                    "loadman_no": loadman_no,
                    "loadman_name": loadman_name,
                    "total_picked": total_picked,
                    "total_saved_before": total_saved_before,
                    "inserted": total_inserted,
                    "total_saved_after": total_saved_after,
                    "undel_stats": undel_stats,
                }
            )

        except Exception as e:
            return JsonResponse({"error": str(e)}, status=500)






@csrf_exempt
def employee_access_view(request):
    if request.method == "POST":
        try:
            data = json.loads(request.body.decode("utf-8"))
            employee_id = data.get("employee_id")
            access_type = data.get("access_type")
            enable_status = data.get("enable_status")

            if not (employee_id and access_type and enable_status is not None):
                return JsonResponse({"status": "error", "message": "Missing required fields"}, status=400)

            with connection.cursor() as cursor:
                # Check if record exists (optimized with TOP 1)
                cursor.execute("""
                    SELECT TOP 1 id 
                    FROM WHR_EMPLOYEE_ACCESS_TBL
                    WHERE employee_id = %s AND access_type = %s
                """, [employee_id, access_type])
                row = cursor.fetchone()

                if row:
                    # Update if exists
                    cursor.execute("""
                        UPDATE WHR_EMPLOYEE_ACCESS_TBL
                        SET enable_status = %s
                        WHERE employee_id = %s AND access_type = %s
                    """, [enable_status, employee_id, access_type])
                    return JsonResponse({"status": "success", "message": "Record updated"})
                else:
                    # Insert if not exists
                    cursor.execute("""
                        INSERT INTO WHR_EMPLOYEE_ACCESS_TBL (employee_id, access_type, enable_status)
                        VALUES (%s, %s, %s)
                    """, [employee_id, access_type, enable_status])
                    return JsonResponse({"status": "success", "message": "Record inserted"})

        except Exception as e:
            return JsonResponse({"status": "error", "message": str(e)}, status=500)

    return JsonResponse({"status": "error", "message": "Invalid request method"}, status=405)



def get_employee_access_view(request, employee_id):
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT access_type, enable_status
                FROM WHR_EMPLOYEE_ACCESS_TBL
                WHERE employee_id = %s
            """, [employee_id])
            rows = cursor.fetchall()

        result = [
            {"access_type": row[0], "enable_status": row[1]}
            for row in rows
        ]

        return JsonResponse({"status": "success", "data": result}, safe=False)

    except Exception as e:
        return JsonResponse({"status": "error", "message": str(e)}, status=500)
    
@csrf_exempt
def update_employee_access(request):
    if request.method != "POST":
        return JsonResponse({"status": "error", "message": "Only POST allowed"}, status=405)

    try:
        body = json.loads(request.body)
        employee_id = body.get("employee_id")
        enable_status = body.get("enable_status")

        if not employee_id or enable_status is None:
            return JsonResponse({"status": "error", "message": "Missing parameters"}, status=400)

        access_type = "swape WHR superuser"

        sql = """
        MERGE WHR_EMPLOYEE_ACCESS_TBL AS target
        USING (SELECT %s AS employee_id, %s AS access_type, %s AS enable_status) AS source
        ON target.employee_id = source.employee_id
        WHEN MATCHED THEN
            UPDATE SET enable_status = source.enable_status
        WHEN NOT MATCHED THEN
            INSERT (employee_id, access_type, enable_status)
            VALUES (source.employee_id, source.access_type, source.enable_status);
        """

        with connection.cursor() as cursor:
            cursor.execute(sql, [employee_id, access_type, enable_status])

        return JsonResponse({"status": "success", "message": "Saved successfully"})

    except Exception as e:
        return JsonResponse({"status": "error", "message": str(e)}, status=500)

@csrf_exempt
def get_whr_superuser_list(request, org_id):
    """
    Fetch WHR Superuser list filtered by ORG_ID.
    URL: /api/whr-superusers/<org_id>/
    """
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT 
                    EMPLOYEE_ID,
                    EMP_NAME,
                    ORG_ID,
                    PHYSICAL_WAREHOUSE,
                    EMP_NAME AS EMPLOYEE_FULL_NAME
                FROM WHR_USER_MANAGEMENT
                WHERE EMP_ROLE = 'WHR SUPERUSER'
                  AND ORG_ID = %s
            """, [org_id])

            rows = cursor.fetchall()

        data = []
        for row in rows:
            data.append({
                "WHR_SuperUser_NO": row[0],
                "WHR_SuperUser_NAME": row[1],
                "ORG_ID": row[2],
                "WAREHOUSE_NAME": row[3],
                "EMPLOYEE_FULL_NAME": row[4],
            })

        return JsonResponse(data, safe=False, status=200)

    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)

# to check the formname and uniquevalues already exists or not
@csrf_exempt
def wms_formname_check_view(request):
    if request.method != "POST":
        return JsonResponse({"error": "Only POST method allowed"}, status=405)

    try:
        data = json.loads(request.body)
        formname = data.get("formname")
        uniquevalues = data.get("uniquevalues")
        status = data.get("status")

        if not all([formname, uniquevalues, status]):
            return JsonResponse({"error": "Missing required fields"}, status=400)

        with connection.cursor() as cursor:
            # Check if record exists with same formname and uniquevalues
            cursor.execute("""
                SELECT id, status FROM [BUYP].[dbo].[WMS_FORM_NAME]
                WHERE formname = %s AND uniquevalues = %s
            """, [formname, uniquevalues])
            existing = cursor.fetchone()

            # CASE 1: No record exists → INSERT
            if not existing:
                cursor.execute("""
                    INSERT INTO [BUYP].[dbo].[WMS_FORM_NAME] (formname, uniquevalues, status)
                    VALUES (%s, %s, %s)
                """, [formname, uniquevalues, status])
                return JsonResponse({"message": "Record inserted successfully."})

            existing_id, existing_status = existing

            # CASE 2: Record exists with same status → Warning
            if existing_status == status:
                return JsonResponse({"warning": "This unique id already processed with another employee."})

            # CASE 3: Record exists with different status
            if status.lower() == "finished":
                # CASE 4: Status == Finished → Delete
                cursor.execute("""
                    DELETE FROM [BUYP].[dbo].[WMS_FORM_NAME]
                    WHERE id = %s
                """, [existing_id])
                return JsonResponse({"message": "Record deleted as finished."})
            else:
                # Update status
                cursor.execute("""
                    UPDATE [BUYP].[dbo].[WMS_FORM_NAME]
                    SET status = %s
                    WHERE id = %s
                """, [status, existing_id])
                return JsonResponse({"message": "Record status updated successfully."})

    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)

@csrf_exempt
def update_dispatch_balance(request):
    if request.method != "POST":
        return JsonResponse({"error": "Only POST allowed"}, status=405)

    try:
        data = json.loads(request.body.decode("utf-8"))

        reqid = data.get("reqid")
        undelid = data.get("undelid")

        if not reqid or undelid is None:
            return JsonResponse({"error": "reqid and undelid are required"}, status=400)

        with connection.cursor() as cursor:

            # --------------------------------------------------------
            # 1️⃣ GET DISPATCHED_QTY and TRUCK_SCAN_QTY
            # --------------------------------------------------------
            cursor.execute("""
                SELECT DISPATCHED_QTY, TRUCK_SCAN_QTY
                FROM WHR_CREATE_DISPATCH
                WHERE REQ_ID = %s AND UNDEL_ID = %s
            """, [reqid, undelid])

            row = cursor.fetchone()

            if not row:
                return JsonResponse({"error": "No record found in WHR_CREATE_DISPATCH"}, status=404)

            dispatched_qty = row[0] or 0
            truck_scan_qty = row[1] or 0

            # Business rule:
            final_truck_qty = dispatched_qty - truck_scan_qty

            # --------------------------------------------------------
            # 2️⃣ GET FINAL PICKED QTY: (SUM PICKED_QTY - COUNT TRUCK_SCAN)
            # --------------------------------------------------------
            cursor.execute("""
                SELECT 
                    ISNULL((
                        SELECT SUM(PICKED_QTY)
                        FROM WHR_DISPATCH_REQUEST
                        WHERE REQ_ID = %s 
                        AND UNDEL_ID = %s
                    ), 0)
                    -
                    ISNULL((
                        SELECT COUNT(*)
                        FROM WHR_TRUCK_SCAN_DETAILS T
                        WHERE T.REQ_ID = %s 
                        AND T.UNDEL_ID = %s
                        AND T.PICK_ID IN (
                                SELECT PICK_ID 
                                FROM WHR_DISPATCH_REQUEST
                                WHERE REQ_ID = %s
                                AND UNDEL_ID = %s
                        )
                    ), 0)
                AS FINAL_PICKED_QTY;

            """, [reqid, undelid, reqid, undelid, reqid, undelid])

            already_picked_qty = cursor.fetchone()[0] or 0

            # --------------------------------------------------------
            # 3️⃣ GET STAGING QTY (ONLY PICK_ID NOT EXISTING IN DISPATCH REQUEST)
            # --------------------------------------------------------
            cursor.execute("""
                SELECT COUNT(*)
                FROM WHR_SAVE_TRUCK_DETAILS_TBL s
                WHERE s.req_no = %s 
                  AND s.Udel_id = %s
                  AND s.PICK_ID NOT IN (
                        SELECT PICK_ID
                        FROM WHR_DISPATCH_REQUEST
                        WHERE REQ_ID = %s AND UNDEL_ID = %s
                  )
            """, [reqid, undelid, reqid, undelid])

            staging_qty = cursor.fetchone()[0] or 0

            # --------------------------------------------------------
            # 4️⃣ FINAL BALANCE CALCULATION
            # --------------------------------------------------------
            balance_qty = dispatched_qty - (
                final_truck_qty +
                already_picked_qty +
                staging_qty
            )

            # Avoid negative values
            balance_qty = max(0, balance_qty)

            # --------------------------------------------------------
            # 5️⃣ UPDATE DISPATCHED_BY_MANAGER
            # --------------------------------------------------------
            cursor.execute("""
                UPDATE WHR_CREATE_DISPATCH
                SET DISPATCHED_BY_MANAGER = %s
                WHERE REQ_ID = %s AND UNDEL_ID = %s
            """, [balance_qty, reqid, undelid])

        # --------------------------------------------------------
        # 6️⃣ RESPONSE
        # --------------------------------------------------------
        return JsonResponse({
            "message": "Updated successfully",
            "reqid": reqid,
            "undelid": undelid,
            "dispatched_qty": dispatched_qty,
            "truck_scan_qty": truck_scan_qty,
            "final_truck_qty": final_truck_qty,
            "already_picked_qty": already_picked_qty,
            "staging_qty": staging_qty,
            "balance_qty": balance_qty
        }, status=200)

    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)

from django.http import JsonResponse, HttpRequest
from django.views.decorators.csrf import csrf_exempt
from django.db import connection
import json
 
# --- Helper function to simulate a POST request for the second view ---
def simulate_post_request(data):
    """
    Creates a minimal HttpRequest object for calling another view function.
    """
    request = HttpRequest()
    request.method = 'POST'
    request._body = json.dumps(data).encode('utf-8')
    return request


@csrf_exempt
def add_shipment_dispatch_update(request):
    if request.method != "POST":
        return JsonResponse({"status": "error", "message": "POST method required"}, status=400)
 
    try:
        # 1. Parse the request body
        body = json.loads(request.body.decode("utf-8"))
 
        # Define columns for INSERT (excluding TRANSFER_TYPE)
        insert_columns = [
            "SHIPMENT_ID", "WAREHOUSE_NAME", "TO_WAREHOUSE_NAME", "SALESMANNO", "SALESMANAME",
            "DATE", "TRANSPORTER_NAME", "DRIVER_NAME", "DRIVER_MOBILENO", "VEHICLE_NO",
            "TRUCK_DIMENSION", "LOADING_CHARGES", "TRANSPORT_CHARGES", "MISC_CHARGES",
            "DELIVERYADDRESS", "SHIPMENT_HEADER_ID", "SHIPMENT_LINE_ID", "LINE_NUM",
            "CREATION_DATE", "CREATED_BY", "ORGANIZATION_ID", "ORGANIZATION_CODE",
            "ORGANIZATION_NAME", "SHIPMENT_NUM", "RECEIPT_NUM", "SHIPPED_DATE",
            "TO_ORGN_ID", "TO_ORGN_CODE", "TO_ORGN_NAME", "QUANTITY_SHIPPED",
            "QUANTITY_PROGRESS", "QUANTITY_RECEIVED", "UNIT_OF_MEASURE", "ITEM_ID",
            "DESCRIPTION", "FRANCHISE", "FAMILY", "CLASS", "SUBCLASS",
            "SHIPMENT_LINE_STATUS_CODE", "ACTIVE_STATUS", "REMARKS"
        ]
 
        # Extract values for the INSERT operation
        insert_values = [body.get(col) for col in insert_columns]
 
        # 2. Execute the INSERT SQL
        placeholders = ','.join(['%s'] * len(insert_values))
        sql = f"INSERT INTO [BUYP].[dbo].[WHR_SHIMENT_DISPATCH] ({','.join(insert_columns)}) VALUES ({placeholders})"
 
        with connection.cursor() as cursor:
            cursor.execute(sql, insert_values)
 
        # --- 3. Prepare data and call the update view ---
        transfer_type = body.get("TRANSFER_TYPE")
       
        # Determine which quantity to use for 'qty_recent' based on TRANSFER_TYPE
        # Assuming: INTERORG uses QUANTITY_RECEIVED for update. Adjust logic if needed.
        if transfer_type == "INTERORG":
             qty_recent = body.get("UPDATE_QUANTITY")
        elif transfer_type == "SOME_OTHER_TYPE": # Example logic for other types
             qty_recent = body.get("UPDATE_QUANTITY")
        else:
             # Default to QUANTITY_RECEIVED if type is unknown or not specified
             qty_recent = body.get("UPDATE_QUANTITY")
       
       
        update_data = {
            "SHIPMENT_NUM": body.get("SHIPMENT_NUM"),
            "SHIPMENT_LINE_ID": body.get("SHIPMENT_LINE_ID"),
            "qty_recent": qty_recent,
            "TRANSFER_TYPE": transfer_type
        }
       
        # Simulate a POST request for the update view
        update_request = simulate_post_request(update_data)
       
        # Call the update view function directly
        update_response = update_Phy_quantity_Shipped_interOrg(update_request)
       
        # Check the status of the update response
        update_status = json.loads(update_response.content).get("status")
       
        # 4. Return combined success response
        if update_status == "success":
            return JsonResponse({
                "status": "success",
                "message": "Shipment record inserted and Quantity updated successfully.",
                "update_details": json.loads(update_response.content)
            })
        else:
            # Although insert succeeded, the update failed.
            # You might want to log this or consider a transaction rollback (if using transactions).
            return JsonResponse({
                "status": "partial_success",
                "message": "Shipment record inserted, but quantity update failed.",
                "update_error": json.loads(update_response.content)
            }, status=207) # 207 Multi-Status
 
    except Exception as e:
        # Rollback the transaction on failure (Django usually handles this in a view context)
        # If the failure is during the initial INSERT, Django should automatically roll it back.
        return JsonResponse({"status": "error", "message": str(e)}, status=500)
 

@require_GET
def quick_bill_visibility(request):
    username = request.GET.get('username')

    if not username:
        return JsonResponse(
            {"quick_bill_visible": False, "error": "username required"},
            status=400
        )

    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT TOP 1
                CASE 
                    WHEN LOWER(ISNULL(Quick_bill_Visible, 'false')) = 'true'
                    THEN 1
                    ELSE 0
                END AS quickbillaccess
            FROM WHR_EMPLOYEE_ACCESS_TBL
            WHERE employee_id = %s
            ORDER BY id DESC
        """, [username])

        row = cursor.fetchone()

    # Convert 1 / 0 → True / False
    quick_bill_visible = bool(row[0]) if row else False

    return JsonResponse({
        "username": username,
        "quick_bill_visible": quick_bill_visible
    })


@require_GET
def Quick_Bill_acess_Get_whr_superuser_list(request):
    data = {}

    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT
                PHYSICAL_WAREHOUSE,
                EMP_USERNAME,
                EMP_NAME,
                QUICK_BILL_ENABLE
            FROM BUYP.dbo.WHR_USER_MANAGEMENT
            WHERE EMP_ROLE = %s
            ORDER BY PHYSICAL_WAREHOUSE, EMP_NAME
        """, ['WHR SuperUser'])

        rows = cursor.fetchall()

    for warehouse, username, name, quick_bill_enable in rows:
        warehouse = warehouse.strip() if warehouse else "UNKNOWN WAREHOUSE"

        # ✅ Normalize QUICK_BILL_ENABLE to boolean
        quick_bill_visible = False
        if quick_bill_enable is not None:
            if isinstance(quick_bill_enable, bool):
                quick_bill_visible = quick_bill_enable
            elif isinstance(quick_bill_enable, int):
                quick_bill_visible = bool(quick_bill_enable)
            elif isinstance(quick_bill_enable, str):
                quick_bill_visible = quick_bill_enable.strip().lower() == 'true'

        if warehouse not in data:
            data[warehouse] = []

        data[warehouse].append({
            "WHR SuperUser No": username,
            "WHR SuperUser Name": name if name else "NO - NAME",
            "QUICK_BILL_ENABLE": quick_bill_visible
        })

    return JsonResponse(data, safe=True)



@csrf_exempt
@require_POST
def update_quick_bill_access(request):
    try:
        body = json.loads(request.body)

        employee_id = body.get('employee_id')
        quick_bill_enable = body.get('quick_bill_enable')

        if employee_id is None or quick_bill_enable is None:
            return JsonResponse(
                {"error": "employee_id and quick_bill_enable are required"},
                status=400
            )

        # ✅ Normalize to DB-safe value
        # Store as 'true' / 'false' (nvarchar safe)
        db_value = 'true' if bool(quick_bill_enable) else 'false'

        with connection.cursor() as cursor:
            cursor.execute("""
                UPDATE BUYP.dbo.WHR_USER_MANAGEMENT
                SET QUICK_BILL_ENABLE = %s
                WHERE EMPLOYEE_ID = %s
            """, [db_value, str(employee_id)])

            if cursor.rowcount == 0:
                return JsonResponse(
                    {"error": "Employee not found"},
                    status=404
                )

        return JsonResponse({
            "employee_id": str(employee_id),
            "QUICK_BILL_ENABLE": bool(quick_bill_enable),
            "message": "Quick Bill access updated successfully"
        })

    except json.JSONDecodeError:
        return JsonResponse({"error": "Invalid JSON"}, status=400)

    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)




@require_GET
def get_quick_bill_enable_status_Employeeid(request):
    employee_id = request.GET.get('employee_id')

    if not employee_id:
        return JsonResponse(
            {"quick_bill_enable": False, "error": "employee_id is required"},
            status=400
        )

    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT TOP 1 QUICK_BILL_ENABLE
            FROM BUYP.dbo.WHR_USER_MANAGEMENT
            WHERE EMPLOYEE_ID = %s
        """, [str(employee_id)])

        row = cursor.fetchone()

    # ✅ Default = False
    quick_bill_enable = False

    if row and row[0] is not None:
        value = row[0]

        if isinstance(value, bool):
            quick_bill_enable = value
        elif isinstance(value, int):
            quick_bill_enable = bool(value)
        elif isinstance(value, str):
            quick_bill_enable = value.strip().lower() == 'true'

    return JsonResponse({
        "employee_id": str(employee_id),
        "quick_bill_enable": quick_bill_enable
    })




@csrf_exempt
@require_POST
def update_admin_quick_bill_visible(request):
    try:
        body = json.loads(request.body)

        employee_id = body.get('employee_id')
        quick_bill_visible = body.get('quick_bill_visible')

        if employee_id is None or quick_bill_visible is None:
            return JsonResponse(
                {"error": "employee_id and quick_bill_visible are required"},
                status=400
            )

        # ✅ Normalize to nvarchar-safe value
        db_value = 'true' if bool(quick_bill_visible) else 'false'

        with connection.cursor() as cursor:
            cursor.execute("""
                UPDATE BUYP.dbo.WHR_EMPLOYEE_ACCESS_TBL
                SET Quick_bill_Visible = %s
                WHERE employee_id = %s
            """, [db_value, str(employee_id)])

            if cursor.rowcount == 0:
                return JsonResponse(
                    {"error": "Employee not found"},
                    status=404
                )

        return JsonResponse({
            "employee_id": str(employee_id),
            "quick_bill_visible": bool(quick_bill_visible),
            "message": "Quick bill visibility updated successfully"
        })

    except json.JSONDecodeError:
        return JsonResponse({"error": "Invalid JSON"}, status=400)

    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)



@require_GET
def get_Admin_quick_bill_visible(request):
    employee_id = request.GET.get('employee_id')

    if not employee_id:
        return JsonResponse(
            {"quick_bill_visible": False, "error": "employee_id is required"},
            status=400
        )

    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT TOP 1 Quick_bill_Visible
            FROM BUYP.dbo.WHR_EMPLOYEE_ACCESS_TBL
            WHERE employee_id = %s
        """, [str(employee_id)])

        row = cursor.fetchone()

    # ✅ Default = false
    quick_bill_visible = False

    if row and row[0] is not None:
        value = row[0]

        if isinstance(value, bool):
            quick_bill_visible = value
        elif isinstance(value, int):
            quick_bill_visible = bool(value)
        elif isinstance(value, str):
            quick_bill_visible = value.strip().lower() == 'true'

    return JsonResponse({
        "employee_id": str(employee_id),
        "quick_bill_visible": quick_bill_visible
    })





