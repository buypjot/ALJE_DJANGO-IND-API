

from django.db.models.functions import Length

from django.views.decorators.http import require_GET,require_POST
from django.utils import timezone  # Django’s timezone aware datetime




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
from ALJE_APP.version import API_VERSION, RELEASE_DATE
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
from ALJE_APP.models import Pickman_ScanModels, Truck_scanModels
from ALJE_APP.serializers import Pickman_ScanModelsserializers
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
from ALJE_APP.serializers import *
from ALJE_APP.models import REQNO_Models
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


@csrf_exempt
def dispatch_fulfilled_records_view(request):
    """
    Optimized view to retrieve ONLY 'Fullfilled' dispatch records.
    Filtering is done at the Database level for high performance.
    """
    if request.method == 'GET':
        warehouse = request.GET.get('warehouse')

        try:
            close_old_connections()
            
            # Constructing the query
            warehouse_condition = ""
            params = []
            
            if warehouse:
                warehouse_condition = "AND d.PHYSICAL_WAREHOUSE = %s"
                params.append(warehouse)
            
            with connection.cursor() as cursor:
                query = f"""
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
                      {warehouse_condition}
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
                LEFT JOIN InvoiceList i ON dd.REQ_ID = i.REQ_ID
                WHERE dd.dis_qty_total = ISNULL(t.previous_truck_qty, 0)
                ORDER BY dd.id DESC;
                """
                cursor.execute(query, params)
                columns = [col[0] for col in cursor.description]
                rows = cursor.fetchall()

                # Optimization: Direct mapping without extra looping/checking
                # The SQL already filtered for 'Fullfilled' and sorted by ID
                result = [
                    {
                        'id': row[0],
                        'reqno': row[1],
                        'commercialNo': row[2],
                        'commercialName': row[3],
                        'salesman_no': row[4],
                        'salesmanName': row[5],
                        'cusno': row[6],
                        'cusname': row[7],
                        'cussite': row[8],
                        'date': row[9],
                        'deliverydate': row[10],
                        'dis_qty_total': row[11],
                        'dis_mangerQty_total': row[12],
                        'balance_qty': row[13],
                        'previous_truck_qty': row[14],
                        'return_qty': row[15],
                        'picked_qty': row[16],
                        'invoice_no_list': row[17]
                    }
                    for row in rows
                ]

                return JsonResponse(result, safe=False)

        except Exception as e:
            return JsonResponse({'status': 'error', 'message': str(e)}, status=500)

    return JsonResponse({'status': 'error', 'message': 'Invalid request method'}, status=405)




@csrf_exempt
def dispatch_fulfilled_invoice_details_view(request):
    """
    Optimized view to retrieve ONLY 'Fullfilled' dispatch invoice details.
    Filtering for 'Fullfilled' status (dis_qty == truck_qty) is done at the Database level.
    Includes CTE optimization to restrict aggregations to relevant REQ_IDs.
    """
    if request.method != 'GET':
        return JsonResponse({'status': 'error', 'message': 'Invalid request method'}, status=405)

    warehouse = request.GET.get('warehouse')
    # status parameter is not needed as we always fetch 'Fullfilled'

    try:
        close_old_connections()
        
        # Constructing the query
        warehouse_condition = ""
        params = []
        
        if warehouse:
            warehouse_condition = "AND d.PHYSICAL_WAREHOUSE = %s"
            params.append(warehouse)

        with connection.cursor() as cursor:
            query = f"""
            ;WITH DispatchData AS (
                SELECT
                    MAX(d.ID) AS id,
                    d.REQ_ID,
                    d.UNDEL_ID,
                    d.INVOICE_NUMBER,
                    d.INVENTORY_ITEM_ID,
                    MAX(d.ITEM_DESCRIPTION) AS ITEM_DESCRIPTION,
                    MAX(d.COMMERCIAL_NO) AS COMMERCIAL_NO,
                    MAX(d.COMMERCIAL_NAME) AS COMMERCIAL_NAME,
                    MAX(d.SALESMAN_NO) AS SALESMAN_NO,
                    MAX(d.SALESMAN_NAME) AS SALESMAN_NAME,
                    MAX(d.CUSTOMER_NUMBER) AS CUSTOMER_NUMBER,
                    MAX(d.CUSTOMER_NAME) AS CUSTOMER_NAME,
                    MAX(d.CUSTOMER_SITE_ID) AS CUSTOMER_SITE_ID,
                    MAX(d.INVOICE_DATE) AS INVOICE_DATE,
                    MAX(d.DELIVERY_DATE) AS DELIVERY_DATE,
                    SUM(d.DISPATCHED_QTY) AS dis_qty_total,
                    SUM(d.DISPATCHED_BY_MANAGER) AS dis_mangerQty_total
                FROM BUYP.dbo.WHR_CREATE_DISPATCH d
                WHERE d.FLAG NOT IN ('R','OU')
                  {warehouse_condition}
                GROUP BY
                    d.REQ_ID, d.UNDEL_ID, d.INVOICE_NUMBER, d.INVENTORY_ITEM_ID
            ),
            RelevantReqs AS (
                SELECT DISTINCT REQ_ID FROM DispatchData
            ),
            TruckQty AS (
                SELECT
                    t.REQ_ID,
                    t.UNDEL_ID,
                    t.INVOICE_NO AS INVOICE_NUMBER,
                    t.ITEM_CODE AS INVENTORY_ITEM_ID,
                    COUNT(*) AS previous_truck_qty
                FROM BUYP.dbo.WHR_TRUCK_SCAN_DETAILS t
                INNER JOIN RelevantReqs r ON t.REQ_ID = r.REQ_ID
                WHERE t.FLAG NOT IN ('R','SR','OU')
                GROUP BY t.REQ_ID, t.UNDEL_ID, t.INVOICE_NO, t.ITEM_CODE
            ),
            ReturnQty AS (
                SELECT
                    rt.REQ_ID,
                    rt.UNDEL_ID,
                    rt.INVOICE_NO AS INVOICE_NUMBER,
                    rt.ITEM_CODE AS INVENTORY_ITEM_ID,
                    MAX(rt.CUSTOMER_NUMBER) AS CUSTOMER_NUMBER,
                    MAX(rt.CUSTOMER_SITE_ID) AS CUSTOMER_SITE_ID,
                    COUNT(*) AS return_qty
                FROM BUYP.dbo.WHR_RETURN_DISPATCH rt
                INNER JOIN RelevantReqs r ON rt.REQ_ID = r.REQ_ID
                WHERE rt.RE_ASSIGN_STATUS != 'Re-Assign-Finished'
                GROUP BY
                    rt.REQ_ID, rt.UNDEL_ID, rt.INVOICE_NO, rt.ITEM_CODE
            ),
            PickedQty AS (
                SELECT
                    p.REQ_ID,
                    p.UNDEL_ID,
                    p.INVOICE_NUMBER,
                    p.INVENTORY_ITEM_ID,
                    SUM(p.PICKED_QTY) AS picked_qty
                FROM BUYP.dbo.WHR_PICKED_MAN p
                INNER JOIN RelevantReqs r ON p.REQ_ID = r.REQ_ID
                WHERE p.FLAG NOT IN ('R','OU')
                GROUP BY p.REQ_ID, p.UNDEL_ID, p.INVOICE_NUMBER, p.INVENTORY_ITEM_ID
            )
            SELECT
                dd.id,
                dd.REQ_ID,
                dd.UNDEL_ID,
                dd.INVOICE_NUMBER,
                dd.INVENTORY_ITEM_ID,
                dd.ITEM_DESCRIPTION,
                dd.COMMERCIAL_NO,
                dd.COMMERCIAL_NAME,
                dd.SALESMAN_NO,
                dd.SALESMAN_NAME,
                dd.CUSTOMER_NUMBER,
                dd.CUSTOMER_NAME,
                dd.CUSTOMER_SITE_ID,
                dd.INVOICE_DATE,
                dd.DELIVERY_DATE,
                dd.dis_qty_total,
                dd.dis_mangerQty_total,
                (dd.dis_qty_total - dd.dis_mangerQty_total) AS balance_qty,
                ISNULL(t.previous_truck_qty,0) AS previous_truck_qty,
                ISNULL(r.return_qty,0) AS return_qty,
                ISNULL(p.picked_qty,0) AS picked_qty
            FROM DispatchData dd
            LEFT JOIN TruckQty t
                ON dd.REQ_ID = t.REQ_ID
               AND dd.UNDEL_ID = t.UNDEL_ID
               AND dd.INVOICE_NUMBER = t.INVOICE_NUMBER
               AND dd.INVENTORY_ITEM_ID = t.INVENTORY_ITEM_ID
            LEFT JOIN ReturnQty r
                ON dd.REQ_ID = r.REQ_ID
               AND dd.UNDEL_ID = r.UNDEL_ID
               AND dd.INVOICE_NUMBER = r.INVOICE_NUMBER
               AND dd.INVENTORY_ITEM_ID = r.INVENTORY_ITEM_ID
            LEFT JOIN PickedQty p
                ON dd.REQ_ID = p.REQ_ID
               AND dd.UNDEL_ID = p.UNDEL_ID
               AND dd.INVOICE_NUMBER = p.INVOICE_NUMBER
               AND dd.INVENTORY_ITEM_ID = p.INVENTORY_ITEM_ID
            WHERE dd.dis_qty_total = ISNULL(t.previous_truck_qty, 0)
            ORDER BY dd.REQ_ID DESC, dd.UNDEL_ID, dd.INVOICE_NUMBER
            """

            cursor.execute(query, params)
            rows = cursor.fetchall()

            result_map = {}

            # Processing filtered rows
            for row in rows:
                (
                    row_id, req_id, undel_id, invoice_no, item_id, item_desc,
                    commercial_no, commercial_name,
                    salesman_no, salesman_name,
                    cusno, cusname, cussite,
                    invoice_date, delivery_date,
                    dis_qty, mgr_qty, balance_qty,
                    truck_qty, return_qty, picked_qty
                ) = row

                if req_id not in result_map:
                    result_map[req_id] = {
                        "reqno": req_id,
                        "commercialNo": commercial_no,
                        "commercialName": commercial_name,
                        "salesman_no": salesman_no,
                        "salesmanName": salesman_name,
                        "cusno": cusno,
                        "cusname": cusname,
                        "cussite": cussite,
                        "date": invoice_date,
                        "deliverydate": delivery_date,
                        "items": []
                    }

                result_map[req_id]["items"].append({
                    "undel_id": undel_id,
                    "invoice_no": invoice_no,
                    "inventory_item_id": item_id,
                    "item_description": item_desc,
                    "dis_qty_total": dis_qty,
                    "dis_mangerQty_total": mgr_qty,
                    "balance_qty": balance_qty,
                    "previous_truck_qty": truck_qty,
                    "return_qty": return_qty,
                    "picked_qty": picked_qty
                })

            final_data = list(result_map.values())
            print(f"Total Dispatch Fulfilled Records: {len(final_data)}")
            return JsonResponse(final_data, safe=False)

    except Exception as e:
        return JsonResponse({'status': 'error', 'message': str(e)}, status=500)