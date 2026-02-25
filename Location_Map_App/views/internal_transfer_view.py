
from django.db.models.functions import Length
from django.db import IntegrityError, transaction, connection

from django.http import JsonResponse
from asyncio.log import logger
import base64
import io
import logging
import math
import mimetypes
import random
from time import time
import uuid
from Location_Map_App.views.location_mapping_views import insert_whr_stock
from rest_framework.decorators import api_view
from rest_framework.views import APIView
from rest_framework.response import Response
from django.views.decorators.csrf import csrf_exempt
import json
from django.db import connection, connections
from datetime import date, datetime
from rest_framework import status
import traceback
from django.views.generic import View 
from django.utils.decorators import method_decorator
import re
from rest_framework import  status

from ..models import WHR_MNG_UNIQID_Models
from django.middleware.csrf import get_token
from urllib.parse import unquote
from django.views.decorators.http import require_GET
from django.views.decorators.http import require_http_methods
from django.http import HttpResponse
import os
from django.utils.encoding import smart_str
from rest_framework.parsers import MultiPartParser, FormParser
from django.views.decorators.http import require_POST
from django.db.models import Max

from django.shortcuts import render

from Location_Map_App.models import TIP_UniqId_tbl
# Create your views here.



def _generate_next_ind_id_helper():
    """
    Internal helper to generate the next unique TIP ID.
    Format: TIPYYMM01, TIPYYMM02...
    Returns the string TIP ID.
    """
    while True:
        try:
            # 1. Get current Year (YY) and Month (MM)
            now = datetime.now()
            year_str = now.strftime("%y")  # e.g., '25'
            month_str = now.strftime("%m") # e.g., '12'
            prefix = f"TIP{year_str}{month_str}" # e.g., 'TIP2512'
 
            # 2. Find the latest sequence number for this Month/Year
            last_entry = TIP_UniqId_tbl.objects.filter(
                TIP__startswith=prefix
            ).annotate(
                text_len=Length('TIP')
            ).order_by('-text_len', '-TIP').first()
 
            if last_entry:
                # Extract the part after the prefix
                last_no = last_entry.TIP
                sequence_str = last_no[len(prefix):]
                if sequence_str.isdigit():
                    next_seq = int(sequence_str) + 1
                else:
                    next_seq = 1
            else:
                next_seq = 1
 
            # 3. Format the new No with zero-padding (2 digits)
            new_ind_id = f"{prefix}{next_seq:02d}"
 
            # 4. Attempt to create the record (Handles Race Conditions via unique constraint)
            with transaction.atomic():
                TIP_UniqId_tbl.objects.create(TIP=new_ind_id)
           
            return new_ind_id
 
        except IntegrityError:
            # Duplicate found! The loop will restart,
            # fetch the NEW latest entry, and try the next number.
            continue




# @csrf_exempt
# def insert_internal_transfer_details(request):
#     """
#     Handles internal stock transfer process.
#     Step 1: Insert into LOC_INTERNAL_TRANSFER_DETAILS_TBL
#     Scenario 1: IF Stock Available -> Transfer Logic (Source Decr, Dest Incr, New Dest Rows)
#     Scenario 2: IF Stock NOT Available -> Insert New Stock (Header & Details) directly
#     """
    
#     if request.method != 'POST':
#         return JsonResponse({'status': 'error', 'message': 'Invalid method'}, status=405)

#     try:
        
#         uniq_id = _generate_next_ind_id_helper()
#         data = json.loads(request.body)
        
#         # Extract Header fields
#         whr_superuser_no = data.get('WHR_SuperUserNo')
#         whr_superuser_name = data.get('WHR_Super_User_Name')
#         org_id = data.get('Org_ID')
#         org_name = data.get('Warehouse_Name')
#         created_by = data.get('Created_By')
        
#         from_whr_barcode = data.get('Location_Barcode') # Header barcode
        
#         # Attempt to get To Location from body
#         to_whr_code = data.get('To_Location_Code') or data.get('To_Location') or ''
#         to_whr_barcode = data.get('To_Location_Barcode') or ''
        
#         table_details = data.get('Table_Details', [])
        
#         if not table_details:
#              return JsonResponse({'status': 'error', 'message': 'No items in transfer'}, status=400)

#         # Inject missing header level details from the first item
#         if table_details:
#             first_item = table_details[0]
#             data['Item_meas'] = first_item.get('Item_meas', 0)
#             data['tot_item_meas'] = first_item.get('tot_item_meas', 0)
#             data['Franchise'] = first_item.get('Franchise', '')
#             data['Item_class'] = first_item.get('Item_class', '')
#             data['Sub_Class'] = first_item.get('Sub_Class', '')
#             data['productCode'] = first_item.get('productCode', '')
#             data['itemCode'] = first_item.get('itemCode', '')
#             data['itemDetails'] = first_item.get('itemDetails', '')
#             data['product_Type'] = first_item.get('product_Type', '')
#             data['quantity'] = first_item.get('quantity', 0)
#             data['To_Location_Code'] = to_whr_code
#             data['To_Location_Barcode'] = to_whr_barcode
#             data['from_Location_Barcode'] = from_whr_barcode
#             data['from_Location_Code'] = first_item.get('Location_Code', '')

#         # ----------------------------------------------------
#         # Step 1: Insert into LOC_INTERNAL_TRANSFER_DETAILS_TBL
#         # ----------------------------------------------------
#         insert_sql = """
#             INSERT INTO [BUYP].[dbo].[LOC_INTERNAL_TRANSFER_DETAILS_TBL] ([TIP_UNIQUE_ID],
#                 [WHR_SUPERUSER_NO], [WHR_SUPERUSER_NAME], [ORG_ID], [ORG_NAME],
#                 [FROM_WHR_CODE], [FROM_WHR_BARCODE], 
#                 [TO_WHR_CODE], [TO_WHR_BARCODE],
#                 [ITEM_CODE], [ITEM_DESCRIPTION], [TRANSFER_QTY],
#                 [CREATION_DATE], [CREATION_BY]
#             ) VALUES (
#                 %s, %s, %s, %s, %s,
#                 %s, %s,
#                 %s, %s,
#                 %s, %s, %s,
#                 GETDATE(), %s
#             )
#         """
        
#         all_stock_available = True
#         stock_check_data = [] # Store tuple (loc, item, qty, uniq_id) for Scenario 1 processing
        
#         with connection.cursor() as cursor:
#             # 1. Insert Loop
#             for item in table_details:
#                 item_vals = [
#                     uniq_id,
#                     whr_superuser_no,
#                     whr_superuser_name,
#                     org_id,
#                     org_name,
#                     item.get('Location_Code'),    # FROM_WHR_CODE
#                     from_whr_barcode,             # FROM_WHR_BARCODE
#                     to_whr_code,
#                     to_whr_barcode,
#                     item.get('itemCode'),
#                     item.get('itemDetails'),
#                     item.get('quantity'),         # TRANSFER_QTY
#                     created_by        
#                 ]
#                 cursor.execute(insert_sql, item_vals)

#             # 2. Check Stock Availability AND Fetch Uniq_Id
#             for item in table_details:
#                 loc_code = item.get('Location_Code')
#                 item_code = item.get('itemCode')
#                 transfer_qty = int(item.get('quantity', 0))
                
#                 # Fetch Count AND Uniq_Id (Using MIN/TOP 1 to get one valid ID if multiple exist, 
#                 # grouping by Uniq_Id usually usually implies checking specific batch, but here we just need ONE valid batch to transfer from)
#                 # Requirement: "The unique ID used for updating... must be taken from... check"
#                 # We need to pick ONE Uniq_Id that has enough stock? 
#                 # Or just check total stock and pick ANY Uniq_Id?
#                 # User query: "SELECT COUNT(*) , [Uniq_Id] ... GROUP BY [Uniq_Id]"? No user query was simple select.
#                 # If we have multiple Uniq_Ids (batches), simple Select Count(*), Uniq_Id might fail or return one random.
#                 # We will use TOP 1 Uniq_Id that has stock.
                
#                 check_sql = """
#                     SELECT TOP 1 COUNT(*) OVER (), [Uniq_Id]
#                     FROM [BUYP].[dbo].[WHR_Stock_Details_tbl]
#                     WHERE [Location_Code] = %s 
#                       AND [Item_Code] = %s 
#                       AND [Stock_status] = 'Available'
#                     GROUP BY [Uniq_Id]
#                 """
#                 # Actually, `COUNT(*) OVER()` with TOP 1 is good trick. 
#                 # But simple aggregate is safer if we just want total count.
#                 # User's snippet: `SELECT COUNT(*) , [Uniq_Id]` -> imply group by? Or just implicit?
#                 # SQL Server requires GROUP BY for non-agg columns.
#                 # Let's try to get Total Count and First Uniq_Id.
                
#                 check_sql_safe = """
#                      SELECT (
#                         SELECT COUNT(*) FROM [BUYP].[dbo].[WHR_Stock_Details_tbl]
#                         WHERE [Location_Code] = %s AND [Item_Code] = %s AND [Stock_status] = 'Available'
#                      ) as TotalIdx, 
#                      (
#                         SELECT TOP 1 [Uniq_Id] FROM [BUYP].[dbo].[WHR_Stock_Details_tbl]
#                         WHERE [Location_Code] = %s AND [Item_Code] = %s AND [Stock_status] = 'Available'
#                      ) as UniqId
#                 """
                
#                 cursor.execute(check_sql_safe, [loc_code, item_code, loc_code, item_code])
#                 row = cursor.fetchone()
                
#                 available_count = row[0] if row else 0
#                 uniq_id = row[1] if row else None
                
#                 if available_count < transfer_qty:
#                     all_stock_available = False
#                     break 
                
#                 stock_check_data.append({
#                     'item': item,
#                     'uniq_id': uniq_id
#                 })
        
#         # ----------------------------------------------------
#         # Scenario Processing
#         # ----------------------------------------------------
#         if all_stock_available:
#             # Scenario 1: Stock is available.
#             # 1. Update Source (Decrement)
#             with connection.cursor() as cursor:
#                 for entry in stock_check_data:
#                     item = entry['item']
#                     uniq_id = entry['uniq_id']
                    
#                     loc_code = item.get('Location_Code')
#                     item_code = item.get('itemCode')
#                     transfer_qty = int(item.get('quantity', 0))
                    
#                     # 1. Update Source Header (Decrement)
#                     update_src_header_sql = """
#                         UPDATE [BUYP].[dbo].[WHR_Stock_Management_Header_tbl]
#                         SET [Total_Item_Qty] = [Total_Item_Qty] - %s
#                         WHERE [ID] = (
#                             SELECT TOP 1 [ID]
#                             FROM [BUYP].[dbo].[WHR_Stock_Management_Header_tbl]
#                             WHERE [Location_Code] = %s AND [Item_Code] = %s
#                             ORDER BY [ID] DESC
#                         )
#                     """
#                     cursor.execute(update_src_header_sql, [transfer_qty, loc_code, item_code])
                    
#                     # 2. Update Source Details (Status = 'Internal Transfer')
#                     update_src_details_sql = f"""
#                         UPDATE TOP ({transfer_qty}) [BUYP].[dbo].[WHR_Stock_Details_tbl]
#                         SET [Stock_status] = 'Internal Transfer'
#                         WHERE [Location_Code] = %s 
#                           AND [Item_Code] = %s 
#                           AND [Stock_status] = 'Available'
#                           AND [Uniq_Id] = %s
#                     """
#                     cursor.execute(update_src_details_sql, [loc_code, item_code, uniq_id])
            
#             # 2. Insert into Destination (using insert_whr_stock_serial)
#             # We need to modify the request body to point to the Destination Location
#             # so that insert_whr_stock_serial uses the To_Location as the Target.
            
#             # Update data dictionary with To_Location headers
#             data['Location_Code'] = to_whr_code
#             data['Location_Barcode'] = to_whr_barcode
            
#             # Also update items in Table_Details to have the new Location_Code
#             # (Though insert_whr_stock_serial reads Location_Code from item, so we must update items)
#             for item in table_details:
#                 item['Location_Code'] = to_whr_code
            
#             # Update Table_Details in data
#             data['Table_Details'] = table_details
            
#             # Re-serialize to bytes and assign to request._body
#             request._body = json.dumps(data).encode('utf-8')
            
#             return insert_whr_stock(request)
            
#         else:
#             # Scenario 2: Stock NOT Available.
#             # Directly call the existing insert view as per user request
#             request._body = json.dumps(data).encode('utf-8')
#             return insert_whr_stock(request)

#     except Exception as e:
#         print(f"❌ Error in insert_internal_transfer_details: {str(e)}")
#         return JsonResponse({'status': 'error', 'message': str(e)}, status=500)





@csrf_exempt
def insert_internal_transfer_details(request):
    """
    Handles internal stock transfer process.
    Step 1: Insert into LOC_INTERNAL_TRANSFER_DETAILS_TBL
    Scenario 1: IF Stock Available -> Transfer Logic (Source Decr, Dest Incr, New Dest Rows)
    Scenario 2: IF Stock NOT Available -> Insert New Stock (Header & Details) directly
    """
    if request.method != 'POST':
        return JsonResponse({'status': 'error', 'message': 'Invalid method'}, status=405)

    try:
        
        trnasper_uniq_id = _generate_next_ind_id_helper()
        data = json.loads(request.body)
        
        # Extract Header fields
        whr_superuser_no = data.get('WHR_SuperUserNo')
        whr_superuser_name = data.get('WHR_Super_User_Name')
        org_id = data.get('Org_ID')
        org_name = data.get('Warehouse_Name')
        created_by = data.get('Created_By')
        
        from_whr_barcode = data.get('Location_Barcode') # Header barcode
        
        # Attempt to get To Location from body
        to_whr_code = data.get('To_Location_Code') or data.get('To_Location') or ''
        to_whr_barcode = data.get('To_Location_Barcode') or ''
        
        table_details = data.get('Table_Details', [])
        
        if not table_details:
             return JsonResponse({'status': 'error', 'message': 'No items in transfer'}, status=400)

        # Inject missing header level details from the first item
        if table_details:
            first_item = table_details[0]
            data['Item_meas'] = first_item.get('Item_meas', 0)
            data['tot_item_meas'] = first_item.get('tot_item_meas', 0)
            data['Franchise'] = first_item.get('Franchise', '')
            data['Item_class'] = first_item.get('Item_class', '')
            data['Sub_Class'] = first_item.get('Sub_Class', '')
            data['productCode'] = first_item.get('productCode', '')
            data['itemCode'] = first_item.get('itemCode', '')
            data['itemDetails'] = first_item.get('itemDetails', '')
            data['product_Type'] = first_item.get('product_Type', '')
            data['quantity'] = first_item.get('quantity', 0)
            data['To_Location_Code'] = to_whr_code
            data['To_Location_Barcode'] = to_whr_barcode
            data['from_Location_Barcode'] = from_whr_barcode
            data['from_Location_Code'] = first_item.get('Location_Code', '')

        # ----------------------------------------------------
        # Step 1: Insert into LOC_INTERNAL_TRANSFER_DETAILS_TBL
        # ----------------------------------------------------
        insert_sql = """
            INSERT INTO [BUYP].[dbo].[LOC_INTERNAL_TRANSFER_DETAILS_TBL] ([TIP_UNIQUE_ID],
                [WHR_SUPERUSER_NO], [WHR_SUPERUSER_NAME], [ORG_ID], [PHYSICAL_WHR],
                [FROM_WHR_CODE], [FROM_WHR_BARCODE], 
                [TO_WHR_CODE], [TO_WHR_BARCODE],
                [ITEM_CODE], [ITEM_DESCRIPTION], [TRANSFER_QTY],
                [CREATION_DATE], [CREATION_BY],
                [ORG_NAME], [STOCK_MNG_ID], [AVAILABLE_QTY]
            ) VALUES (
                %s, %s, %s, %s,%s,
                %s, %s,
                %s, %s,
                %s, %s, %s,
                GETDATE(), %s,
                %s, %s, %s
            )
        """
        
        all_stock_available = True
        stock_check_data = [] # Store tuple for Scenario 1 processing
        
        with connection.cursor() as cursor:
            # 1. Check Stock Availability AND Fetch Uniq_Id, Org_Name, Available_Qty
            for item in table_details:
                loc_code = item.get('Location_Code')
                item_code = item.get('itemCode')
                transfer_qty = int(item.get('quantity', 0))
                
                check_sql_safe = """
                     SELECT (
                        SELECT COUNT(*) FROM [BUYP].[dbo].[WHR_Stock_Details_tbl]
                        WHERE [Location_Code] = %s AND [Item_Code] = %s AND [Stock_status] = 'Available'
                     ) as TotalIdx, 
                     (
                        SELECT TOP 1 [Uniq_Id] FROM [BUYP].[dbo].[WHR_Stock_Details_tbl]
                        WHERE [Location_Code] = %s AND [Item_Code] = %s AND [Stock_status] = 'Available'
                     ) as UniqId,
                     (
                        SELECT TOP 1 [Org_Name] FROM [BUYP].[dbo].[WHR_Stock_Details_tbl]
                        WHERE [Location_Code] = %s AND [Item_Code] = %s AND [Stock_status] = 'Available'
                     ) as OrgName
                """
                
                cursor.execute(check_sql_safe, [loc_code, item_code, loc_code, item_code, loc_code, item_code])
                row = cursor.fetchone()
                
                available_count = row[0] if row else 0
                uniq_id = row[1] if row else None
                fetched_org_name = row[2] if row else ''
                
                if available_count < transfer_qty:
                    all_stock_available = False
                    break 
                
                stock_check_data.append({
                    'item': item,
                    'uniq_id': uniq_id,
                    'fetched_org_name': fetched_org_name,
                    'available_count': available_count
                })

            # 2. Insert into LOC_INTERNAL_TRANSFER_DETAILS_TBL
            for entry in stock_check_data:
                item = entry['item']
                item_vals = [
                    trnasper_uniq_id,
                    whr_superuser_no,
                    whr_superuser_name,
                    org_id,
                    org_name,                     # PHYSICAL_WHR
                    item.get('Location_Code'),    # FROM_WHR_CODE
                    from_whr_barcode,             # FROM_WHR_BARCODE
                    to_whr_code,
                    to_whr_barcode,
                    item.get('itemCode'),
                    item.get('itemDetails'),
                    item.get('quantity'),         # TRANSFER_QTY
                    created_by,
                    entry['fetched_org_name'],    # ORG_NAME
                    entry['uniq_id'],             # STOCK_MNG_ID
                    entry['available_count']      # AVAILABLE_QTY
                ]
                cursor.execute(insert_sql, item_vals)
        
        # ----------------------------------------------------
        # Scenario Processing
        # ----------------------------------------------------
        if all_stock_available:
            # Scenario 1: Stock is available.
            # 1. Update Source (Decrement)
            with connection.cursor() as cursor:
                for entry in stock_check_data:
                    item = entry['item']
                    uniq_id = entry['uniq_id']
                    
                    loc_code = item.get('Location_Code')
                    item_code = item.get('itemCode')
                    transfer_qty = int(item.get('quantity', 0))
                    
                    # 1. Update Source Header (Decrement)
                    update_src_header_sql = """
                        UPDATE [BUYP].[dbo].[WHR_Stock_Management_Header_tbl]
                        SET [Total_Item_Qty] = [Total_Item_Qty] - %s
                        WHERE [ID] = (
                            SELECT TOP 1 [ID]
                            FROM [BUYP].[dbo].[WHR_Stock_Management_Header_tbl]
                            WHERE [Location_Code] = %s AND [Item_Code] = %s
                            ORDER BY [ID] DESC
                        )
                    """
                    cursor.execute(update_src_header_sql, [transfer_qty, loc_code, item_code])
                    
                    # 2. Update Source Details (Status = 'Internal Transfer')
                    update_src_details_sql = f"""
                        UPDATE TOP ({transfer_qty}) [BUYP].[dbo].[WHR_Stock_Details_tbl]
                        SET [Stock_status] = 'Internal Transfer'
                        WHERE [Location_Code] = %s 
                          AND [Item_Code] = %s 
                          AND [Stock_status] = 'Available'
                          AND [Uniq_Id] = %s
                    """
                    cursor.execute(update_src_details_sql, [loc_code, item_code, uniq_id])
            
            # 2. Insert into Destination (using insert_whr_stock_serial)
            # We need to modify the request body to point to the Destination Location
            # so that insert_whr_stock_serial uses the To_Location as the Target.
            
            # Update data dictionary with To_Location headers
            data['Location_Code'] = to_whr_code
            data['Location_Barcode'] = to_whr_barcode
            
            # Also update items in Table_Details to have the new Location_Code
            # (Though insert_whr_stock_serial reads Location_Code from item, so we must update items)
            for item in table_details:
                item['Location_Code'] = to_whr_code
            
            # Update Table_Details in data
            data['Table_Details'] = table_details
            
            # Re-serialize to bytes and assign to request._body
            request._body = json.dumps(data).encode('utf-8')
            
            return insert_whr_stock(request)
            
        else:
            # Scenario 2: Stock NOT Available.
            # Directly call the existing insert view as per user request
            request._body = json.dumps(data).encode('utf-8')
            return insert_whr_stock(request)

    except Exception as e:
        print(f"❌ Error in insert_internal_transfer_details: {str(e)}")
        return JsonResponse({'status': 'error', 'message': str(e)}, status=500)
    


@api_view(['GET'])
def get_internal_transfer_details(request):
    """
    API view to get all internal transfer details (header and linelevel items).
    Groups them by their distinct TIP_UNIQUE_ID.
    Supports pagination to fetch 1000 items at a time.
    """
    try:
        page = int(request.query_params.get('page', 1000000))
        limit = int(request.query_params.get('limit', 1000000))
    except ValueError:
        page = 1000000
        limit = 1000000
 
    offset = (page - 1000000) * limit
 
    query = """
    SELECT
        [TIP_UNIQUE_ID],
        [WHR_SUPERUSER_NO],
        [WHR_SUPERUSER_NAME],
        [ORG_ID],
        [ORG_NAME],
        [CREATION_DATE],
        [PHYSICAL_WHR],
        [FROM_WHR_CODE],
        [FROM_WHR_BARCODE],
        [TO_WHR_CODE],
        [TO_WHR_BARCODE],
        [STOCK_MNG_ID],
        [ITEM_CODE],
        [ITEM_DESCRIPTION],
        [AVAILABLE_QTY],
        [TRANSFER_QTY]
    FROM [BUYP].[dbo].[LOC_INTERNAL_TRANSFER_DETAILS_TBL]
    ORDER BY [CREATION_DATE] DESC
    OFFSET %s ROWS FETCH NEXT %s ROWS ONLY
    """
 
    try:
        with connection.cursor() as cursor:
            cursor.execute(query, [offset, limit])
            columns = [col[0] for col in cursor.description]
            rows = [dict(zip(columns, r)) for r in cursor.fetchall()]
 
            if not rows:
                return Response([], status=status.HTTP_200_OK)
           
            def get_str(val, default=""):
                return str(val) if val is not None else default
 
            grouped_data = {}
            for row in rows:
                t_id = get_str(row.get('TIP_UNIQUE_ID'))
                if t_id not in grouped_data:
                    creation_date_val = row.get('CREATION_DATE')
                    if hasattr(creation_date_val, 'strftime'):
                        creation_date_str = creation_date_val.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                    else:
                        creation_date_str = str(creation_date_val) if creation_date_val else ""
 
                    grouped_data[t_id] = {
                        "TIP_UNIQUE_ID": t_id,
                        "WHR_SUPERUSER_NO": get_str(row.get('WHR_SUPERUSER_NO')),
                        "WHR_SUPERUSER_NAME": get_str(row.get('WHR_SUPERUSER_NAME')),
                        "ORG_ID": get_str(row.get('ORG_ID')),
                        "ORG_NAME": get_str(row.get('ORG_NAME')),
                        "CREATION_DATE": creation_date_str,
                        "PHYSICAL_WHR": get_str(row.get('PHYSICAL_WHR')),
                        "FROM_WHR_CODE": get_str(row.get('FROM_WHR_CODE')),
                        "FROM_WHR_BARCODE": get_str(row.get('FROM_WHR_BARCODE')),
                        "TO_WHR_CODE": get_str(row.get('TO_WHR_CODE')),
                        "TO_WHR_BARCODE": get_str(row.get('TO_WHR_BARCODE')),
                        "linelevel_details": []
                    }
 
                line_item = {
                    "STOCK_MNG_ID": get_str(row.get('STOCK_MNG_ID')),
                    "ITEM_CODE": get_str(row.get('ITEM_CODE')),
                    "ITEM_DESCRIPTION": get_str(row.get('ITEM_DESCRIPTION')),
                    "AVAILABLE_QTY": get_str(row.get('AVAILABLE_QTY'), "0"),
                    "TRANSFER_QTY": get_str(row.get('TRANSFER_QTY'), "0")
                }
                grouped_data[t_id]["linelevel_details"].append(line_item)
               
            result_list = list(grouped_data.values())
            return Response(result_list, status=status.HTTP_200_OK)
 
    except Exception as e:
        return Response({"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)