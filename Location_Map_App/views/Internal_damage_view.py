
from django.db import IntegrityError, transaction, connection
from ALJE_PROJECT import settings
from minio import Minio
from django.db.models.functions import Length
from datetime import datetime ,timedelta
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
from ALJE_PROJECT.settings import MINIO_INBOUND_CONTAINER_QC_BUCKET_NAME, MINIO_INBOUND_PO_BUCKET_NAME,MINIO_BUCKET_INTERNAL_DAMAGE

from Location_Map_App.models import IDP_UniqId_tbl
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

# Create your views here.



#-----------------------------------------------------------------------------------------------------
                # Internal Damage details #
#-----------------------------------------------------------------------------------------------------


@csrf_exempt
def check_item_location_status_query(request):
    if request.method != 'POST':
        return JsonResponse(
            {"status": "error", "message": "POST method required"},
            status=405
        )

    try:
        body = json.loads(request.body)

        item_code = body.get("item_code")
        product_code = body.get("product_code")
        serial_no = body.get("serial_no")

        # 🔴 Validation
        if not item_code or not product_code or not serial_no:
            return JsonResponse(
                {
                    "status": "error",
                    "message": "item_code, product_code and serial_no are required"
                },
                status=400
            )

        with connection.cursor() as cursor:

            # 1️⃣ Get all locations where ITEM exists
            cursor.execute("""
                SELECT DISTINCT Location_Code
                FROM WHR_Stock_Details_tbl
                WHERE Item_Code = %s
                  AND Stock_status = 'Available'
            """, [item_code])

            item_locations = [row[0] for row in cursor.fetchall()]

            # 2️⃣ Get location where PRODUCT + SERIAL exists
            cursor.execute("""
                SELECT Location_Code
                FROM WHR_Stock_Details_tbl
                WHERE Item_Code = %s
                  AND Product_Code = %s
                  AND SerialNo = %s
                  AND Stock_status = 'Available'
            """, [item_code, product_code, serial_no])

            matched_row = cursor.fetchone()
            matched_location = matched_row[0] if matched_row else None

        # 3️⃣ Prepare response
        data = []
        for loc in item_locations:
            data.append({
                "location_code": loc,
                "status": True if loc == matched_location else False
            })

        return JsonResponse(
            {
                "status": "success",
                "data": data
            },
            safe=False
        )

    except Exception as e:
        return JsonResponse(
            {"status": "error", "message": str(e)},
            status=500
        )



def _generate_next_ind_id_helper():
    """
    Internal helper to generate the next unique IDP ID.
    Format: IDPYYMM01, IDPYYMM02...
    Returns the string IDP ID.
    """
    while True:
        try:
            # 1. Get current Year (YY) and Month (MM)
            now = datetime.now()
            year_str = now.strftime("%y")  # e.g., '25'
            month_str = now.strftime("%m") # e.g., '12'
            prefix = f"IDP{year_str}{month_str}" # e.g., 'IDP2512'
 
            # 2. Find the latest sequence number for this Month/Year
            last_entry = IDP_UniqId_tbl.objects.filter(
                IDP__startswith=prefix
            ).annotate(
                text_len=Length('IDP')
            ).order_by('-text_len', '-IDP').first()
 
            if last_entry:
                # Extract the part after the prefix
                last_no = last_entry.IDP
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
                IDP_UniqId_tbl.objects.create(IDP=new_ind_id)
           
            return new_ind_id
 
        except IntegrityError:
            # Duplicate found! The loop will restart,
            # fetch the NEW latest entry, and try the next number.
            continue


# -------------------------------------------------------------------------
# INSERT INTERNAL DAMAGE DETAILS (MinIO + SQL)
# -------------------------------------------------------------------------
@api_view(['POST'])
def insert_internal_damage_details(request):
    """
    Inserts internal damage details with image upload to MinIO.
    Logic:
    1. Generate Unique ID (IND...).
    2. Upload images to MinIO with dynamic folder structure.
    3. Insert data into LOC_Internal_Damage_Details_tbl.
    """
    try:
        # 1. Generate Unique ID
        uniq_id = _generate_next_ind_id_helper()

        # 2. MinIO Configuration
        minio_endpoint = settings.MINIO_ENDPOINT
        access_key = settings.MINIO_ACCESS_KEY
        secret_key = settings.MINIO_SECRET_KEY
        bucket_name =MINIO_BUCKET_INTERNAL_DAMAGE
        
        client = Minio(
            minio_endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=False
        )
        
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)

        items = request.data
        if not items or not isinstance(items, list):
             return Response({"status": "error", "message": "Body must be a list of items"}, status=status.HTTP_400_BAD_REQUEST)

        insert_params = []
        created_date = datetime.now()
        created_by = items[0].get('Created_By') if items else None 
        ip_address = request.META.get('REMOTE_ADDR')

        # Tracker for set-N logic within this batch
        # Key: (Item_Code, Product_Code, Serail_No) -> Count
        item_tracker = {}

        for item in items:
            item_code = item.get('Item_Code')
            product_code = item.get('Product_Code')
            serial_no = item.get('Serail_No')
            
            # Tracker Key
            key = (item_code, product_code, serial_no)
            
            if key in item_tracker:
                item_tracker[key] += 1
            else:
                item_tracker[key] = 1
            
            set_n_val = item_tracker[key]
            
            # Folder Path Construction
            # Uniq_Id/Item_Code/ProductCode-SerialNo/set-N/
            folder_path = f"{uniq_id}/{item_code}/{product_code}-{serial_no}/set-{set_n_val}/"
            
            # Handle Image Uploads
            image_details = item.get('imageDetails', [])
            if image_details:
                for idx, img_str in enumerate(image_details):
                    try:
                        content_type = 'image/jpeg'
                        extension = '.jpg'
                        encoded = img_str

                        # Decode Base64
                        if ',' in img_str:
                            header, encoded = img_str.split(',', 1)
                            # header example: data:image/png;base64
                            if 'data:' in header and ';base64' in header:
                                try:
                                    mime_raw = header.split(':')[1].split(';')[0]
                                    if mime_raw:
                                        content_type = mime_raw
                                        # Simple extension mapping
                                        if 'png' in mime_raw: extension = '.png'
                                        elif 'jpeg' in mime_raw or 'jpg' in mime_raw: extension = '.jpg'
                                        elif 'gif' in mime_raw: extension = '.gif'
                                        elif 'webp' in mime_raw: extension = '.webp'
                                        elif 'bmp' in mime_raw: extension = '.bmp'
                                except:
                                    pass # Fallback to defaults

                        # Basic validation
                        img_bytes = base64.b64decode(encoded)
                        # Ensure its not empty
                        if len(img_bytes) > 0:
                            # If no header provided, try to detect from bytes
                            if extension == '.jpg' and content_type == 'image/jpeg' and not ',' in img_str:
                                if img_bytes.startswith(b'\x89PNG\r\n\x1a\n'):
                                    content_type = 'image/png'; extension = '.png'
                                elif img_bytes.startswith(b'GIF87a') or img_bytes.startswith(b'GIF89a'):
                                    content_type = 'image/gif'; extension = '.gif'
                                elif img_bytes.startswith(b'RIFF') and img_bytes[8:12] == b'WEBP':
                                    content_type = 'image/webp'; extension = '.webp'
                                elif img_bytes.startswith(b'BM'):
                                    content_type = 'image/bmp'; extension = '.bmp'

                            img_stream = io.BytesIO(img_bytes)
                            
                            # Generate Filename
                            img_name = f"image_{idx+1}{extension}"
                            object_name = f"{folder_path}{img_name}"
                            
                            client.put_object(
                                bucket_name,
                                object_name,
                                img_stream,
                                length=len(img_bytes),
                                content_type=content_type 
                            )
                    except Exception as img_err:
                        print(f"Error uploading image {idx} for {key}: {img_err}")
                        pass
            
            # Prepare Data for Insert
            row = (
                uniq_id,
                item.get('Org_ID'),
                item.get('Org_Name'),
                item.get('Warhouse_Name'),
                item.get('WHR_Superuser_No'),
                item.get('WHR_SuperUser_Name'),
                item_code,
                item.get('Item_Ddescription'), 
                product_code,
                serial_no,
                item.get('Franchse'),
                item.get('Item_Class'),
                item.get('Sub_Class'),
                item.get('Prod_Part'),
                item.get('Location_Code'),
                folder_path, # Image_Minio_location
                item.get('Created_By'), 
                created_date,
                ip_address,
                None, # Update_By
                None, # Update_Date
                None, # Update_IP
                item.get('Atrribute1'),
                item.get('Attribute2'),
                item.get('Attribute3'),
                item.get('Atrribute4'),
                item.get('Attribute5'),
                item.get('Flag1'),
                item.get('Flag2'),
                item.get('Remarks'),
            )
            insert_params.append(row)

        if insert_params:
            with connection.cursor() as cursor:
                insert_sql = """
                    INSERT INTO [BUYP].[dbo].[LOC_Internal_Damage_Details_tbl] (
                        [Uniq_Id], [Org_ID], [Org_Name], [Warhouse_Name], 
                        [WHR_Superuser_No], [WHR_SuperUser_Name], [Item_Code], 
                        [Item_Ddescription], [Product_Code], [Serail_No], 
                        [Franchse], [Item_Class], [Sub_Class], [Prod_Part], 
                        [Location_Code], [Image_Minio_location], [Created_By], 
                        [Created_date], [IP], [Update_By], [Update_Date], 
                        [Update_IP], [Atrribute1], [Attribute2], [Attribute3], 
                        [Atrribute4], [Attribute5], [Flag1], [Flag2],[Remarks]
                    ) VALUES (
                        %s, %s, %s, %s, 
                        %s, %s, %s, 
                        %s, %s, %s, 
                        %s, %s, %s, %s, 
                        %s, %s, %s, 
                        %s, %s, %s, %s, 
                        %s, %s, %s, %s, 
                        %s, %s, %s, %s, %s
                    )
                """
                cursor.executemany(insert_sql, insert_params)

                # ---------------------------------------------------------
                # 2. UPDATE STOCK STATUS (Logic: Select TOP 1 Available -> Update)
                # ---------------------------------------------------------
                for item in items:
                    try:
                        loc_code = item.get('Location_Code')
                        item_code = item.get('Item_Code')
                        prod_code = item.get('Product_Code')
                        serial_no = item.get('Serail_No')

                        # ---------------------------------------------------------
                        # Logic:
                        # 1. Try to find match WITH SerialNo (if provided)
                        # 2. If not found, try to find match WITHOUT SerialNo (Top 1 Available)
                        # ---------------------------------------------------------
                        
                        stock_id = None
                        
                        # Attempt 1: Strict Match (Location + Item + Product + SerialNo)
                        if serial_no:
                            query_strict = """
                                SELECT TOP 1 id FROM [BUYP].[dbo].[WHR_Stock_Details_tbl]
                                WHERE Location_Code = %s 
                                AND Item_Code = %s 
                                AND Product_Code = %s 
                                AND SerialNo = %s
                                AND Stock_status = 'Available'
                            """
                            cursor.execute(query_strict, [loc_code, item_code, prod_code, serial_no])
                            row = cursor.fetchone()
                            if row:
                                stock_id = row[0]

                        # Attempt 2: Loose Match (If strict failed or no serial provided)
                        if not stock_id:
                            query_loose = """
                                SELECT TOP 1 id FROM [BUYP].[dbo].[WHR_Stock_Details_tbl]
                                WHERE Location_Code = %s 
                                AND Item_Code = %s 
                                AND Product_Code = %s 
                                AND Stock_status = 'Available' order by id desc
                            """
                            cursor.execute(query_loose, [loc_code, item_code, prod_code])
                            row = cursor.fetchone()
                            if row:
                                stock_id = row[0]

                        # Update if ID found
                        if stock_id:
                            cursor.execute(
                                "UPDATE [BUYP].[dbo].[WHR_Stock_Details_tbl] SET Stock_status = 'Internal Damage Partically' WHERE id = %s",
                                [stock_id]
                            )
                    except Exception as e_stock:
                        print(f"Stock Update Error for {item.get('Item_Code')}: {e_stock}")

        return Response({
            "status": "success",
            "message": "Data inserted and images uploaded successfully",
            "Uniq_Id": uniq_id,
            "rows_inserted": len(insert_params)
        }, status=status.HTTP_201_CREATED)

    except Exception as e:
        return Response(
            {"status": "error", "message": str(e), "trace": traceback.format_exc()},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )


from concurrent.futures import ThreadPoolExecutor

@api_view(['GET'])
def get_internal_damage_details(request):
    try:
        # 1. Parse Pagination Params
        page = int(request.query_params.get('page', 1))
        page_size = int(request.query_params.get('limit', 1000000))
        offset = (page - 1) * page_size

        # 2. MinIO Configuration
        minio_endpoint = settings.MINIO_ENDPOINT
        access_key = settings.MINIO_ACCESS_KEY
        secret_key = settings.MINIO_SECRET_KEY
        bucket_name = MINIO_BUCKET_INTERNAL_DAMAGE
        # -------------------------------------------------
        # 2. Status Param
        # -------------------------------------------------
        damage_status = request.query_params.get('status')  # may be None

        # Use a global or cached client if possible, mostly thread-safe
        client = Minio(
            minio_endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=False
        )

        grouped_data = {}

        with connection.cursor() as cursor:
            # OPTIMIZED SQL: Fetch only necessary range
            # Note: OFFSET-FETCH is standard SQL (SQL Server 2012+), assuming MSSQL based on [BUYP].[dbo]
            # If older version, use row_number(). usage: ORDER BY Uniq_Id OFFSET X ROWS FETCH NEXT Y ROWS ONLY
            
            # -------------------------------------------------
            # 4. Total Count (apply filter only if needed)
            # -------------------------------------------------
            if damage_status == "Need_To_Approval":
                cursor.execute("""
                    SELECT COUNT(*)
                    FROM [BUYP].[dbo].[LOC_Internal_Damage_Details_tbl]
                    WHERE Damage_Status IS NULL
                """)
            else:
                cursor.execute("""
                    SELECT COUNT(*)
                    FROM [BUYP].[dbo].[LOC_Internal_Damage_Details_tbl]
                """)
            total_records = cursor.fetchone()[0]
            # -------------------------------------------------
            # 5. Dynamic Query Builder
            # -------------------------------------------------
            where_clause = ""
            params = []

            if damage_status == "Need_To_Approval":
                where_clause = "WHERE Damage_Status IS NULL"
            # Fetch Paginated Data
            # We order by Uniq_Id to ensure consistent pagination group-wise
            query = f"""
                SELECT * FROM [BUYP].[dbo].[LOC_Internal_Damage_Details_tbl] {where_clause}
                ORDER BY Uniq_Id DESC, Item_Code ASC
                OFFSET {offset} ROWS FETCH NEXT {page_size} ROWS ONLY
            """
            cursor.execute(query)
            rows = cursor.fetchall()
            columns = [col[0] for col in cursor.description]
            
            # Helper function for parallel MinIO fetching
            def fetch_minio_urls(row_dict):
                image_location = row_dict.get('Image_Minio_location')
                if not image_location: return []
                
                urls = []
                try:
                    # Only check if location string seems valid to avoid useless calls
                    objects = client.list_objects(bucket_name, prefix=image_location, recursive=True)
                    for obj in objects:
                        url = client.get_presigned_url(
                            "GET",
                            bucket_name,
                            obj.object_name,
                            expires=timedelta(hours=1),
                        )
                        urls.append(url)
                except Exception:
                    pass # Fail silently or log
                return urls

            # Prepare data list for processing
            raw_data = [dict(zip(columns, row)) for row in rows]
            
            # Execute MinIO fetches in parallel for this batch
            with ThreadPoolExecutor(max_workers=10) as executor:
                # Map futures to rows
                future_to_row = {executor.submit(fetch_minio_urls, r): r for r in raw_data}
                
                for future in future_to_row:
                    row_dict = future_to_row[future]
                    uniq_id = row_dict.get('Uniq_Id')
                    
                    # Grouping Logic
                    if uniq_id not in grouped_data:
                        grouped_data[uniq_id] = {
                            "Uniq_Id": uniq_id,
                            "Org_ID": row_dict.get("Org_ID"),
                            "Org_Name": row_dict.get("Org_Name"),
                            "Warhouse_Name": row_dict.get("Warhouse_Name"),
                            "WHR_Superuser_No": row_dict.get("WHR_Superuser_No"),
                            "WHR_SuperUser_Name": row_dict.get("WHR_SuperUser_Name"),
                            "Created_date": row_dict.get("Created_date"),
                            "Created_By": row_dict.get("Created_By"),
                            "item_count": 0,
                            "item_details": []
                        }

                    # Get result from future
                    image_urls = future.result()

                    # Create Item Details (WITHOUT Flags/Attributes)
                    item_detail = {
                        "Id": row_dict.get("id"),
                        "Item_Code": row_dict.get("Item_Code"),
                        "Item_Ddescription": row_dict.get("Item_Ddescription"),
                        "Product_Code": row_dict.get("Product_Code"),
                        "Serail_No": row_dict.get("Serail_No"),
                        "Franchse": row_dict.get("Franchse"),
                        "Item_Class": row_dict.get("Item_Class"),
                        "Sub_Class": row_dict.get("Sub_Class"),
                        "Prod_Part": row_dict.get("Prod_Part"),
                        "Location_Code": row_dict.get("Location_Code"),
                        
                        "Damage_Status": row_dict.get("Damage_Status"),
                        "Remarks": row_dict.get("Remarks"),
                        "Image_Minio_location": row_dict.get("Image_Minio_location"),
                        "IP": row_dict.get("IP"),
                        "image_download_urls": image_urls
                    }
                    
                    grouped_data[uniq_id]["item_details"].append(item_detail)
                    grouped_data[uniq_id]["item_count"] += 1

        # Calculate Pagination Metadata
        import math
        total_pages = math.ceil(total_records / page_size)
        
        base_url = request.build_absolute_uri(request.path)
        
        if page < total_pages:
            next_page = f"{base_url}?page={page+1}&limit={page_size}"
        else:
            next_page = None
            
        if page > 1:
            previous_page = f"{base_url}?page={page-1}&limit={page_size}"
        else:
            previous_page = None

        final_response = list(grouped_data.values())

        return Response({
            "status": "success",
            "pagination": {
                "page": page,
                "limit": page_size,
                "total_records": total_records,
                "total_pages": total_pages,
                "next": next_page,        # Renamed to standard DRF 'next'
                "previous": previous_page # Renamed to standard DRF 'previous'
            },
            "data": final_response
        }, status=status.HTTP_200_OK)

    except Exception as e:
        import traceback
        return Response(
            {"status": "error", "message": str(e), "trace": traceback.format_exc()},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )
    

#-----------------------------------------------------------------------------------------------------------------
# UPDATE INTERNAL DAMAGE STATUS (Example: Mark as Partially Damaged)
#-----------------------------------------------------------------------------------------------------------------

@csrf_exempt
def update_internal_damage_status(request):
    if request.method != "POST":
        return JsonResponse(
            {"status": "error", "message": "POST method required"},
            status=405
        )

    try:
        body = json.loads(request.body.decode("utf-8"))
        damage_id = body.get("id")
        status = body.get("status")  # Reject or Approved

        if not damage_id or status not in ["Reject", "Approved"]:
            return JsonResponse(
                {"status": "error", "message": "Invalid id or status"},
                status=400
            )

        with connections["default"].cursor() as cursor:

            # --------------------------------------------------
            # 1. Fetch Item_Code, Location_Code, Damage_Status
            # --------------------------------------------------
            cursor.execute("""
                SELECT Item_Code, Location_Code, Damage_Status
                FROM LOC_Internal_Damage_Details_tbl
                WHERE id = %s
            """, [damage_id])

            row = cursor.fetchone()
            if not row:
                return JsonResponse(
                    {"status": "error", "message": "Invalid damage ID"},
                    status=404
                )

            item_code, location_code, existing_status = row

            # --------------------------------------------------
            # 2. STOP if already Approved or Reject
            # --------------------------------------------------
            if existing_status in ["Approved", "Reject"]:
                return JsonResponse({
                    "status": "info",
                    "message": "This ID is already updated"
                })

            # --------------------------------------------------
            # 3. Update Internal Damage Status
            # --------------------------------------------------
            cursor.execute("""
                UPDATE LOC_Internal_Damage_Details_tbl
                SET Damage_Status = %s
                WHERE id = %s
            """, [status, damage_id])

            # --------------------------------------------------
            # 4. Stop if Location_Code is NULL or UNKNOWN
            # --------------------------------------------------
            if location_code is None or str(location_code).strip().upper() == "UNKNOWN":
                return JsonResponse({
                    "status": "success",
                    "message": "Damage status updated. Stock update skipped due to invalid location."
                })

            # --------------------------------------------------
            # 5. Get Stock ID (Internal Damage Partically)
            # --------------------------------------------------
            cursor.execute("""
                SELECT TOP 1 id
                FROM WHR_Stock_Details_tbl
                WHERE Item_Code = %s
                  AND Location_Code = %s
                  AND Stock_status = 'Internal Damage Partically'
                ORDER BY id DESC
            """, [item_code, location_code])

            stock_row = cursor.fetchone()
            if not stock_row:
                return JsonResponse({
                    "status": "success",
                    "message": "Damage updated. No matching stock row found."
                })

            stock_id = stock_row[0]

            # --------------------------------------------------
            # 6. Update Stock Status Based on Approval
            # --------------------------------------------------
            if status == "Reject":
                new_stock_status = "Available"
            else:  # Approved
                new_stock_status = "Internal Damage Confirmed"

            cursor.execute("""
                UPDATE WHR_Stock_Details_tbl
                SET Stock_status = %s
                WHERE id = %s
            """, [new_stock_status, stock_id])

        return JsonResponse({
            "status": "success",
            "message": f"Damage {status} and stock updated successfully"
        })

    except Exception as e:
        return JsonResponse(
            {"status": "error", "message": str(e)},
            status=500
        )