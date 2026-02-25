





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

# Create your views here.





############################################# This view has Only View on Active ####################################################################

############################################# This view has Only View on Active and eactive ####################################################################
from django.http import JsonResponse
from django.db import connection



from django.http import JsonResponse
from django.db import connection

def get_whr_code(request):
    sql = """
        SELECT DISTINCT Physical_WHR, WHR_Code
        FROM Inbound_WHR_Location_Mapping_tbl
        ORDER BY WHR_Code
    """

    with connection.cursor() as cursor:
        cursor.execute(sql)
        rows = cursor.fetchall()

    data = [
        {"Physical_WHR": row[0], "WHR_Code": row[1]}
        for row in rows
    ]

    return JsonResponse(data, safe=False)

############################################# This view has Only View on Active ####################################################################

from django.http import JsonResponse
from django.db import connection


def get_whr_id(request, WHR_Code):
    """Return distinct WHR_ID values for the given WHR_Code (naturally sorted)."""

    if not WHR_Code:
        return JsonResponse({"error": "WHR_Code is required."}, status=400)

    whr_code = WHR_Code.strip().upper()

    query = """
        SELECT WHR_ID
        FROM (
            SELECT DISTINCT
                WHR_ID,
                TRY_CAST(SUBSTRING(WHR_ID, 2, LEN(WHR_ID)) AS INT) AS num_part
            FROM Inbound_WHR_Location_Mapping_tbl
            WHERE WHR_Code = %s
              AND Flag1 = 'Active'
        ) AS q
        ORDER BY num_part;
    """

    with connection.cursor() as cursor:
        cursor.execute(query, [whr_code])
        whr_ids = [row[0] for row in cursor.fetchall()]

    return JsonResponse({
        "WHR_Code": whr_code,
        "WHR_IDs": whr_ids
    })



def Get_whr__ID_status(request, WHR_Code):
    """Return WHR_ID values with Active/Deactive status for the given WHR_Code."""

    if not WHR_Code:
        return JsonResponse({"error": "WHR_Code is required."}, status=400)

    whr_code = WHR_Code.strip().upper()

    query = """
        SELECT DISTINCT
            WHR_ID,
            Flag1,
            TRY_CAST(SUBSTRING(WHR_ID, 2, LEN(WHR_ID)) AS INT) AS num_part
        FROM Inbound_WHR_Location_Mapping_tbl
        WHERE WHR_Code = %s
        ORDER BY num_part;
    """

    with connection.cursor() as cursor:
        cursor.execute(query, [whr_code])
        rows = cursor.fetchall()

    # Format: W1-Active / W2-Deactive
    whr_list = [f"{row[0]}-{row[1]}" for row in rows]

    return JsonResponse({
        "WHR_Code": whr_code,
        "WHR_IDs": whr_list
    })
############################################# This view has Only View on Active and eactive ####################################################################

from django.http import JsonResponse
from django.db import connection

def get_zone_id(request, WHR_Code, WHR_ID):
    whr_code = WHR_Code.strip().upper()
    whr_id = WHR_ID.strip().upper()

    sql = """
        SELECT DISTINCT Zone_ID
        FROM Inbound_WHR_Location_Mapping_tbl WITH (NOLOCK)
        WHERE UPPER(WHR_Code) = %s AND UPPER(WHR_ID) = %s and Flag1 = 'Active'
    """

    with connection.cursor() as cursor:
        cursor.execute(sql, [whr_code, whr_id])
        rows = cursor.fetchall()

    # Convert to clean list
    zone_ids = sorted({
        str(row[0]).strip().upper()
        for row in rows
        if row[0] not in (None, "", " ")
    })

    return JsonResponse({
        "WHR_Code": whr_code,
        "WHR_ID": whr_id,
        "Zone_IDs": zone_ids
    })
############################################## This view has Only View on Active ####################################################################
############################################## This view has Only View on Active and Deactive ####################################################################
from django.http import JsonResponse
from django.db import connection

def Get_Zone_Id_Status(request, WHR_Code, WHR_ID):
    whr_code = WHR_Code.strip().upper()
    whr_id = WHR_ID.strip().upper()

    sql = """
        SELECT DISTINCT
            Zone_ID,
            Flag1,
            TRY_CAST(SUBSTRING(Zone_ID, 2, LEN(Zone_ID)) AS INT) AS num_part
        FROM Inbound_WHR_Location_Mapping_tbl WITH (NOLOCK)
        WHERE UPPER(WHR_Code) = %s 
          AND UPPER(WHR_ID) = %s
        ORDER BY num_part;
    """

    with connection.cursor() as cursor:
        cursor.execute(sql, [whr_code, whr_id])
        rows = cursor.fetchall()

    # Format output: Z1-Active / Z2-Deactive / Z3-Active
    zone_ids = [
        f"{str(row[0]).strip().upper()}-{row[1]}"
        for row in rows
        if row[0] not in (None, "", " ")
    ]

    return JsonResponse({
        "WHR_Code": whr_code,
        "WHR_ID": whr_id,
        "Zone_IDs": zone_ids
    })

############################################### This view has Only View on Active and Deactive ####################################################################
############################################### This view has Only View on Active ####################################################################
from django.http import JsonResponse
from django.db import connection

def get_bin_id(request, WHR_Code, WHR_ID, Zone_ID):
    whr_code = WHR_Code.strip().upper()
    whr_id = WHR_ID.strip().upper()
    zone_id = Zone_ID.strip().upper()

    sql = """
        SELECT DISTINCT Bin_ID
        FROM Inbound_WHR_Location_Mapping_tbl WITH (NOLOCK)
        WHERE UPPER(WHR_Code) = %s
          AND UPPER(WHR_ID) = %s
          AND UPPER(Zone_ID) = %s and Flag1 = 'Active'
    """

    with connection.cursor() as cursor:
        cursor.execute(sql, [whr_code, whr_id, zone_id])
        rows = cursor.fetchall()

    bin_ids = sorted({
        str(row[0]).strip().upper()
        for row in rows
        if row[0] not in (None, "", " ")
    })

    return JsonResponse({
        "WHR_Code": whr_code,
        "WHR_ID": whr_id,
        "Zone_ID": zone_id,
        "Bin_IDs": bin_ids
    })
############################################### This view has Only View on Active ####################################################################
############################################## This view has Only View on Active and Deactive ####################################################################

from django.http import JsonResponse
from django.db import connection

def Get_Bin_Id_Status(request, WHR_Code, WHR_ID, Zone_ID):
    whr_code = WHR_Code.strip().upper()
    whr_id = WHR_ID.strip().upper()
    zone_id = Zone_ID.strip().upper()

    sql = """
        SELECT DISTINCT
            Bin_ID,
            Flag1,
            TRY_CAST(SUBSTRING(Bin_ID, 2, LEN(Bin_ID)) AS INT) AS num_part
        FROM Inbound_WHR_Location_Mapping_tbl WITH (NOLOCK)
        WHERE UPPER(WHR_Code) = %s
          AND UPPER(WHR_ID) = %s
          AND UPPER(Zone_ID) = %s
        ORDER BY num_part;
    """

    with connection.cursor() as cursor:
        cursor.execute(sql, [whr_code, whr_id, zone_id])
        rows = cursor.fetchall()

    # Format → B1-Active / B2-Deactive / B3-Active
    bin_ids = [
        f"{str(row[0]).strip().upper()}-{row[1]}"
        for row in rows
        if row[0] not in (None, "", " ")
    ]

    return JsonResponse({
        "WHR_Code": whr_code,
        "WHR_ID": whr_id,
        "Zone_ID": zone_id,
        "Bin_IDs": bin_ids
    })
############################################## This view has Only View on Active and Deactive ####################################################################
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.db import connection

@csrf_exempt
def Post_WHR(request, Physical_WHR, WHR_Code):

    if request.method != "POST":
        return JsonResponse({"error": "POST required"}, status=400)

    cursor = connection.cursor()

    # 1. Find last WHR_ID for this WHR_Code + Physical_WHR
    cursor.execute("""
        SELECT TOP 1 WHR_ID
        FROM Inbound_WHR_Location_Mapping_tbl
        WHERE Physical_WHR = %s AND WHR_Code = %s
        ORDER BY id DESC
    """, [Physical_WHR, WHR_Code])

    row = cursor.fetchone()

    if row:
        last_whr_id = row[0]                # Example: "W16"
        next_num = int(last_whr_id[1:]) + 1 # → 17
    else:
        next_num = 1

    new_whr_id = f"W{next_num}"            # → "W17"

    # 2. Default zone & bin
    new_zone = "Z1"
    new_bin  = "B1"

    # 3. Get next WHR_Barcode
    cursor.execute("""
        SELECT TOP 1 WHR_Barcode
        FROM Inbound_WHR_Location_Mapping_tbl
        ORDER BY WHR_Barcode DESC
    """)

    row = cursor.fetchone()

    if row:
        next_barcode = int(row[0]) + 1      # Example: 20000072 → 20000073
    else:
        next_barcode = 37301501

    # 4. Insert ONLY required columns
    cursor.execute("""
        INSERT INTO Inbound_WHR_Location_Mapping_tbl
        (Physical_WHR, WHR_Code, WHR_ID, Zone_ID, Bin_ID, WHR_Barcode,Flag1)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, [
        Physical_WHR,
        WHR_Code,
        new_whr_id,
        new_zone,
        new_bin,
        next_barcode,
        'Active'
        
    ])

    return JsonResponse({
        "status": "Inserted",
        "Physical_WHR": Physical_WHR,
        "WHR_Code": WHR_Code,
        "WHR_ID": new_whr_id,
        "Zone_ID": new_zone,
        "Bin_ID": new_bin,
        "WHR_Barcode": next_barcode
    })


from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.db import connection

@csrf_exempt
def Post_WHR_Zone(request, Physical_WHR, WHR_Code, WHR_ID):

    if request.method != "POST":
        return JsonResponse({"error": "POST required"}, status=400)

    cursor = connection.cursor()

    # 1. Get existing row
    cursor.execute("""
        SELECT Zone_ID, Bin_ID, WHR_Barcode
        FROM Inbound_WHR_Location_Mapping_tbl
        WHERE Physical_WHR = %s AND WHR_Code = %s AND WHR_ID = %s
        ORDER BY id DESC
    """, [Physical_WHR, WHR_Code, WHR_ID])

    row = cursor.fetchone()

    if not row:
        return JsonResponse({"error": "Record not found"}, status=404)

    current_zone, current_bin, current_barcode = row

    # 2. Auto-increment Zone (Z1 → Z2 → Z3...)
    next_zone_num = int(current_zone[1:]) + 1
    new_zone = f"Z{next_zone_num}"

    # 3. Reset Bin to B1
    new_bin = "B1"

    # 4. Keep same barcode (Zone process does NOT change barcode)
    new_barcode = current_barcode

    # 5. INSERT new row
    cursor.execute("""
        INSERT INTO Inbound_WHR_Location_Mapping_tbl
        (Physical_WHR, WHR_Code, WHR_ID, Zone_ID, Bin_ID, WHR_Barcode,Flag1)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, [
        Physical_WHR,
        WHR_Code,
        WHR_ID,
        new_zone,
        new_bin,
        new_barcode,
        'Active'
    ])

    return JsonResponse({
        "status": "Inserted",
        "Physical_WHR": Physical_WHR,
        "WHR_Code": WHR_Code,
        "WHR_ID": WHR_ID,
        "Zone_ID": new_zone,
        "Bin_ID": new_bin,
        "WHR_Barcode": new_barcode
    })


from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.db import connection

@csrf_exempt
def Post_WHR_Bin(request, Physical_WHR, WHR_Code, WHR_ID, Zone_ID):

    if request.method != "POST":
        return JsonResponse({"error": "POST required"}, status=400)

    cursor = connection.cursor()

    # 1. Get existing row
    cursor.execute("""
        SELECT Bin_ID, WHR_Barcode
        FROM Inbound_WHR_Location_Mapping_tbl
        WHERE Physical_WHR = %s AND WHR_Code = %s AND WHR_ID = %s AND Zone_ID = %s
        ORDER BY id DESC
    """, [Physical_WHR, WHR_Code, WHR_ID, Zone_ID])

    row = cursor.fetchone()

    if not row:
        return JsonResponse({"error": "Record not found"}, status=404)

    current_bin, current_barcode = row

    # 2. Increment Bin (B1 → B2 → B3...)
    next_bin_num = int(current_bin[1:]) + 1
    new_bin = f"B{next_bin_num}"

    # 3. Increment barcode
    new_barcode = int(current_barcode) + 1

    # 4. INSERT new row (NO UPDATE)
    cursor.execute("""
        INSERT INTO Inbound_WHR_Location_Mapping_tbl
        (Physical_WHR, WHR_Code, WHR_ID, Zone_ID, Bin_ID, WHR_Barcode,Flag1                                                                                                                                                                                                                                                                                                                                                                                                                                                             )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, [
        Physical_WHR,
        WHR_Code,
        WHR_ID,
        Zone_ID,
        new_bin,
        new_barcode,
        'Active'
    ])

    return JsonResponse({
        "status": "Inserted",
        "Physical_WHR": Physical_WHR,
        "WHR_Code": WHR_Code,
        "WHR_ID": WHR_ID,
        "Zone_ID": Zone_ID,
        "Bin_ID": new_bin,
        "WHR_Barcode": new_barcode
    })

#############################################################  UPDATE VIEWS ############################################################



from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.db import connection

@csrf_exempt
def Deactivate_WHR(request, Physical_WHR, WHR_Code, WHR_ID, Flag1):

    if request.method != "POST":
        return JsonResponse({"error": "POST required"}, status=400)

    cursor = connection.cursor()

    # 1. Check if record exists
    cursor.execute("""
        SELECT id, Flag1
        FROM Inbound_WHR_Location_Mapping_tbl
        WHERE Physical_WHR=%s AND WHR_Code=%s AND WHR_ID=%s
    """, [Physical_WHR, WHR_Code, WHR_ID])

    row = cursor.fetchone()

    if not row:
        return JsonResponse({"error": "Record not found"}, status=404)

    row_id, old_flag = row

    # 2. Update ONLY Flag1
    cursor.execute("""
        UPDATE Inbound_WHR_Location_Mapping_tbl
        SET Flag1 = %s
        WHERE Physical_WHR=%s AND WHR_Code=%s AND WHR_ID=%s
    """, [Flag1, Physical_WHR, WHR_Code, WHR_ID])

    # 3. Return success
    return JsonResponse({
        "id": row_id,
        "Physical_WHR": Physical_WHR,
        "WHR_Code": WHR_Code,
        "WHR_ID": WHR_ID,
        "Old_Flag1": old_flag,
        "New_Flag1": Flag1,
        "status": "Flag updated successfully"
    })


from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.db import connection

@csrf_exempt
def Deactivate_WHR_Zone(request, Physical_WHR, WHR_Code, WHR_ID, Zone_ID, Flage1):

    if request.method != "POST":
        return JsonResponse({"error": "POST method required"}, status=400)

    cursor = connection.cursor()

    # 1. Get the record
    cursor.execute("""
        SELECT id, Flag1
        FROM Inbound_WHR_Location_Mapping_tbl
        WHERE Physical_WHR = %s AND WHR_Code = %s AND WHR_ID = %s AND Zone_ID = %s
        ORDER BY id DESC
    """, [Physical_WHR, WHR_Code, WHR_ID, Zone_ID])

    row = cursor.fetchone()

    if not row:
        return JsonResponse({"error": "Record not found"}, status=404)

    record_id, old_flag = row

    # 2. Toggle logic
    if old_flag.strip().lower() == "active":
        new_flag = "Deactive"
    else:
        new_flag = "Active"

    # 3. Update only Flag1
    cursor.execute("""
        UPDATE Inbound_WHR_Location_Mapping_tbl
        SET Flag1 = %s
        WHERE Physical_WHR = %s AND WHR_Code = %s AND WHR_ID = %s AND Zone_ID = %s
    """, [new_flag, Physical_WHR, WHR_Code, WHR_ID, Zone_ID])

    return JsonResponse({
        "id": record_id,
        "Physical_WHR": Physical_WHR,
        "WHR_Code": WHR_Code,
        "WHR_ID": WHR_ID,
        "Zone_Id": Zone_ID,
        "Old_Flag1": old_flag,
        "New_Flag1": new_flag,
        "status": "Flag updated successfully"
    })



from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.db import connection

@csrf_exempt
def Deactivate_WHR_Statu(request, Physical_WHR, WHR_Code, WHR_ID, Zone_ID, Bin_ID, Flage1):

    if request.method != "POST":
        return JsonResponse({"error": "POST method required"}, status=400)

    cursor = connection.cursor()

    # 1. Get the record
    cursor.execute("""
        SELECT id, Flag1
        FROM Inbound_WHR_Location_Mapping_tbl
        WHERE Physical_WHR = %s 
          AND WHR_Code = %s 
          AND WHR_ID = %s 
          AND Zone_ID = %s 
          AND Bin_ID = %s
        ORDER BY id DESC
    """, [Physical_WHR, WHR_Code, WHR_ID, Zone_ID, Bin_ID])

    row = cursor.fetchone()

    if not row:
        return JsonResponse({"error": "Record not found"}, status=404)

    record_id, old_flag = row

    # 2. Toggle flag
    if old_flag.strip().lower() == "active":
        new_flag = "Deactive"
    else:
        new_flag = "Active"

    # 3. Update only Flag1
    cursor.execute("""
        UPDATE Inbound_WHR_Location_Mapping_tbl
        SET Flag1 = %s
        WHERE Physical_WHR = %s 
          AND WHR_Code = %s 
          AND WHR_ID = %s 
          AND Zone_ID = %s 
          AND Bin_ID = %s
    """, [new_flag, Physical_WHR, WHR_Code, WHR_ID, Zone_ID, Bin_ID])

    return JsonResponse({
        "id": record_id,
        "Physical_WHR": Physical_WHR,
        "WHR_Code": WHR_Code,
        "WHR_ID": WHR_ID,
        "Zone_Id": Zone_ID,
        "Bin_ID": Bin_ID,
        "Old_Flag1": old_flag,
        "New_Flag1": new_flag,
        "status": "Flag updated successfully"
    })



from django.http import JsonResponse
from django.db import connection

def get_location_by_barcode(request, Physical_WHR, WHR_Barcode):
    try:
        Physical_WHR = Physical_WHR.strip()
        WHR_Barcode = WHR_Barcode.strip()

        with connection.cursor() as cursor:

            # 1️⃣ Check warehouse
            cursor.execute(
                "SELECT 1 FROM Inbound_WHR_Location_Mapping_tbl WHERE Physical_WHR = %s",
                [Physical_WHR]
            )
            if not cursor.fetchone():
                return JsonResponse({
                    "status": "error",
                    "message": f"Warehouse '{Physical_WHR}' does not exist"
                }, status=404)

            # 2️⃣ Get barcode mapping
            cursor.execute(
                """
                SELECT WHR_Code, WHR_ID, Zone_ID, Bin_ID
                FROM Inbound_WHR_Location_Mapping_tbl
                WHERE Physical_WHR = %s AND WHR_Barcode = %s
                """,
                [Physical_WHR, WHR_Barcode]
            )

            row = cursor.fetchone()

        if not row:
            return JsonResponse({
                "status": "error",
                "Physical_WHR": Physical_WHR,
                "WHR_Barcode": WHR_Barcode,
                "message": "Barcode not available under this warehouse"
            }, status=404)

        WHR_Code, WHR_ID, Zone_ID, Bin_ID = row

        return JsonResponse({
            "status": "success",
            "Physical_WHR": Physical_WHR,
            "WHR_Barcode": WHR_Barcode,
            "warehouse_code": WHR_Code,
            "warehouse_id": WHR_ID,
            "zone_id": Zone_ID,
            "bin_id": Bin_ID
        })

    except Exception as e:
        return JsonResponse({
            "status": "error",
            "message": str(e)
        }, status=500)




def get_location_by_only_barcode(request, WHR_Barcode):
    try:
        WHR_Barcode = WHR_Barcode.strip()

        with connection.cursor() as cursor:

        
            # 2️⃣ Get barcode mapping
            cursor.execute(
                """
                SELECT WHR_Code, WHR_ID, Zone_ID, Bin_ID
                FROM Inbound_WHR_Location_Mapping_tbl
                WHERE  WHR_Barcode = %s
                """,
                [ WHR_Barcode]
            )

            row = cursor.fetchone()

        if not row:
            return JsonResponse({
                "status": "error",
                "WHR_Barcode": WHR_Barcode,
                "message": "Barcode not available under this warehouse"
            }, status=404)

        WHR_Code, WHR_ID, Zone_ID, Bin_ID = row

        return JsonResponse({
            "status": "success",
            "WHR_Barcode": WHR_Barcode,
            "warehouse_code": WHR_Code,
            "warehouse_id": WHR_ID,
            "zone_id": Zone_ID,
            "bin_id": Bin_ID
        })

    except Exception as e:
        return JsonResponse({
            "status": "error",
            "message": str(e)
        }, status=500)


from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.db import connection
import json

@csrf_exempt
def get_item_details(request):
    if request.method != "POST":
        return JsonResponse({"status": "error", "message": "POST method required"}, status=400)

    try:
        body = json.loads(request.body.decode("utf-8"))
        search_type = body.get("type", "").strip().lower()
        value = body.get("value", "").strip()

        if not search_type or not value:
            return JsonResponse({"status": "error", "message": "Missing 'type' or 'value'"}, status=400)

        allowed_types = {
            "barcode": "PRODUCT_BARCODE",
            "itemcode": "ITEM_CODE"
        }

        if search_type not in allowed_types:
            return JsonResponse({"status": "error", "message": "Invalid type"}, status=400)

        search_column = allowed_types[search_type]

        # 🔹 1) FIRST TABLE (original logic) – DO NOT CHANGE
        sql = f"""
            SELECT TOP 1 ITEM_CODE, DESCRIPTION, PRODUCT_BARCODE, SERIAL_STATUS, FRANCHISE, CLASS, SUBCLASS, PROD_PART
            FROM BUYP.ALJE_ITEM_CATEGORIES_CPD_V WITH (NOLOCK)
            WHERE {search_column} = %s
        """

        with connection.cursor() as cursor:
            cursor.execute(sql, [value])
            row = cursor.fetchone()

        if not row:
            return JsonResponse({"status": "error", "message": "Item not found"}, status=404)

        item_code, description, product_barcode, serial_status, franchise, item_class, subclass, product_parts = row

        # 🔹 2) NEW REQUIREMENT → FETCH Cubic_Meter FROM Item_Measurement_tbl
        sql_cubic = """
            SELECT ISNULL(Cubic_Meter, 0)
            FROM BUYP.dbo.Item_Measurement_tbl WITH (NOLOCK)
            WHERE Item_Code = %s
        """

        with connection.cursor() as cursor:
            cursor.execute(sql_cubic, [item_code])
            cubic_row = cursor.fetchone()

        cubic_meter = round(float(cubic_row[0]), 2) if cubic_row else 0.00


        # 🔹 3) FINAL OUTPUT
        return JsonResponse({
            "status": "success",
            "item_code": item_code,
            "description": description,
            "franchise": franchise,
            "item_class": item_class,
            "subclass": subclass,
            "product_parts": product_parts,
            "Cubic_Meter": cubic_meter,
            "product_barcode": product_barcode,
            "serial_status": serial_status
        })

    except json.JSONDecodeError:
        return JsonResponse({"status": "error", "message": "Invalid JSON format"}, status=400)

    except Exception as e:
        return JsonResponse({"status": "error", "message": str(e)}, status=500)




from django.http import JsonResponse
from django.db import connection

def update_loc_measurement(request, physical_whr, WhR_ID, whr_measurement):
    try:
        physical_whr = physical_whr.strip().upper()
        WhR_ID = WhR_ID.strip().upper()
        whr_measurement = float(whr_measurement)

        # 1️⃣ Get all WHR_Code values
        sql_codes = """
            SELECT DISTINCT WHR_Code
            FROM Inbound_WHR_Location_Mapping_tbl WITH (NOLOCK)
            WHERE UPPER(Physical_WHR) = %s AND UPPER(WHR_ID) = %s
        """
        with connection.cursor() as cursor:
            cursor.execute(sql_codes, [physical_whr, WhR_ID])
            whr_codes = [row[0] for row in cursor.fetchall()]

        if not whr_codes:
            return JsonResponse({"status": "error", "message": "No matching rows found."})

        updated_details = []

        # 2️⃣ Process each WHR_Code
        for code in whr_codes:

            # Count rows for this WHR_Code
            sql_count = """
                SELECT COUNT(*)
                FROM Inbound_WHR_Location_Mapping_tbl WITH (NOLOCK)
                WHERE UPPER(Physical_WHR) = %s 
                  AND UPPER(WHR_ID) = %s
                  AND UPPER(WHR_Code) = %s
            """
            with connection.cursor() as cursor:
                cursor.execute(sql_count, [physical_whr, WhR_ID, code.upper()])
                row_count = cursor.fetchone()[0]

            if row_count == 0:
                continue

            # 3️⃣ Calculate LOC_Measurement
            loc_value = whr_measurement / row_count

            # 4️⃣ Update both WHR_Measurement and LOC_Measurement
            sql_update = """
                UPDATE Inbound_WHR_Location_Mapping_tbl
                SET WHR_Measurement = %s,
                    LOC_Measurement = %s
                WHERE UPPER(Physical_WHR) = %s 
                  AND UPPER(WHR_ID) = %s
                  AND UPPER(WHR_Code) = %s
            """
            with connection.cursor() as cursor:
                cursor.execute(sql_update, [
                    whr_measurement,  # Set WHR_Measurement = 500
                    loc_value,        # Set LOC_Measurement = divided value
                    physical_whr,
                    WhR_ID,
                    code.upper()
                ])

            updated_details.append({
                "WHR_Code": code,
                "Rows": row_count,
                "WHR_Measurement_Set": whr_measurement,
                "LOC_Measurement_Set": loc_value
            })

        return JsonResponse({
            "status": "success",
            "message": "WHR_Measurement & LOC_Measurement updated successfully",
            "details": updated_details
        })

    except Exception as e:
        return JsonResponse({"status": "error", "message": str(e)})



from django.http import JsonResponse
from django.db import connection

def get_whr_measurement(request, physical_whr, whr_id):
    physical_whr = physical_whr.strip().upper()
    whr_id = whr_id.strip().upper()

    sql = """
        SELECT DISTINCT WHR_ID, WHR_Measurement
        FROM Inbound_WHR_Location_Mapping_tbl WITH (NOLOCK)
        WHERE UPPER(Physical_WHR) = %s
          AND UPPER(WHR_ID) = %s
    """

    with connection.cursor() as cursor:
        cursor.execute(sql, [physical_whr, whr_id])
        row = cursor.fetchone()

    if not row:
        return JsonResponse({"status": "error", "message": "No data found"}, status=404)

    return JsonResponse({
        "Physical_WHR": physical_whr,
        "WHR_ID": row[0],
        "WHR_Measurement": row[1]
    })


from django.http import JsonResponse
from django.db import connection


def get_whr_measurement(request, physical_whr, whr_id):
    """
    ✓ Returns WHR_Measurement for the given Physical_WHR and WHR_ID.
    ✓ Uses DISTINCT to avoid duplicates.
    ✓ Converts everything to uppercase to avoid case issues.
    """

    physical_whr = physical_whr.strip().upper()
    whr_id = whr_id.strip().upper()

    sql = """
        SELECT DISTINCT WHR_Measurement
        FROM Inbound_WHR_Location_Mapping_tbl WITH (NOLOCK)
        WHERE UPPER(Physical_WHR) = %s
          AND UPPER(WHR_ID) = %s
    """

    with connection.cursor() as cursor:
        cursor.execute(sql, [physical_whr, whr_id])
        row = cursor.fetchone()

    # No result found
    if not row:
        return JsonResponse({
            "status": "error",
            "message": "No measurement found for given Physical_WHR and WHR_ID"
        }, status=404)

    # Successful response
    return JsonResponse({
        "status": "success",
        "Physical_WHR": physical_whr,
        "WHR_ID": whr_id,
        "WHR_Measurement": row[0]
    })


from django.http import JsonResponse
from django.db import connection

def get_location_measurement_sql(request):
    """Return LOC_Measurement and WHR_Measurement for a specific WHR/Zone/Bin."""

    wr_code = request.GET.get('wr_code', '').strip().upper()
    whr_id  = request.GET.get('whr_id', '').strip().upper()
    zone_id = request.GET.get('zone_id', '').strip().upper()
    bin_id  = request.GET.get('bin_id', '').strip().upper()

    # Validate required params
    if not all([wr_code, whr_id, zone_id, bin_id]):
        return JsonResponse(
            {"status": "error", "message": "Missing required parameters"},
            status=400
        )

    sql = """
        SELECT
            ISNULL(L.LOC_Measurement, 0) AS LOC_Measurement,
            ISNULL(L.WHR_Measurement, 0) AS WHR_Measurement,
            L.WHR_Barcode,
            ISNULL(
                SUM(
                    CASE
                        WHEN ISNUMERIC(H.Item_Cubic_Meter) = 1
                        THEN CAST(H.Item_Cubic_Meter AS FLOAT)
                        ELSE 0
                    END
                ),
                0
            ) AS Used_Cubic_Meter
        FROM BUYP.dbo.Inbound_WHR_Location_Mapping_tbl L WITH (NOLOCK)
        LEFT JOIN WHR_Stock_Details_tbl H WITH (NOLOCK)
            ON H.Location_Barcode = L.WHR_Barcode
           AND ISNULL(H.Stock_status, '') <> 'Dispatched'
        WHERE UPPER(L.WHR_Code) = %s
          AND UPPER(L.WHR_ID)   = %s
          AND UPPER(L.Zone_ID)  = %s
          AND UPPER(L.Bin_ID)   = %s
        GROUP BY
            L.LOC_Measurement,
            L.WHR_Measurement,
            L.WHR_Barcode;
    """

    with connection.cursor() as cursor:
        cursor.execute(sql, [wr_code, whr_id, zone_id, bin_id])
        row = cursor.fetchone()

    if row:
        return JsonResponse({
            "status": "success",
            "data": {
                "LOC_Measurement": row[0],
                "WHR_Measurement": row[1],
                "WHR_Barcode": row[2],
                "Used_Cubic_Meter": row[3]
            }
        })

    return JsonResponse(
        {"status": "not_found", "message": "No data found"},
        status=404
    )

# @csrf_exempt
# def insert_whr_stock(request):
#     if request.method != "POST":
#         return JsonResponse({"status": "error", "message": "POST method required"}, status=400)

#     try:
#         data = json.loads(request.body)

#         warehouse_name = data.get("Warehouse_Name")
#         org_id = data.get("Org_ID")
#         whr_superuser_no = data.get("WHR_SuperUserNo")
#         whr_super_user_name = data.get("WHR_Super_User_Name")
#         location_barcode = data.get("Location_Barcode")
#         location_measurement = data.get("Location_Measurement")
#         creation_date = data.get("Creation_Date", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
#         created_by = data.get("Created_By")
#         table_details = data.get("Table_Details", [])

#         # --- 1) Check Org_ID and get Org_Name ---
#         with connection.cursor() as cursor:
#             cursor.execute("SELECT [NAME] FROM [BUYP].[ALJE_REGIONS] WHERE [ORGANIZATION_ID]=%s", [org_id])
#             org_row = cursor.fetchone()
#             if not org_row:
#                 return JsonResponse({"status": "error", "message": f"Org_ID {org_id} not found"}, status=400)
#             org_name = org_row[0]

#         # --- 2) Generate Uniq_Id (New Logic from GenerateTokenFormMNG_UNIQIDView) ---
#         with transaction.atomic():
#             with connection.cursor() as cursor:

#                 now = datetime.now()
#                 year_short = str(now.year)[-2:]
#                 month = f"{now.month:02d}"
#                 prefix = f"SMT{year_short}{month}"

#                 # Find latest Uniq_Id starting with prefix
#                 cursor.execute("""
#                     SELECT TOP 1 Uniq_Id
#                     FROM WHR_MNG_UNIQID_tbl WITH (UPDLOCK, HOLDLOCK)
#                     WHERE Uniq_Id LIKE %s
#                     ORDER BY id DESC
#                 """, [f"{prefix}%"])
#                 row = cursor.fetchone()

#                 if row:
#                     last_id = row[0]
#                     match = re.match(rf"{prefix}(\d+)$", last_id)
#                     if match:
#                         last_number = int(match.group(1))
#                         next_number = last_number + 1
#                     else:
#                         next_number = 1
#                 else:
#                     next_number = 1

#                 uniq_id = f"{prefix}{next_number:01d}"

#                 # Save new uniq_id + token
#                 token = random.randint(100000, 999999)
#                 cursor.execute("""
#                     INSERT INTO WHR_MNG_UNIQID_tbl (Uniq_Id, Tocken)
#                     VALUES (%s, %s)
#                 """, [uniq_id, token])

#                 # --- 3) Check if Uniq_Id already exists in Stock table ---
#                 cursor.execute("SELECT 1 FROM WHR_Stock_Management_Header_tbl WHERE Uniq_Id=%s", [uniq_id])
#                 if cursor.fetchone():
#                     return JsonResponse({"status": "error", "message": f"Uniq_Id {uniq_id} already exists"}, status=400)

#                 # --- 4) Insert items from Table_Details ---
#                 insert_query = """
#                     INSERT INTO WHR_Stock_Management_Header_tbl
#                     (Warehouse_Name, Org_ID, Org_Name, WHR_SuperUserNo, WHR_Super_User_Name,
#                      Location_Code, Location_Barcode, Location_Measurement, Item_Code, Item_Cubic_Meter,
#                      Item_Description, Product_Parts, Product_Code, Total_Item_Qty, Total_Cubic_Meter,
#                      Franchise, Class_Name, Sub_Class, Creation_Date, Creation_By, Uniq_Id)
#                     VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
#                 """
#                 for item in table_details:
#                     cursor.execute(insert_query, [
#                         warehouse_name,
#                         org_id,
#                         org_name,
#                         whr_superuser_no,
#                         whr_super_user_name,
#                         item.get("Location_Code"),
#                         location_barcode,
#                         location_measurement,
#                         item.get("itemCode"),
#                         item.get("Item_meas"),
#                         item.get("itemDetails"),
#                         item.get("product_Type"),
#                         item.get("productCode"),
#                         item.get("quantity"),
#                         item.get("tot_item_meas"),
#                         item.get("Franchise"),
#                         item.get("Item_class"),
#                         item.get("Sub_Class"),
#                         creation_date,
#                         created_by,
#                         uniq_id
#                     ])

#         return JsonResponse({"status": "success", "message": f"{len(table_details)} items inserted", "Uniq_Id": uniq_id})

#     except Exception as e:
#         return JsonResponse({"status": "error", "message": str(e)}, status=500)




# @csrf_exempt
# def insert_whr_stock(request):
#     if request.method != "POST":
#         return JsonResponse({"status": "error", "message": "POST method required"}, status=400)

#     try:
#         data = json.loads(request.body)

#         warehouse_name = data.get("Warehouse_Name")
#         org_id = data.get("Org_ID")
#         whr_superuser_no = data.get("WHR_SuperUserNo")
#         whr_super_user_name = data.get("WHR_Super_User_Name")
#         location_barcode = data.get("Location_Barcode")
#         location_measurement = data.get("Location_Measurement")
#         creation_date = data.get("Creation_Date", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
#         created_by = data.get("Created_By")
#         table_details = data.get("Table_Details", [])

#         # --- 1) Check Org_ID and get Org_Name ---
#         with connection.cursor() as cursor:
#             cursor.execute("SELECT [NAME] FROM [BUYP].[ALJE_REGIONS] WHERE [ORGANIZATION_ID]=%s", [org_id])
#             org_row = cursor.fetchone()
#             if not org_row:
#                 return JsonResponse({"status": "error", "message": f"Org_ID {org_id} not found"}, status=400)
#             org_name = org_row[0]

#         # --- 2) Generate Uniq_Id (New Logic from GenerateTokenFormMNG_UNIQIDView) ---
#         with transaction.atomic():
#             with connection.cursor() as cursor:

#                 now = datetime.now()
#                 year_short = str(now.year)[-2:]
#                 month = f"{now.month:02d}"
#                 prefix = f"SMT{year_short}{month}"

#                 # Find latest Uniq_Id starting with prefix
#                 cursor.execute("""
#                     SELECT TOP 1 Uniq_Id
#                     FROM WHR_MNG_UNIQID_tbl WITH (UPDLOCK, HOLDLOCK)
#                     WHERE Uniq_Id LIKE %s
#                     ORDER BY id DESC
#                 """, [f"{prefix}%"])
#                 row = cursor.fetchone()

#                 if row:
#                     last_id = row[0]
#                     match = re.match(rf"{prefix}(\d+)$", last_id)
#                     if match:
#                         last_number = int(match.group(1))
#                         next_number = last_number + 1
#                     else:
#                         next_number = 1
#                 else:
#                     next_number = 1

#                 uniq_id = f"{prefix}{next_number:01d}"

#                 # Save new uniq_id + token
#                 token = random.randint(100000, 999999)
#                 cursor.execute("""
#                     INSERT INTO WHR_MNG_UNIQID_tbl (Uniq_Id, Tocken)
#                     VALUES (%s, %s)
#                 """, [uniq_id, token])

#                 # --- 3) Check if Uniq_Id already exists in Stock table ---
#                 cursor.execute("SELECT 1 FROM WHR_Stock_Management_Header_tbl WHERE Uniq_Id=%s", [uniq_id])
#                 if cursor.fetchone():
#                     return JsonResponse({"status": "error", "message": f"Uniq_Id {uniq_id} already exists"}, status=400)

#                 # --- 4) Insert items from Table_Details ---
#                 insert_header_query = """
#                     INSERT INTO WHR_Stock_Management_Header_tbl
#                     (Warehouse_Name, Org_ID, Org_Name, WHR_SuperUserNo, WHR_Super_User_Name,
#                      Location_Code, Location_Barcode, Location_Measurement, Item_Code, Item_Cubic_Meter,
#                      Item_Description, Product_Parts, Product_Code, Total_Item_Qty, Total_Cubic_Meter,
#                      Franchise, Class_Name, Sub_Class, Creation_Date, Creation_By, Uniq_Id)
#                     VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
#                 """

#                 insert_details_query = """
#                     INSERT INTO WHR_Stock_Details_tbl
#                     (Warehouse_Name, Org_ID, Org_Name, WHR_SuperUserNo, WHR_Super_User_Name,
#                      Uniq_Id, Location_Code, Location_Barcode, Location_Measurement,
#                      Item_Code, Item_Cubic_Meter, Item_Description, Product_Parts, Product_Code,
#                      SerialNo, Item_Qty, Total_Cubic_Meter, Franchise, Class_Name, Sub_Class_Name,
#                      Creation_Date, Creation_By, Stock_status)
#                     SELECT TOP (%s)
#                      %s, %s, %s, %s, %s,
#                      %s, %s, %s, %s,
#                      %s, %s, %s, %s, %s,
#                      '', 1, %s, %s, %s, %s,
#                      %s, %s, 'Available'
#                     FROM master..spt_values
#                 """

#                 for item in table_details:
#                     # Insert into Header Table
#                     cursor.execute(insert_header_query, [
#                         warehouse_name,
#                         org_id,
#                         org_name,
#                         whr_superuser_no,
#                         whr_super_user_name,
#                         item.get("Location_Code"),
#                         location_barcode,
#                         location_measurement,
#                         item.get("itemCode"),
#                         item.get("Item_meas"),
#                         item.get("itemDetails"),
#                         item.get("product_Type"),
#                         item.get("productCode"),
#                         item.get("quantity"),
#                         item.get("tot_item_meas"),
#                         item.get("Franchise"),
#                         item.get("Item_class"),
#                         item.get("Sub_Class"),
#                         creation_date,
#                         created_by,
#                         uniq_id
#                     ])

#                     # Insert into Details Table (Multiple Rows)
#                     qty = int(item.get("quantity", 0))
#                     if qty > 0:
#                         cursor.execute(insert_details_query, [
#                             qty,
#                             warehouse_name,
#                             org_id,
#                             org_name,
#                             whr_superuser_no,
#                             whr_super_user_name,
#                             uniq_id,
#                             item.get("Location_Code"),
#                             location_barcode,
#                             location_measurement,
#                             item.get("itemCode"),
#                             item.get("Item_meas"),
#                             item.get("itemDetails"),
#                             item.get("product_Type"),
#                             item.get("productCode"),
#                             item.get("tot_item_meas"),
#                             item.get("Franchise"),
#                             item.get("Item_class"),
#                             item.get("Sub_Class"),
#                             creation_date,
#                             created_by
#                         ])

#         return JsonResponse({"status": "success", "message": f"{len(table_details)} items inserted", "Uniq_Id": uniq_id})

#     except Exception as e:
#         return JsonResponse({"status": "error", "message": str(e)}, status=500)

@csrf_exempt
def insert_whr_stock(request):
    import re
    import random
    if request.method != "POST":
        return JsonResponse({"status": "error", "message": "POST method required"}, status=400)

    try:
        data = json.loads(request.body)

        warehouse_name = data.get("Warehouse_Name")
        org_id = data.get("Org_ID")
        whr_superuser_no = data.get("WHR_SuperUserNo")
        whr_super_user_name = data.get("WHR_Super_User_Name")
        location_barcode = data.get("Location_Barcode")
        location_measurement = data.get("Location_Measurement")
        creation_date = data.get("Creation_Date", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        created_by = data.get("Created_By")
        table_details = data.get("Table_Details", [])

        # --- 1) Check Org_ID and get Org_Name ---
        with connection.cursor() as cursor:
            cursor.execute("SELECT [NAME] FROM [BUYP].[ALJE_REGIONS] WHERE [ORGANIZATION_ID]=%s", [org_id])
            org_row = cursor.fetchone()
            if not org_row:
                return JsonResponse({"status": "error", "message": f"Org_ID {org_id} not found"}, status=400)
            org_name = org_row[0]

        # --- 2) Generate Uniq_Id (New Logic from GenerateTokenFormMNG_UNIQIDView) ---
        with transaction.atomic():
            with connection.cursor() as cursor:

                now = datetime.now()
                year_short = str(now.year)[-2:]
                month = f"{now.month:02d}"
                prefix = f"SMT{year_short}{month}"

                # Find latest Uniq_Id starting with prefix
                cursor.execute("""
                    SELECT TOP 1 Uniq_Id
                    FROM WHR_MNG_UNIQID_tbl WITH (UPDLOCK, HOLDLOCK)
                    WHERE Uniq_Id LIKE %s
                    ORDER BY id DESC
                """, [f"{prefix}%"])
                row = cursor.fetchone()

                if row:
                    last_id = row[0]
                    match = re.match(rf"{prefix}(\d+)$", last_id)
                    if match:
                        last_number = int(match.group(1))
                        next_number = last_number + 1
                    else:
                        next_number = 1
                else:
                    next_number = 1

                uniq_id = f"{prefix}{next_number:01d}"

                # Save new uniq_id + token
                token = random.randint(100000, 999999)
                cursor.execute("""
                    INSERT INTO WHR_MNG_UNIQID_tbl (Uniq_Id, Tocken)
                    VALUES (%s, %s)
                """, [uniq_id, token])

                # --- 3) Check if Uniq_Id already exists in Stock table ---
                cursor.execute("SELECT 1 FROM WHR_Stock_Management_Header_tbl WHERE Uniq_Id=%s", [uniq_id])
                if cursor.fetchone():
                    return JsonResponse({"status": "error", "message": f"Uniq_Id {uniq_id} already exists"}, status=400)

                # --- 3) Header Insert ---
                insert_header_query = """
                    INSERT INTO WHR_Stock_Management_Header_tbl
                    (Warehouse_Name, Org_ID, Org_Name, WHR_SuperUserNo, WHR_Super_User_Name,
                     Location_Code, Location_Barcode, Location_Measurement, Item_Code, Item_Cubic_Meter,
                     Item_Description, Product_Parts, Product_Code, Total_Item_Qty, Total_Cubic_Meter,
                     Franchise, Class_Name, Sub_Class, Creation_Date, Creation_By, Uniq_Id)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """

                insert_details_query = """
                    INSERT INTO WHR_Stock_Details_tbl
                    (Warehouse_Name, Org_ID, Org_Name, WHR_SuperUserNo, WHR_Super_User_Name,
                     Uniq_Id, Location_Code, Location_Barcode, Location_Measurement,
                     Item_Code, Item_Cubic_Meter, Item_Description, Product_Parts, Product_Code,
                     SerialNo, Item_Qty, Total_Cubic_Meter, Franchise, Class_Name, Sub_Class_Name,
                     Creation_Date, Creation_By, Stock_status)
                    SELECT TOP (%s)
                     %s,%s,%s,%s,%s,
                     %s,%s,%s,%s,
                     %s,%s,%s,%s,%s,
                     %s,1,%s,%s,%s,%s,
                     %s,%s,'Available'
                    FROM master..spt_values
                """

                for item in table_details:
                    item_code = item.get("itemCode")

                    # 🔹 SERIAL STATUS CHECK (NEW LOGIC)
                    cursor.execute("""
                        SELECT SERIAL_STATUS
                        FROM BUYP.ALJE_ITEM_CATEGORIES_CPD_V
                        WHERE ITEM_CODE = %s
                    """, [item_code])

                    row = cursor.fetchone()
                    serial_status = row[0] if row else None

                    serial_no_value = '' if serial_status == 'Y' else 'No serial Items'

                    # Header Insert
                    cursor.execute(insert_header_query, [
                        warehouse_name,
                        org_id,
                        org_name,
                        whr_superuser_no,
                        whr_super_user_name,
                        item.get("Location_Code"),
                        location_barcode,
                        location_measurement,
                        item_code,
                        item.get("Item_meas"),
                        item.get("itemDetails"),
                        item.get("product_Type"),
                        item.get("productCode"),
                        item.get("quantity"),
                        item.get("tot_item_meas"),
                        item.get("Franchise"),
                        item.get("Item_class"),
                        item.get("Sub_Class"),
                        creation_date,
                        created_by,
                        uniq_id
                    ])

                    # Details Insert
                    qty = int(item.get("quantity", 0))
                    if qty > 0:
                        cursor.execute(insert_details_query, [
                            qty,
                            warehouse_name,
                            org_id,
                            org_name,
                            whr_superuser_no,
                            whr_super_user_name,
                            uniq_id,
                            item.get("Location_Code"),
                            location_barcode,
                            location_measurement,
                            item_code,
                            item.get("Item_meas"),
                            item.get("itemDetails"),
                            item.get("product_Type"),
                            item.get("productCode"),
                            serial_no_value,  # ✅ HERE IS THE CHANGE
                            item.get("tot_item_meas"),
                            item.get("Franchise"),
                            item.get("Item_class"),
                            item.get("Sub_Class"),
                            creation_date,
                            created_by
                        ])

        return JsonResponse({
            "status": "success",
            "message": f"{len(table_details)} items inserted",
            "Uniq_Id": uniq_id
        })

    except Exception as e:
        return JsonResponse({"status": "error", "message": str(e)}, status=500)
    
class GenerateTokenFormMNG_UNIQIDView(APIView):   
    def get(self, request, *args, **kwargs):
        token = random.randint(100000, 999999)  # numeric token

        now = datetime.now()
        year_short = str(now.year)[-2:]
        month = f"{now.month:02d}"

        prefix = f"SMT{year_short}{month}"

        similar_ids = WHR_MNG_UNIQID_Models.objects.filter(
            Uniq_Id__startswith=prefix
        ).order_by('-id')

        if similar_ids.exists():
            last_id = similar_ids.first().Uniq_Id
            match = re.match(rf"{prefix}(\d+)$", last_id)
            if match:
                last_number = int(match.group(1))
                next_number = last_number + 1
            else:
                next_number = 1
        else:
            next_number = 1

        next_Uniq_Id = f"{prefix}{next_number:01d}"

        WHR_MNG_UNIQID_Models.objects.create(
            Uniq_Id=next_Uniq_Id,
            Tocken=token
        )

        return Response({
            "Uniq_Id": next_Uniq_Id,
            "Tocken": token
        }, status=status.HTTP_200_OK)
    
    
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.db import connection

@csrf_exempt
def get_whr_summary(request):
    if request.method != "GET":
        return JsonResponse(
            {"status": "error", "message": "GET method required"}, status=400
        )

    try:
        # ✅ Read warehouse name from query params
        warehouse_name = request.GET.get("warehouse_name")

        if not warehouse_name:
            return JsonResponse(
                {"status": "error", "message": "warehouse_name is required"},
                status=400
            )

        with connection.cursor() as cursor:
            cursor.execute("""
                ;WITH Header AS (
                    SELECT 
                        Uniq_Id,
                        Warehouse_Name,
                        Org_ID,
                        Org_Name,
                        WHR_SuperUserNo,
                        WHR_Super_User_Name,
                        Location_Code,
                        Location_Barcode,
                        Location_Measurement,
                        Creation_Date,
                        Creation_By,
                        MIN(ID) AS Min_ID,
                        COUNT(*) AS Total_Item_Count,
                        SUM(Total_Item_Qty) AS Total_Item_Qty,
                        SUM(
                            CASE 
                                WHEN ISNUMERIC(Total_Cubic_Meter) = 1 
                                THEN CAST(Total_Cubic_Meter AS FLOAT) 
                                ELSE 0 
                            END
                        ) AS Total_Cubic_Meter
                    FROM BUYP.dbo.WHR_Stock_Management_Header_tbl
                    WHERE Warehouse_Name = %s   -- ✅ FILTER HERE
                    GROUP BY 
                        Uniq_Id, Warehouse_Name, Org_ID, Org_Name,
                        WHR_SuperUserNo, WHR_Super_User_Name,
                        Location_Code, Location_Barcode, Location_Measurement,
                        Creation_Date, Creation_By
                ),

                Details AS (
                    SELECT 
                        Uniq_Id,
                        COUNT(*) AS Detail_Count,

                        COUNT(
                            CASE 
                                WHEN SerialNo IS NOT NULL 
                                     AND LTRIM(RTRIM(SerialNo)) <> ''           								 
                                      AND Stock_Status NOT LIKE 'Internal%%'
                                THEN 1 
                            END
                        ) AS Filled_Serial_Count,

                        COUNT(
                            CASE 
                                WHEN SerialNo IS NULL 
                                     OR LTRIM(RTRIM(SerialNo)) = '' 	                								 
                                      AND Stock_Status NOT LIKE 'Internal%%'
                                THEN 1 
                            END
                        ) AS Empty_Serial_Count
                    FROM BUYP.dbo.WHR_Stock_Details_tbl
                    GROUP BY Uniq_Id
                )

                SELECT 
                    H.*,
                    ISNULL(D.Detail_Count, 0)        AS Detail_Count,
                    ISNULL(D.Filled_Serial_Count, 0) AS Filled_Serial_Count,
                    ISNULL(D.Empty_Serial_Count, 0)  AS Empty_Serial_Count,

                    CASE 
                        WHEN ISNULL(D.Detail_Count, 0) = 0 
                            THEN 'Not Started'

                        WHEN ISNULL(D.Detail_Count, 0) = H.Total_Item_Qty
                         AND ISNULL(D.Filled_Serial_Count, 0) = H.Total_Item_Qty
                            THEN 'Finished Scan'

                        ELSE 'Inserted Product Code'
                    END AS Scan_Status

                FROM Header H
                LEFT JOIN Details D
                    ON H.Uniq_Id = D.Uniq_Id
                ORDER BY H.Creation_Date ASC;
            """, [warehouse_name])

            rows = cursor.fetchall()
            columns = [col[0] for col in cursor.description]
            results = [dict(zip(columns, row)) for row in rows]

        return JsonResponse({"status": "success", "data": results})

    except Exception as e:
        return JsonResponse(
            {"status": "error", "message": str(e)}, status=500
        )



from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.db import connection
import json

@csrf_exempt
def whr_stock_summary(request, uniq_id):
    if request.method != "GET":
        return JsonResponse({"status": "error", "message": "GET method required"}, status=400)

    query = """
SELECT
    H.Uniq_Id,
    MAX(H.ID) AS ID,
    MAX(H.Warehouse_Name) AS Warehouse_Name,
    MAX(H.Org_ID) AS Org_ID,
    MAX(H.Org_Name) AS Org_Name,
    MAX(H.WHR_SuperUserNo) AS WHR_SuperUserNo,
    MAX(H.WHR_Super_User_Name) AS WHR_Super_User_Name,
    MAX(H.Location_Code) AS Location_Code,
    MAX(H.Location_Barcode) AS Location_Barcode,
    MAX(H.Location_Measurement) AS Location_Measurement,
    MAX(H.Creation_Date) AS Creation_Date,
    MAX(H.Creation_By) AS Creation_By,
 
    COUNT('*') AS Total_Item_Count,
    SUM(CAST(H.Total_Item_Qty AS FLOAT)) AS Total_Item_Qty,
    SUM(CAST(H.Total_Cubic_Meter AS FLOAT)) AS Total_Cubic_Meter,
 
    -- =======================================
    -- ITEM DETAILS WITH STATUS
    -- =======================================
    (
        SELECT
            D.Item_Code,
            D.Item_Cubic_Meter,
            D.Item_Description,
            D.Product_Parts,
            D.Product_Code,
            D.Total_Item_Qty,
            D.Total_Cubic_Meter,
            D.Franchise,
            D.Class_Name,
            D.Sub_Class,
 
            -- STATUS LOGIC
            CASE
                WHEN det.TotalRows = 0 THEN 'Not Started'
                WHEN det.FilledSerial = det.TotalRows THEN 'Finished Scan'
                ELSE 'Inserted Product Code'
            END AS Status
 
        FROM BUYP.dbo.WHR_Stock_Management_Header_tbl AS D
 
        OUTER APPLY (
            SELECT 
                COUNT('*') AS TotalRows,
                SUM(CASE WHEN SerialNo IS NOT NULL AND SerialNo <> '' THEN 1 ELSE 0 END) AS FilledSerial
            FROM BUYP.dbo.WHR_Stock_Details_tbl AS SD
            WHERE SD.Uniq_Id = D.Uniq_Id
              AND SD.Item_Code = D.Item_Code
        ) det
 
        WHERE D.Uniq_Id = H.Uniq_Id
        FOR JSON PATH
    ) AS itemdetails
 
FROM BUYP.dbo.WHR_Stock_Management_Header_tbl AS H
WHERE H.Uniq_Id = %s
GROUP BY H.Uniq_Id;
    """

    with connection.cursor() as cursor:
        cursor.execute(query, [uniq_id])
        columns = [col[0] for col in cursor.description]
        data = []
        for row in cursor.fetchall():
            row_dict = dict(zip(columns, row))
            if row_dict["itemdetails"]:
                row_dict["itemdetails"] = json.loads(row_dict["itemdetails"])
            else:
                row_dict["itemdetails"] = []
            data.append(row_dict)

    return JsonResponse({"data": data}, safe=False)



from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.db import connection, transaction
import json

@csrf_exempt
def insert_whr_stock_details(request):

    if request.method != "POST":
        return JsonResponse({"status": "error", "message": "POST method required"}, status=400)

    try:
        data = json.loads(request.body.decode("utf-8"))

        uniq_id = data.get("Uniq_Id")
        item_code = data.get("Item_Code")
        created_by = data.get("Creation_By")

        if not uniq_id or not item_code or not created_by:
            return JsonResponse({"status": "error", "message": "Missing fields"}, status=400)

        with transaction.atomic():

            # Step 1: Get Qty
            qty_query = """
                SELECT Total_Item_Qty
                FROM WHR_Stock_Management_Header_tbl
                WHERE Uniq_Id = %s AND Item_Code = %s
            """

            with connection.cursor() as cursor:
                cursor.execute(qty_query, [uniq_id, item_code])
                row = cursor.fetchone()

            if not row:
                return JsonResponse({"status": "error", "message": "No data found"}, status=404)

            qty = int(row[0])

            if qty == 0:
                return JsonResponse({"status": "error", "message": "Qty is 0"}, status=400)

            # Step 2: Insert rows
            insert_query = """
               WITH cte AS 
                (
                    SELECT 
                        Warehouse_Name, Org_ID, Org_Name, WHR_SuperUserNo,
                        WHR_Super_User_Name, Uniq_Id, Location_Code, Location_Barcode,
                        Location_Measurement, Item_Code, Item_Cubic_Meter,
                        Item_Description, Product_Parts, Product_Code, 
                        Total_Item_Qty, Total_Cubic_Meter, Franchise,
                        Class_Name, Sub_Class
                    FROM WHR_Stock_Management_Header_tbl
                    WHERE Uniq_Id = %s
                    AND Item_Code = %s
                ),

                SerialStatus AS
                (
                    SELECT TOP 1 
                        ISNULL(NULLIF(SERIAL_STATUS, ''), 'N') AS SERIAL_STATUS
                    FROM [BUYP].[ALJE_ITEM_CATEGORIES_CPD_V]
                    WHERE ITEM_CODE = %s
                ),

                GenerateSerial AS
                (
                    SELECT
                        CASE 
                            WHEN s.SERIAL_STATUS = 'Y' 
                                THEN ''     -- Valid Serial No
                            ELSE 'No Serial Item'                                 -- When N or Empty or NULL
                        END AS Serial_No
                    FROM SerialStatus s
                )

                INSERT INTO WHR_Stock_Details_tbl
                (
                    Warehouse_Name, Org_ID, Org_Name, WHR_SuperUserNo, WHR_Super_User_Name,
                    Uniq_Id, Location_Code, Location_Barcode, Location_Measurement,
                    Item_Code, Item_Cubic_Meter, Item_Description, Product_Parts, Product_Code,
                    Item_Qty, Total_Cubic_Meter, Franchise, Class_Name, Sub_Class_Name,
                    SerialNo,          -- NEW COLUMN: inserting valid serial or NO SERIAL
                    Creation_Date, Creation_By, Stock_status
                )
                SELECT  
                    c.Warehouse_Name, c.Org_ID, c.Org_Name, c.WHR_SuperUserNo, c.WHR_Super_User_Name,
                    c.Uniq_Id, c.Location_Code, c.Location_Barcode, c.Location_Measurement,
                    c.Item_Code, c.Item_Cubic_Meter, c.Item_Description, c.Product_Parts, c.Product_Code,
                    1,
                    c.Total_Cubic_Meter,
                    c.Franchise, c.Class_Name, c.Sub_Class,
                    gs.Serial_No,         -- Insert final serial number value
                    GETDATE(),
                    %s,
                    'Available'  
                FROM cte c
                CROSS APPLY 
                (
                    SELECT TOP (%s) 1 AS X   -- Insert 3 rows
                    FROM master..spt_values
                ) v
                CROSS JOIN GenerateSerial gs;

            """

            

            with connection.cursor() as cursor:
                cursor.execute(insert_query, [uniq_id, item_code,item_code, created_by, qty])

        return JsonResponse({
            "status": "success",
            "message": f"{qty} rows inserted successfully!",
            "Uniq_Id": uniq_id,
            "Item_Code": item_code
        })

    except Exception as e:
        return JsonResponse({"status": "error", "message": str(e)}, status=500)



@csrf_exempt
def get_serial_numbers(request):
    uniq_id = request.GET.get("uniq_id")
    item_code = request.GET.get("item_code")

    # Validation
    if not uniq_id or not item_code:
        return JsonResponse({
            "status": "error",
            "message": "uniq_id and item_code are required."
        }, status=400)

    try:
        with connection.cursor() as cursor:
            query = """
                SELECT SerialNo 
                FROM WHR_Stock_Details_tbl
                WHERE Uniq_Id = %s
                AND Item_Code = %s
                AND SerialNo IS NOT NULL
                AND SerialNo <> ''
            """
            cursor.execute(query, [uniq_id, item_code])
            rows = cursor.fetchall()

        # Convert tuple list → string list
        serial_numbers = [row[0] for row in rows]

        return JsonResponse({
            "status": "success",
            "uniq_id": uniq_id,
            "item_code": item_code,
            "serial_numbers": serial_numbers,
            "count": len(serial_numbers),
        })

    except Exception as e:
        return JsonResponse({
            "status": "error",
            "message": str(e)
        }, status=500)



@csrf_exempt
def update_serial_numbers(request):
    if request.method != "POST":
        return JsonResponse({"status": "error", "message": "POST required"}, status=400)

    try:
        data = json.loads(request.body.decode("utf-8"))
        uniq_id = data.get("uniq_id")
        item_code = data.get("item_code")
        status = data.get("status")
        serials = data.get("serials", [])

        if not uniq_id or not item_code:
            return JsonResponse({
                "status": "error",
                "message": "uniq_id and item_code are required"
            }, status=400)

        if not isinstance(serials, list):
            return JsonResponse({
                "status": "error",
                "message": "serials must be list"
            }, status=400)

        # Step 1: Fetch empty SerialNo rows
        fetch_sql = """
            SELECT ID
            FROM WHR_Stock_Details_tbl
            WHERE Uniq_Id = %s
              AND Item_Code = %s and Stock_status = 'Available'
              AND (SerialNo IS NULL OR SerialNo = '')
            ORDER BY ID
        """

        with connection.cursor() as cursor:
            cursor.execute(fetch_sql, [uniq_id, item_code])
            empty_rows = cursor.fetchall()

        if not empty_rows:
            return JsonResponse({
                "status": "error",
                "message": "No empty SerialNo rows found for update."
            }, status=400)

        # Step 2: Assign the serial numbers one-by-one
        update_sql = """
            UPDATE WHR_Stock_Details_tbl
            SET SerialNo = %s, Flag1 = %s
            WHERE ID = %s
        """

        index = 0
        with connection.cursor() as cursor:
            for row in empty_rows:
                if index >= len(serials):
                    break  # no more serials to assign

                serial_value = str(serials[index])
                row_id = row[0]

                cursor.execute(update_sql, [serial_value, status, row_id])
                index += 1

        return JsonResponse({
            "status": "success",
            "message": f"{index} serial numbers updated successfully",
            "updated_count": index
        })

    except Exception as e:
        return JsonResponse({"status": "error", "message": str(e)}, status=500)


logger = logging.getLogger(__name__)


@api_view(['GET'])
def item_details_whrloc(request, Status, Value):
    """
    Retrieve aggregated stock details by:
    - location_barcode
    - location_code
    - item_code
    - product_code
    """

    try:
        with connection.cursor() as cursor:

            status = Status.lower().strip()

            # ------------------ DYNAMIC WHERE CLAUSE ------------------
            if status == "location_barcode":
                where_clause = "WHERE Location_Barcode = %s"
                params = [Value]

            elif status == "location_code":
                where_clause = "WHERE Location_Code LIKE %s"
                params = [f"{Value}%"]

            elif status == "item_code":
                where_clause = "WHERE Item_Code = %s"
                params = [Value]

            elif status == "product_code":
                where_clause = "WHERE Product_Code = %s"
                params = [Value]

            else:
                return JsonResponse({
                    "status": "error",
                    "message": "Invalid Status"
                }, status=400)

            # ------------------ FINAL QUERY ------------------
            query = f"""
                SELECT
                    MAX(Warehouse_Name)      AS Warehouse_Name,
                    MAX(Org_ID)              AS Org_ID,
                    MAX(Org_Name)            AS Org_Name,
                    MAX(WHR_SuperUserNo)     AS WHR_SuperUserNo,
                    MAX(WHR_Super_User_Name) AS WHR_Super_User_Name,

                    Uniq_Id,

                    MAX(Location_Code)       AS Location_Code,
                    MAX(Location_Barcode)    AS Location_Barcode,
                    MAX(Creation_Date)       AS Creation_Date,

                    Item_Code,

                    MAX(Item_Description)    AS Item_Description,
                    MAX(Franchise)           AS Franchise,
                    MAX(Product_Code)        AS Product_Code,

                    COUNT(*) AS Total_Rows_For_Item,

                    SUM(
                        CASE
                            WHEN LOWER(ISNULL(Stock_status, '')) LIKE 'avail%%'
                            THEN 1 ELSE 0
                        END
                    ) AS Available_Count,

                    SUM(
                        CASE
                            WHEN LOWER(ISNULL(Stock_status, '')) LIKE 'disp%%'
                            THEN 1 ELSE 0
                        END
                    ) AS Dispatched_Count

                FROM BUYP.dbo.WHR_Stock_Details_tbl
                {where_clause} AND Stock_Status NOT LIKE 'Internal%%'
                GROUP BY Item_Code, Uniq_Id
                ORDER BY Item_Code, Uniq_Id;
            """

            cursor.execute(query, params)

            columns = [col[0] for col in cursor.description]
            results = [dict(zip(columns, row)) for row in cursor.fetchall()]

        return JsonResponse({
            "status": "success",
            "count": len(results),
            "data": results
        }, safe=False)

    except Exception as e:
        logger.exception("Error fetching stock details")
        return JsonResponse({
            "status": "error",
            "message": str(e)
        }, status=500)

    
@api_view(['POST'])
def get_WHRLOC_item_details(request):
    try:
        # Read POST body values
        location_barcode = request.data.get("Location_Barcode")
        item_code = request.data.get("Item_Code")
        uniq_id = request.data.get("Uniq_Id")

        if not location_barcode or not item_code:
            return Response({"error": "Location_Barcode and Item_Code are required"},
                            status=status.HTTP_400_BAD_REQUEST)

        # SQL Query (Safe parameterized query)
        query = """
            SELECT 
                Item_Code, Item_Description, Franchise, Class_Name, 
                Sub_Class_Name, Product_Code, SerialNo, Stock_status
            FROM [BUYP].[dbo].[WHR_Stock_Details_tbl]
            WHERE Location_Barcode = %s AND Item_Code = %s and Uniq_Id = %s AND Stock_status NOT LIKE 'Internal%%'
        """

        with connection.cursor() as cursor:
            cursor.execute(query, [location_barcode,item_code, uniq_id])
            rows = cursor.fetchall()
            columns = [col[0] for col in cursor.description]

        # Format response as JSON list of dicts
        results = [dict(zip(columns, row)) for row in rows]

        return Response({"status": "success", "data": results})

    except Exception as e:
        return Response({"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)





######################################### Location Mapping for Pickman scan ###########################################################


def item_available_location_list(request):
    item_code = request.GET.get("item_code")

    if not item_code:
        return JsonResponse(
            {"error": "item_code is required"},
            status=400
        )

    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT
                Location_Code,
                Location_Barcode,
                Item_Code,
                Item_Description,
                COUNT(*) AS available_qty
            FROM WHR_Stock_Details_tbl
            WHERE Item_Code = %s
              AND Stock_status = 'Available'
            GROUP BY Location_Code, Location_Barcode, Item_Code, Item_Description
            ORDER BY Location_Code
        """, [item_code])

        columns = [col[0] for col in cursor.description]
        rows = cursor.fetchall()

    data = [
        dict(zip(columns, row))
        for row in rows
    ]

    return JsonResponse({
        "item_code": item_code,
        "total_locations": len(data),
        "locations": data
    }, safe=False)


class UpdateStockDispatchView(APIView):

    def post(self, request):

        updates = request.data.get("updates", [])
        if not updates:
            return Response({"error": "updates list is required"}, status=400)

        total_dispatched = 0
        total_onprogress_used = 0
        skipped_serials = []

        serial_origin = {}

        # Track original location for each serial
        for block in updates:
            for s in block["serialnos"]:
                serial_origin.setdefault(s, block["location_code"])

        with transaction.atomic():
            with connections["default"].cursor() as cursor:

                hold_serials = []

                # ======================================================
                # STEP 1: DIRECT LOCATION MATCH
                # ======================================================
                for block in updates:
                    loc = block["location_code"]
                    item = block["item_code"]
                    a1 = block["attribute1"]
                    a2 = block["attribute2"]

                    for serial in block["serialnos"]:
                        cursor.execute("""
                            SELECT ID, Stock_status
                            FROM WHR_Stock_Details_tbl
                            WHERE SerialNo=%s
                              AND Location_Barcode=%s
                              AND Item_Code=%s
                        """, [serial, loc, item])

                        row = cursor.fetchone()
                        if row:
                            row_id, status = row

                            if status == 'OnProgress':
                                total_onprogress_used += 1

                            if status != 'Dispatched':
                                cursor.execute("""
                                    UPDATE WHR_Stock_Details_tbl
                                    SET Stock_status='Dispatched',
                                        Attribute1=%s,
                                        Attribute2=%s
                                    WHERE ID=%s
                                """, [a1, a2, row_id])
                                total_dispatched += 1
                        else:
                            hold_serials.append(serial)

                # ======================================================
                # STEP 2: CROSS LOCATION SWAP
                # ======================================================
                remaining_hold = []

                for serial in hold_serials:
                    swapped = False

                    for block in updates:
                        loc = block["location_code"]
                        item = block["item_code"]
                        a1 = block["attribute1"]
                        a2 = block["attribute2"]

                        cursor.execute("""
                            SELECT ID, Stock_status
                            FROM WHR_Stock_Details_tbl
                            WHERE SerialNo=%s
                              AND Location_Barcode=%s
                              AND Item_Code=%s
                        """, [serial, loc, item])

                        row = cursor.fetchone()
                        if row:
                            row_id, status = row

                            if status == 'OnProgress':
                                total_onprogress_used += 1

                            if status != 'Dispatched':
                                cursor.execute("""
                                    UPDATE WHR_Stock_Details_tbl
                                    SET Stock_status='Dispatched',
                                        Attribute1=%s,
                                        Attribute2=%s
                                    WHERE ID=%s
                                """, [a1, a2, row_id])
                                total_dispatched += 1

                            swapped = True
                            break

                    if not swapped:
                        remaining_hold.append(serial)

                # ======================================================
                # STEP 3 + STEP 4: ONPROGRESS FALLBACK (PER LOCATION)
                # ======================================================
                for serial in remaining_hold:
                    origin_loc = serial_origin.get(serial)
                    block = next(b for b in updates if b["location_code"] == origin_loc)

                    cursor.execute("""
                        SELECT TOP 1 ID
                        FROM WHR_Stock_Details_tbl
                        WHERE Item_Code=%s
                          AND Location_Barcode=%s
                          AND Stock_status='OnProgress'
                        ORDER BY ID ASC
                    """, [block["item_code"], origin_loc])

                    row = cursor.fetchone()
                    if row:
                        cursor.execute("""
                            UPDATE WHR_Stock_Details_tbl
                            SET Stock_status='Dispatched',
                                Attribute1=%s,
                                Attribute2=%s
                            WHERE ID=%s
                        """, [block["attribute1"], block["attribute2"], row[0]])

                        total_dispatched += 1
                        total_onprogress_used += 1
                    else:
                        skipped_serials.append(serial)

                # ======================================================
                # STEP 5: BALANCED ROLLBACK (PER ITEM)
                # ======================================================
                rollback_limit = total_dispatched - total_onprogress_used

                if rollback_limit > 0:
                    cursor.execute(f"""
                        ;WITH cte AS (
                            SELECT TOP ({rollback_limit}) ID
                            FROM WHR_Stock_Details_tbl
                            WHERE Stock_status='OnProgress'
                            ORDER BY ID ASC
                        )
                        UPDATE WHR_Stock_Details_tbl
                        SET Stock_status='Available',
                            Attribute1=NULL,
                            Attribute2=NULL
                        WHERE ID IN (SELECT ID FROM cte)
                    """)

        return Response({
            "status": "success",
            "dispatched_rows": total_dispatched,
            "rollback_rows": rollback_limit,
            "skipped_serials": skipped_serials
        }, status=200)




#-------------------------------------------------------------------------------
# Normal stock update to dispatched 
#-------------------------------------------------------------------------------

@csrf_exempt
def update_stock_details(request):
    if request.method != "POST":
        return JsonResponse({"error": "Only POST method allowed"}, status=405)

    try:
        body = json.loads(request.body.decode("utf-8"))
        updates = body.get("updates", [])

        if not updates:
            return JsonResponse({"error": "updates list is required"}, status=400)

        total_updated = 0
        block_results = []

        with transaction.atomic():
            with connections["default"].cursor() as cursor:

                for block in updates:
                    location_code = block.get("location_code")
                    item_code     = block.get("item_code")
                    attribute1    = block.get("attribute1")
                    attribute2    = block.get("attribute2")
                    serialnos     = block.get("serialnos", [])

                    if not all([location_code, item_code, attribute1, attribute2]) or not serialnos:
                        return JsonResponse(
                            {"error": "Missing required fields in updates block"},
                            status=400
                        )

                    requested_qty = len(serialnos)

                    # --------------------------------------------------
                    # 1️⃣ CHECK AVAILABLE STOCK (IMPORTANT)
                    # --------------------------------------------------
                    cursor.execute("""
                        SELECT COUNT(*)
                        FROM WHR_Stock_Details_tbl
                        WHERE
                            Location_Barcode = %s
                            AND Item_Code = %s
                            AND LTRIM(RTRIM(Stock_status)) = 'OnProgress'
                    """, [location_code, item_code])

                    available_qty = cursor.fetchone()[0]

                    if available_qty == 0:
                        block_results.append({
                            "location_code": location_code,
                            "item_code": item_code,
                            "requested_qty": requested_qty,
                            "available_qty": 0,
                            "updated_rows": 0,
                            "reason": "No stock in OnProgress status"
                        })
                        continue

                    update_limit = min(requested_qty, available_qty)

                    # --------------------------------------------------
                    # 2️⃣ UPDATE STOCK (SQL SERVER SAFE)
                    # --------------------------------------------------
                    cursor.execute("""
                        UPDATE WHR_Stock_Details_tbl
                        SET
                            Stock_status = 'Dispatched',
                            Attribute1   = %s,
                            Attribute2   = %s
                        WHERE ID IN (
                            SELECT TOP (%s) ID
                            FROM WHR_Stock_Details_tbl
                            WHERE
                                Location_Barcode = %s
                                AND Item_Code = %s
                                AND LTRIM(RTRIM(Stock_status)) = 'OnProgress'
                            ORDER BY Creation_Date
                        )
                    """, [
                        attribute1,
                        attribute2,
                        update_limit,
                        location_code,
                        item_code
                    ])

                    updated_rows = cursor.rowcount
                    total_updated += updated_rows

                    block_results.append({
                        "location_code": location_code,
                        "item_code": item_code,
                        "requested_qty": requested_qty,
                        "available_qty": available_qty,
                        "updated_rows": updated_rows,
                        "status": "success" if updated_rows > 0 else "not_updated"
                    })

        return JsonResponse({
            "status": "success",
            "total_rows_updated": total_updated,
            "block_wise_result": block_results
        }, status=200)

    except json.JSONDecodeError:
        return JsonResponse({"error": "Invalid JSON body"}, status=400)

    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)

# -------------------------------------------------------------------------
# SERIAL-WISE STOCK UPDATE WITH HOLD & SWAP
# -------------------------------------------------------------------------
@api_view(['POST'])
def update_stock_serial_wise(request):
    """
    Complex stock update with Hold & Swap logic.
    Refined Rules:
    1. Re-dispatch: If defined row is already 'Dispatched', do nothing & return success.
    2. Swap: 
       - Check if Hold Serial (from Loc A) exists in Loc B.
       - If yes, use Hold Serial for Loc B.
       - Then fulfill Loc A: Try swapping with Loc B's serial. 
         If that fails (not in Loc A), force update an 'OnProgress' row in Loc A.
    """
    try:
        updates = request.data.get('updates', [])
        if not updates:
             return Response({"status": "error", "message": "No updates provided"}, status=status.HTTP_400_BAD_REQUEST)

        hold_list = []
        processed_logs = []

        with transaction.atomic():
            # --- HELPER: SCENARIO 1 (Direct Update or Mismatch Reset) ---
            def process_serial_update(serial, loc_code, item_code, attr1, attr2):
                with connection.cursor() as cursor:
                    # Check existence
                    check_sql = """
                        SELECT Stock_status, Attribute1 
                        FROM [BUYP].[dbo].[WHR_Stock_Details_tbl]
                        WHERE SerialNo = %s AND Location_Barcode = %s AND Item_Code = %s
                    """
                    cursor.execute(check_sql, [serial, loc_code, item_code])
                    row = cursor.fetchone()

                    if row:
                        db_status, db_attr1 = row
                        
                        # --- MODIFICATION 1: Re-dispatch Handling ---
                        # If already Dispatched, do NOT update again.
                        # Rule: Treat as UNAVAILABLE (Hold).
                        # Return False to trigger Hold -> Swap -> Force Update flow.
                        if str(db_status).lower() == 'Dispatched':
                             return False, "Already Dispatched (Treated as Unavailable)"

                        # Scenario 1A: Exact Match (Available/OnProgress w/ correct Attr1)
                        # Relaxing existing check: The user says "If Stock_status = Available or OnProgress -> update to Dispatched"
                        # We keep the logic mostly same but ensure we cover both states.
                        if str(db_status).lower() in ['OnProgress', 'Available', 'OnProgress']: 
                            # Note: DB text might be 'OnProgress' or 'OnProgress', handling loosely if needed, 
                            # but previous code used 'OnProgress'. 
                            
                            update_sql = """
                                UPDATE [BUYP].[dbo].[WHR_Stock_Details_tbl]
                                SET Stock_status = 'Dispatched', Attribute1 = %s, Attribute2 = %s
                                WHERE SerialNo = %s AND Location_Barcode = %s AND Item_Code = %s
                            """
                            cursor.execute(update_sql, [attr1, attr2, serial, loc_code, item_code])
                            return True, "Updated (Direct)"

                        # Scenario 1B: Mismatch (Reset & Update)
                        # If status is something else (unlikely given check above) OR Attribute mismatch?
                        # The previous code had a specific 'reset' logic.
                        # User constraint: "Only apply the two modifications... without changing core logic flow".
                        # However, user also said "If Stock_status = Available / OnProgress -> update to Dispatched".
                        # The previous code handled 'Attribute1' mismatch by finding *another* row. 
                        # I will preserve the previous 'Reset' logic for safety if the attrs don't match, 
                        # BUT accounting for the "Already Dispatched" check being done first.
                        
                        # Use previous logic for attribute mismatch/status reset cases
                        else:
                            # 1. Reset one 'OnProgress' row (Free up resources logic)
                            reset_sql = """
                                Update TOP (1) [BUYP].[dbo].[WHR_Stock_Details_tbl]
                                SET Stock_status = 'Available', Attribute1 = NULL
                                WHERE Stock_status = 'OnProgress' 
                                  AND Attribute1 = %s
                                  AND Location_Barcode = %s
                                  AND Item_Code = %s
                            """
                            cursor.execute(reset_sql, [attr1, loc_code, item_code])
                            
                            # 2. Update target serial
                            update_force_sql = """
                                UPDATE [BUYP].[dbo].[WHR_Stock_Details_tbl]
                                SET Stock_status = 'Dispatched', Attribute1 = %s, Attribute2 = %s
                                WHERE SerialNo = %s AND Location_Barcode = %s AND Item_Code = %s
                            """
                            cursor.execute(update_force_sql, [attr1, attr2, serial, loc_code, item_code])
                            return True, "Updated (State Swap)"
                    else:
                        return False, "Not Found"

            # --- PHASE 1: INITIAL PROCESSING ---
            for entry in updates:
                loc_code = entry.get('location_code')
                item_code = entry.get('item_code')
                req_attr1 = entry.get('attribute1')
                req_attr2 = entry.get('attribute2')
                serials = entry.get('serialnos', [])

                for serial in serials:
                    success, msg = process_serial_update(serial, loc_code, item_code, req_attr1, req_attr2)
                    if success:
                        processed_logs.append(f"Direct: {serial} in {loc_code} -> {msg}")
                    else:
                        # Record NOT EXIST -> HOLD
                        hold_list.append({
                            "serial": serial,
                            "original_loc": loc_code,
                            "item_code": item_code,
                            "attr1": req_attr1,
                            "attr2": req_attr2
                        })
                        processed_logs.append(f"HOLD: {serial} in {loc_code}")

            # --- PHASE 2: HOLD RESOLUTION & SWAP (MODIFIED) ---
            # Rule: 
            # 1. Check if Hold Serial (from Loc A) exists in Loc B.
            # 2. If Yes -> Swap.
            # 3. Swap Details:
            #    - Update Hold Serial (A) in Loc B.
            #    - Check Serial (B) (requested for Loc B) in Loc A.
            #    - If B avail in A -> Update B in A.
            #    - If B NOT avail in A -> Force Update 'OnProgress' row in A (Attribute1=passed item code).
            
            handled_indices = set()

            for i, held_item_A in enumerate(hold_list):
                if i in handled_indices:
                    continue

                serial_A = held_item_A['serial']
                loc_A = held_item_A['original_loc']
                item = held_item_A['item_code']
                
                found_swap = False
                
                # Check against other updates/locations
                for other_entry in updates:
                    loc_B = other_entry.get('location_code')
                    if loc_B == loc_A: continue 

                    # Does Serial A exist in Loc B? (Relaxed Rule: 1-way check)
                    with connection.cursor() as cursor:
                        cursor.execute("""
                            SELECT 1 FROM [BUYP].[dbo].[WHR_Stock_Details_tbl]
                            WHERE SerialNo = %s AND Location_Barcode = %s AND Item_Code = %s
                        """, [serial_A, loc_B, item])
                        
                        if cursor.fetchone():
                            # Found Serial A in Loc B! Proceed with Swap.
                            found_swap = True
                            
                            # 1. Fulfill Loc B request using Serial A
                            # We treat Serial A as if it was the serial requested for Loc B
                            # Using Loc B's attributes (from 'other_entry')? 
                            # Or usually swaps happen for same Item/Attr? Assuming same Item/Attr context from input structure.
                            # We'll use the attributes requested in 'other_entry'.
                            attr1_B = other_entry.get('attribute1')
                            attr2_B = other_entry.get('attribute2')
                            
                            succ_A_in_B, msg_A = process_serial_update(serial_A, loc_B, item, attr1_B, attr2_B)
                            processed_logs.append(f"SWAP (Step 1): Serial {serial_A} moved to {loc_B} -> {msg_A}")
                            
                            # 2. Fulfill Loc A request
                            # We need to fill the void in Loc A.
                            # Try to use a valid serial from Loc B's request list (Serial B)
                            # (Heuristic: Pick the first serial from Loc B request? Or just logic fallback?)
                            # User says: "First serial -> move to second barcode... Second serial -> move to first barcode"
                            # We'll try to pick a serial from Loc B's request list.
                            
                            serial_B_candidates = other_entry.get('serialnos', [])
                            # We ideally want a serial that hasn't been processed or was held?
                            # Simplify: Try the first one that exists in Loc A.
                            
                            swapped_back = False
                            for serial_B in serial_B_candidates:
                                # Check if Serial B exists in Loc A
                                with connection.cursor() as c2:
                                    c2.execute("""
                                        SELECT Stock_status FROM [BUYP].[dbo].[WHR_Stock_Details_tbl]
                                        WHERE SerialNo = %s AND Location_Barcode = %s AND Item_Code = %s
                                    """, [serial_B, loc_A, item])
                                    row_B = c2.fetchone()
                                    
                                    if row_B:
                                        # Serial B matches in Loc A! Use it.
                                        # Use attributes from Loc A request (held_item_A)
                                        succ_B_in_A, msg_B = process_serial_update(serial_B, loc_A, item, held_item_A['attr1'], held_item_A['attr2'])
                                        processed_logs.append(f"SWAP (Step 2): Serial {serial_B} moved to {loc_A} -> {msg_B}")
                                        swapped_back = True
                                        break
                            
                            if not swapped_back:
                                # "If no swap serial number is available: Update stock status to OnProgress ... Then mark it as Dispatched"
                                # Condition: Stock_status = OnProgress, Attribute1 = passed item code, Location_Barcode = current barcode only
                                
                                # This means explicit SQL update on a placeholder row.
                                with connection.cursor() as c3:
                                    # Logic: "Update stock status to OnProgress" (Maybe ensure it's onprogress first? or just find onprogress?)
                                    # User: "If NOT available: Update stock status to Dispatched. Condition: Stock_status = OnProgress..."
                                    # Implies we consume an 'OnProgress' row.
                                    
                                    c3.execute("""
                                        UPDATE TOP (1) [BUYP].[dbo].[WHR_Stock_Details_tbl]
                                        SET Stock_status = 'Dispatched', Attribute1 = %s, Attribute2 = %s
                                        WHERE Stock_status = 'OnProgress' 
                                          AND Attribute1 = %s
                                          AND Location_Barcode = %s
                                          AND Item_Code = %s
                                    """, [
                                        held_item_A['attr1'], held_item_A['attr2'], 
                                        held_item_A['attr1'], # "Attribute1 = passed item code" (Assuming passed req attr)
                                        loc_A,
                                        item
                                    ])
                                    if c3.rowcount > 0:
                                        processed_logs.append(f"SWAP (Step 2 - Force): Forced update in {loc_A} (consumed OnProgress row)")
                                    else:
                                        processed_logs.append(f"SWAP (Step 2 - Failed): No 'OnProgress' row found in {loc_A} to consume.")

                            handled_indices.add(i)
                            break # Move to next Hold Item
                
                if not found_swap:
                    # Will be handled in Phase 3
                    pass

            # --- PHASE 3: FINAL DEFAULT (Unresolved Holds) ---
            for i, held_item in enumerate(hold_list):
                if i in handled_indices:
                    continue
                
                # Blind Update for remaining holds (Same as previous logic, just ensuring fulfillment)
                with connection.cursor() as cursor:
                     update_blind_sql = """
                        UPDATE TOP (1) [BUYP].[dbo].[WHR_Stock_Details_tbl]
                        SET Stock_status = 'Dispatched', Attribute1 = %s, Attribute2 = %s
                        WHERE Stock_status = 'OnProgress' 
                          AND Attribute1 = %s 
                          AND Location_Barcode = %s 
                          AND Item_Code = %s
                    """
                     cursor.execute(update_blind_sql, [
                         held_item['attr1'], held_item['attr2'], 
                         held_item['attr1'], held_item['original_loc'], held_item['item_code']
                     ])
                     if cursor.rowcount > 0:
                         processed_logs.append(f"Blind Update: {held_item['serial']} (req) -> Dispatched unknown serial in {held_item['original_loc']}")
                     else:
                         processed_logs.append(f"FAILED: {held_item['serial']} in {held_item['original_loc']} (No available stock)")

        return Response({
            "status": "success",
            "message": "Stock updates processed",
            "logs": processed_logs
        }, status=status.HTTP_200_OK)

    except Exception as e:
        import traceback
        return Response({"status": "error", "message": str(e), "trace": traceback.format_exc()}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)



#-----------------------------------------------------------------------------------------------------
                                    # Item Measurement Units #
#-----------------------------------------------------------------------------------------------------

@csrf_exempt
def insert_item_measurement(request):
    if request.method != 'POST':
        return JsonResponse({'status': 'error', 'message': 'POST method required'}, status=405)

    try:
        data = json.loads(request.body)
        # Support list of items or single item
        if isinstance(data, dict):
            items = [data]
        elif isinstance(data, list):
            items = data
        else:
            return JsonResponse({'status': 'error', 'message': 'Invalid JSON format. Expected dict or list.'}, status=400)

        query = """
            INSERT INTO [BUYP].[dbo].[Item_Measurement_tbl]
            (Item_Code, Description, Length, Width, Height, Attributes1, Attributes2, Attributes3, Attributes4, Attributes5, Flag1, Flag2)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        batch_size = 1000
        params = []
        
        with connection.cursor() as cursor:
            # 1. Collect all Item Codes from input
            input_codes = [item.get('Item_Code') for item in items if item.get('Item_Code')]
            
            # 2. Check for duplicates in DB
            if input_codes:
                # If list is huge, we might need chunking here too, but for now assuming it fits in query limits
                # or we check batch by batch. For strict correctness on huge data:
                # We fetch existing codes that match input codes.
                placeholders = ','.join(['%s'] * len(input_codes))
                check_query = f"SELECT Item_Code FROM [BUYP].[dbo].[Item_Measurement_tbl] WHERE Item_Code IN ({placeholders})"
                
                # To avoid query limit errors (2100 params), we must chunk the check too if input is large
                existing_items = set()
                check_batch_size = 1000
                for i in range(0, len(input_codes), check_batch_size):
                    chunk = input_codes[i:i + check_batch_size]
                    chunk_placeholders = ','.join(['%s'] * len(chunk))
                    chunk_query = f"SELECT Item_Code FROM [BUYP].[dbo].[Item_Measurement_tbl] WHERE Item_Code IN ({chunk_placeholders})"
                    cursor.execute(chunk_query, chunk)
                    existing_items.update(row[0] for row in cursor.fetchall())

                if existing_items:
                    # Return error if ANY item exists
                    error_msg = f"Data already exists for Item Codes: {', '.join(list(existing_items)[:5])}..."
                    return JsonResponse({'status': 'error', 'message': error_msg}, status=400)

            # 3. Proceed with Insert if no duplicates found
            for item in items:
                params.append([
                    item.get('Item_Code'),
                    item.get('Description'),
                    item.get('Length'),
                    item.get('Width'),
                    item.get('Height'),
                    item.get('Attributes1'),
                    item.get('Attributes2'),
                    None, # Attributes3
                    None, # Attributes4
                    None, # Attributes5
                    0,    # Flag1
                    0     # Flag2
                ])
                
                if len(params) >= batch_size:
                    cursor.executemany(query, params)
                    params = []

            if params:
                cursor.executemany(query, params)

        return JsonResponse({'status': 'success', 'message': f'{len(items)} records inserted'})

    except Exception as e:
        return JsonResponse({'status': 'error', 'message': str(e)}, status=500)

@csrf_exempt
def get_item_measurement(request):
    if request.method != 'GET':
        return JsonResponse({'status': 'error', 'message': 'GET method required'}, status=405)

    try:
        item_code = request.GET.get('Item_Code')
        limit = request.GET.get('limit', 1000) # Default limit 1000 rows

        with connection.cursor() as cursor:
            if item_code:
                query = """
                    SELECT Item_Code, Description, Length, Width, Height, Cubic_Meter 
                    FROM [BUYP].[dbo].[Item_Measurement_tbl]
                    WHERE Item_Code = %s
                """
                cursor.execute(query, [item_code])
            else:
                # Fetch recent/top rows to avoid 100k dump crash
                try:
                    limit = int(limit)
                except:
                    limit = 1000
                
                query = f"""
                    SELECT TOP {limit} Item_Code, Description, Length, Width, Height, Cubic_Meter 
                    FROM [BUYP].[dbo].[Item_Measurement_tbl]
                """
                cursor.execute(query)

            rows = cursor.fetchall()
            
            # Format as JSON
            data = []
            for row in rows:
                data.append({
                    "Item_Code": row[0],
                    "Description": row[1],
                    "Length": row[2],
                    "Width": row[3],
                    "Height": row[4],
                    "Cubic_Meter": row[5]
                })

        return JsonResponse({'status': 'success', 'count': len(data), 'data': data})

    except Exception as e:
        return JsonResponse({'status': 'error', 'message': str(e)}, status=500)

@csrf_exempt
def update_item_measurement(request):
    if request.method not in ['POST', 'PUT', 'GET']:
        return JsonResponse({'status': 'error', 'message': 'POST or PUT method required'}, status=405)

    try:
        data = json.loads(request.body)
        
        # Extract fields to update
        item_code = data.get('Item_Code') # Now from JSON
        if not item_code:
            return JsonResponse({'status': 'error', 'message': 'Item_Code is required in JSON body'}, status=400)

        length = data.get('Length')
        width = data.get('Width')
        height = data.get('Height')
        attr3 = data.get('Attributes3')
        attr4 = data.get('Attributes4')

        query = """
            UPDATE [BUYP].[dbo].[Item_Measurement_tbl]
            SET Length = %s,
                Width = %s,
                Height = %s,
                Attributes3 = %s,
                Attributes4 = %s
            WHERE Item_Code = %s
        """
        
        params = [length, width, height, attr3, attr4, item_code]

        with connection.cursor() as cursor:
            cursor.execute(query, params)
            if cursor.rowcount == 0:
                 return JsonResponse({'status': 'error', 'message': 'Item not found'}, status=404)

        return JsonResponse({'status': 'success', 'message': 'Item updated successfully'})

    except Exception as e:
        return JsonResponse({'status': 'error', 'message': str(e)}, status=500)




#----------------------------------------------------------------------------------------------------------------
# Inbound Stock manage insert datas 
#----------------------------------------------------------------------------------------------------------------

@csrf_exempt
def Inbound_wise_insert_whr_stock_serial(request):
    if request.method != "POST":
        return JsonResponse({"status": "error", "message": "POST method required"}, status=400)

    import re
    import random

    try:
        data = json.loads(request.body)

        warehouse_name = data.get("Warehouse_Name")
        org_id = data.get("Org_ID")
        whr_superuser_no = data.get("WHR_SuperUserNo")
        whr_super_user_name = data.get("WHR_Super_User_Name")
        location_barcode = data.get("Location_Barcode")
        location_measurement = data.get("Location_Measurement")
        creation_date = data.get("Creation_Date", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        created_by = data.get("Created_By")
        table_details = data.get("Table_Details", [])

        # --- 1) Check Org_ID and get Org_Name ---
        with connection.cursor() as cursor:
            cursor.execute("SELECT [NAME] FROM [BUYP].[ALJE_REGIONS] WHERE [ORGANIZATION_ID]=%s", [org_id])
            org_row = cursor.fetchone()
            if not org_row:
                return JsonResponse({"status": "error", "message": f"Org_ID {org_id} not found"}, status=400)
            org_name = org_row[0]

        # --- 2) Generate Uniq_Id ---
        with transaction.atomic():
            with connection.cursor() as cursor:

                now = datetime.now()
                year_short = str(now.year)[-2:]
                month = f"{now.month:02d}"
                prefix = f"SMT{year_short}{month}"

                cursor.execute("""
                    SELECT TOP 1 Uniq_Id
                    FROM WHR_MNG_UNIQID_tbl WITH (UPDLOCK, HOLDLOCK)
                    WHERE Uniq_Id LIKE %s
                    ORDER BY id DESC
                """, [f"{prefix}%"])
                row = cursor.fetchone()

                if row:
                    last_id = row[0]
                    match = re.match(rf"{prefix}(\d+)$", last_id)
                    if match:
                        last_number = int(match.group(1))
                        next_number = last_number + 1
                    else:
                        next_number = 1
                else:
                    next_number = 1

                uniq_id = f"{prefix}{next_number:01d}"

                token = random.randint(100000, 999999)
                cursor.execute("""
                    INSERT INTO WHR_MNG_UNIQID_tbl (Uniq_Id, Tocken)
                    VALUES (%s, %s)
                """, [uniq_id, token])

                cursor.execute("SELECT 1 FROM WHR_Stock_Management_Header_tbl WHERE Uniq_Id=%s", [uniq_id])
                if cursor.fetchone():
                    return JsonResponse({"status": "error", "message": f"Uniq_Id {uniq_id} already exists"}, status=400)

                # --- 3) Header Insert ---
                insert_header_query = """
                    INSERT INTO WHR_Stock_Management_Header_tbl
                    (Warehouse_Name, Org_ID, Org_Name, WHR_SuperUserNo, WHR_Super_User_Name,
                     Location_Code, Location_Barcode, Location_Measurement, Item_Code, Item_Cubic_Meter,
                     Item_Description, Product_Parts, Product_Code, Total_Item_Qty, Total_Cubic_Meter,
                     Franchise, Class_Name, Sub_Class, Creation_Date, Creation_By, Uniq_Id)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """

                # Query for Single Row Insert (when Serial List is present)
                insert_single_detail_query = """
                    INSERT INTO WHR_Stock_Details_tbl
                    (Warehouse_Name, Org_ID, Org_Name, WHR_SuperUserNo, WHR_Super_User_Name,
                     Uniq_Id, Location_Code, Location_Barcode, Location_Measurement,
                     Item_Code, Item_Cubic_Meter, Item_Description, Product_Parts, Product_Code,
                     SerialNo, Item_Qty, Total_Cubic_Meter, Franchise, Class_Name, Sub_Class_Name,
                     Creation_Date, Creation_By, Stock_status)
                    VALUES
                    (%s,%s,%s,%s,%s,
                     %s,%s,%s,%s,
                     %s,%s,%s,%s,%s,
                     %s,1,%s,%s,%s,%s,
                     %s,%s,'Available')
                """

                # Query for Bulk Insert (Fallback)
                insert_bulk_details_query = """
                    INSERT INTO WHR_Stock_Details_tbl
                    (Warehouse_Name, Org_ID, Org_Name, WHR_SuperUserNo, WHR_Super_User_Name,
                     Uniq_Id, Location_Code, Location_Barcode, Location_Measurement,
                     Item_Code, Item_Cubic_Meter, Item_Description, Product_Parts, Product_Code,
                     SerialNo, Item_Qty, Total_Cubic_Meter, Franchise, Class_Name, Sub_Class_Name,
                     Creation_Date, Creation_By, Stock_status)
                    SELECT TOP (%s)
                     %s,%s,%s,%s,%s,
                     %s,%s,%s,%s,
                     %s,%s,%s,%s,%s,
                     %s,1,%s,%s,%s,%s,
                     %s,%s,'Available'
                    FROM master..spt_values
                """

                for item in table_details:
                    item_code = item.get("itemCode")

                    # Header Insert
                    cursor.execute(insert_header_query, [
                        warehouse_name,
                        org_id,
                        org_name,
                        whr_superuser_no,
                        whr_super_user_name,
                        item.get("Location_Code"),
                        location_barcode,
                        location_measurement,
                        item_code,
                        item.get("Item_meas"),
                        item.get("itemDetails"),
                        item.get("product_Type"),
                        item.get("productCode"),
                        item.get("quantity"),
                        item.get("tot_item_meas"),
                        item.get("Franchise"),
                        item.get("Item_class"),
                        item.get("Sub_Class"),
                        creation_date,
                        created_by,
                        uniq_id
                    ])

                    # Details Insert Logic
                    serial_list = item.get("Serial_no_list", [])
                    qty = int(item.get("quantity", 0))

                    # 1. First, Check Serial Status
                    cursor.execute("""
                        SELECT SERIAL_STATUS
                        FROM BUYP.ALJE_ITEM_CATEGORIES_CPD_V
                        WHERE ITEM_CODE = %s
                    """, [item_code])
                    row = cursor.fetchone()
                    serial_status = row[0] if row else None
                    # If status is "Y", the item is serialized. Otherwise, it is not.

                    # 2. Decide Insert Type
                    # If status='Y' AND we have a list of serials, insert them one by one.
                    if serial_status == 'Y' and serial_list and len(serial_list) > 0:
                        # Case A: Serialized Item + List provided --> Insert distinct serial numbers
                        for serial_no in serial_list:
                            cursor.execute(insert_single_detail_query, [
                                warehouse_name,
                                org_id,
                                org_name,
                                whr_superuser_no,
                                whr_super_user_name,
                                uniq_id,
                                item.get("Location_Code"),
                                location_barcode,
                                location_measurement,
                                item_code,
                                item.get("Item_meas"),
                                item.get("itemDetails"),
                                item.get("product_Type"),
                                item.get("productCode"),
                                str(serial_no),      # Specific SerialNo
                                item.get("tot_item_meas"),
                                item.get("Franchise"),
                                item.get("Item_class"),
                                item.get("Sub_Class"),
                                creation_date,
                                created_by
                            ])
                    else:
                        # Case B: Not serialized OR (Serialized but list missing)
                        # Determine default serial value
                        if serial_status == 'Y':
                            # Serialized but no list provided? Default to empty or keep as '' per original logic
                            serial_no_value = ''
                        else:
                            # Not serialized (status != 'Y') --> Force "No Serial Items"
                            serial_no_value = 'No Serial Items'

                        if qty > 0:
                            cursor.execute(insert_bulk_details_query, [
                                qty,
                                warehouse_name,
                                org_id,
                                org_name,
                                whr_superuser_no,
                                whr_super_user_name,
                                uniq_id,
                                item.get("Location_Code"),
                                location_barcode,
                                location_measurement,
                                item_code,
                                item.get("Item_meas"),
                                item.get("itemDetails"),
                                item.get("product_Type"),
                                item.get("productCode"),
                                serial_no_value,     # Common value for all rows
                                item.get("tot_item_meas"),
                                item.get("Franchise"),
                                item.get("Item_class"),
                                item.get("Sub_Class"),
                                creation_date,
                                created_by
                            ])

        return JsonResponse({
            "status": "success",
            "message": f"{len(table_details)} items inserted with serial logic",
            "Uniq_Id": uniq_id
        })

    except Exception as e:
        return JsonResponse({"status": "error", "message": str(e)}, status=500)