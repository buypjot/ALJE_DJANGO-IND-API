import json
from django.db import models


class REQNO_Models(models.Model):
    REQ_ID=  models.CharField(max_length=200, default="",unique=True)
    TOCKEN=  models.CharField(max_length=200, default="")
    class Meta:
        db_table = "WHR_REQID_tbl"
        ordering = ["id"] 

class PICKID_Models(models.Model):
    PICK_ID=  models.CharField(max_length=200, default="",unique=True)
    TOCKEN=  models.CharField(max_length=200, default="")
    class Meta:
        db_table = "WHR_PICKID_tbl"
        ordering = ["id"] 

class QUICK_BILL_ID_Models(models.Model):
    QUICK_BILL_ID=  models.CharField(max_length=200, default="",unique=True)
    TOCKEN=  models.CharField(max_length=200, default="")
    class Meta:
        db_table = "WHR_QUICK_BILL_ID_TBL"
        ordering = ["id"] 

# class DELIVERYID_Models(models.Model):
#     DELIVERY_ID=  models.CharField(max_length=200, default="",unique=True)
#     TOCKEN=  models.CharField(max_length=200, default="")
#     class Meta:
#         db_table = "WHR_DELIVERYID_tbl"
#         ordering = ["id"] 


from django.db import models, transaction, IntegrityError
from django.utils import timezone
 
 
# class DELIVERYID_Models(models.Model):
#     DELIVERY_ID = models.CharField(max_length=20, unique=True)
#     TOCKEN = models.PositiveIntegerField(default=0)
 
#     class Meta:
#         db_table = "WHR_UNIQUE_DELIVERYID_tbl"   # Custom table name in MSSQL
#         ordering = ["id"]          # Default ordering by id
 
#     @staticmethod
#     def generate_code():
#         today = timezone.now()
#         year = today.strftime("%y")   # Last 2 digits of year
#         month = today.strftime("%m")  # Month
 
#         # Lock the last row to avoid race conditions
#         last_record = (
#             DELIVERYID_Models.objects
#             .select_for_update(skip_locked=True)   # row-level lock
#             .order_by("-TOCKEN")
#             .first()
#         )
 
#         if last_record:
#             next_counter = last_record.TOCKEN + 1
#         else:
#             next_counter = 1
 
#         counter_str = str(next_counter).zfill(2)  # 01, 02, 03...
#         return f"DL{year}{month}{counter_str}", next_counter
 
#     def save(self, *args, **kwargs):
#         if not self.DELIVERY_ID:
#             for _ in range(200):  # retry a few times if duplicate happens
#                 try:
#                     with transaction.atomic():
#                         self.DELIVERY_ID, self.TOCKEN = self.generate_code()
#                         super().save(*args, **kwargs)
#                         return
#                 except IntegrityError:
#                     # If duplicate happened, try again with next counter
#                     continue
#             raise IntegrityError("Failed to generate unique LicenseCode after retries.")
#         else:
#             super().save(*args, **kwargs)
 
#     def __str__(self):
#         return self.DELIVERY_ID





from django.db import models, transaction, IntegrityError
from django.utils import timezone


class DELIVERYID_Models(models.Model):
    DELIVERY_ID = models.CharField(max_length=20, unique=True)
    TOCKEN = models.PositiveIntegerField(default=0)

    class Meta:
        db_table = "WHR_UNIQUE_DELIVERYID_tbl"
        ordering = ["id"]

    @staticmethod
    def generate_code():
        today = timezone.now()
        year = today.strftime("%y")   # Last 2 digits of year
        month = today.strftime("%m")  # Month

        prefix = f"DL{year}{month}"

        # Only check current month's records
        last_record = (
            DELIVERYID_Models.objects
            .filter(DELIVERY_ID__startswith=prefix)   # filter by current month prefix
            .select_for_update(skip_locked=True)
            .order_by("-TOCKEN")
            .first()
        )

        if last_record:
            next_counter = last_record.TOCKEN + 1
        else:
            next_counter = 1

        counter_str = str(next_counter).zfill(2)  # 01, 02, ...
        return f"{prefix}{counter_str}", next_counter

    def save(self, *args, **kwargs):
        if not self.DELIVERY_ID:
            for _ in range(600):
                try:
                    with transaction.atomic():
                        self.DELIVERY_ID, self.TOCKEN = self.generate_code()
                        super().save(*args, **kwargs)
                        return
                except IntegrityError:
                    continue
            raise IntegrityError("Failed to generate unique LicenseCode after retries.")
        else:
            super().save(*args, **kwargs)

    def __str__(self):
        return self.DELIVERY_ID
    
class RETURNID_Models(models.Model):
    RETURN_ID=  models.CharField(max_length=200, default="",unique=True)
    TOCKEN=  models.CharField(max_length=200, default="")
    class Meta:
        db_table = "WHR_RETURNID_tbl"
        ordering = ["id"] 


# class ShipmentID_Models(models.Model):
#     Shipment_Id=  models.CharField(max_length=200, default="",unique=True)
#     Tocken=  models.CharField(max_length=200, default="")
#     class Meta:
#         db_table = "WHR_SHIPMENT_ID"
#         ordering = ["id"] 


 
from django.db import models, transaction, IntegrityError
from django.utils import timezone


class ShipmentID_Models(models.Model):
    shipment_id = models.CharField(max_length=20, unique=True)
    tocken = models.PositiveIntegerField(default=0)

    class Meta:
        db_table = "WHR_UNIQUE_SHIPMENT_ID"
        ordering = ["id"]

    @staticmethod
    def generate_shipment_id():
        today = timezone.now()
        year = today.strftime("%y")   # last 2 digits of year
        month = today.strftime("%m")  # 2-digit month

        prefix = f"INO{year}{month}"

        # Only check current month's records
        last_record = (
            ShipmentID_Models.objects
            .filter(shipment_id__startswith=prefix)   # filter by this month's prefix
            .select_for_update(skip_locked=True)
            .order_by("-tocken")
            .first()
        )

        if last_record:
            next_tocken = last_record.tocken + 1
        else:
            next_tocken = 1

        tocken_str = str(next_tocken).zfill(2)  # 01, 02, 03...
        shipment_code = f"{prefix}{tocken_str}"
        return shipment_code, next_tocken

    def save(self, *args, **kwargs):
        if not self.shipment_id:
            for _ in range(600):  # retry loop
                try:
                    with transaction.atomic():
                        self.shipment_id, self.tocken = self.generate_shipment_id()
                        super().save(*args, **kwargs)
                        return
                except IntegrityError:
                    continue
            raise IntegrityError("Failed to generate unique Shipment ID after retries.")
        else:
            super().save(*args, **kwargs)

    def __str__(self):
        return self.shipment_id



class INVOICE_RETURN_ID_Models(models.Model):
    INVOICE_RETURN_ID=  models.CharField(max_length=200, default="",unique=True)
    TOCKEN=  models.CharField(max_length=200, default="")
    class Meta:
        db_table = "INVOICE_REUTRN_ID_TBL"
        ordering = ["id"] 


 
class User_member_detailsModels(models.Model):
    PHYSICAL_WAREHOUSE=  models.CharField(max_length=200, default="")
    ORG_ID=  models.CharField(max_length=200, default="")
    ORG_NAME=  models.CharField(max_length=200, default="")
    EMPLOYEE_ID=  models.CharField(max_length=200, default="")
    EMP_NAME=  models.CharField(max_length=200, default="")
    EMP_MAIL=  models.CharField(max_length=200, default="")
    EMP_ROLE=  models.CharField(max_length=200, default="")
    EMP_USERNAME=  models.CharField(max_length=200, default="")
    EMP_PASSWORD = models.CharField(max_length=255)
    CREATION_DATE=  models.DateTimeField(max_length=200, default="")
    CREATED_BY=  models.CharField(max_length=200, default="")
    CREATED_IP=  models.CharField(max_length=200, default="")
    CREATED_MAC=  models.CharField(max_length=200, default="")
    LAST_UPDATE_DATE=  models.DateTimeField(max_length=200, default="")
    LAST_UPDATED_BY=  models.CharField(max_length=200, default="")
    LAST_UPDATE_IP=  models.CharField(max_length=200, default="")
    FLAG=  models.CharField(max_length=200, default="")
    EMP_ACCESS_CONTROL=  models.CharField(max_length=200, default="")
   
    class Meta:
        db_table = "WHR_USER_MANAGEMENT"
        ordering = ["id"]
    @property
    def decrypted_password(self):
        # if stored encrypted, decrypt here; else return plain
        return self.EMP_PASSWORD
 
class Physical_WarehouseModels(models.Model):
    ORGANIZATION_ID=  models.CharField(max_length=200, default="")
    REGION_NAME=  models.CharField(max_length=200, default="")
    WAREHOUSE_NAME=  models.CharField(max_length=200, default="")
 
    class Meta:
        db_table = "ALJE_PHYSICAL_WHR"
        ordering = ["id"] 



class Salesman_List(models.Model):

    SALESREP_ID=  models.CharField(max_length=200, default="")
    SALESMAN_NO=  models.CharField(max_length=200, default="")
    SALESMAN_NAME=  models.CharField(max_length=200, default="")

    class Meta:
        db_table = "WHR_SALESMAN_LIST"
        ordering = ["id"] 


class WHRCreateDispatch(models.Model):
    REQ_ID= models.CharField(max_length=255 , default="")
    PHYSICAL_WAREHOUSE= models.CharField(max_length=255 , default="")
    ORG_ID= models.CharField(max_length=255 , default="")
    ORG_NAME= models.CharField(max_length=255 , default="")
    COMMERCIAL_NO= models.CharField(max_length=255 , default="")
    COMMERCIAL_NAME= models.CharField(max_length=255 , default="")
    SALESMAN_NO= models.CharField(max_length=255 , default="")
    SALESMAN_NAME= models.CharField(max_length=255 , default="")
    CUSTOMER_NUMBER= models.CharField(max_length=255 , default="")
    CUSTOMER_NAME= models.CharField(max_length=255 , default="")
    CUSTOMER_SITE_ID= models.CharField(max_length=255 , default="")
    INVOICE_DATE= models.CharField(max_length=255 , default="")
    INVOICE_NUMBER= models.CharField(max_length=255 , default="")
    CUSTOMER_TRX_ID= models.CharField(max_length=255 , default="")
    CUSTOMER_TRX_LINE_ID= models.CharField(max_length=255 , default="")
    LINE_NUMBER= models.CharField(max_length=255 , default="")
    INVENTORY_ITEM_ID= models.CharField(max_length=255 , default="")
    ITEM_DESCRIPTION= models.CharField(max_length=255 , default="")
    TOT_QUANTITY= models.CharField(max_length=255 , default="")
    DISPATCHED_QTY= models.CharField(max_length=255 , default="")
    BALANCE_QTY= models.CharField(max_length=255 , default="")
    DISPATCHED_BY_MANAGER= models.CharField(max_length=255 , default="")
    TRUCK_SCAN_QTY= models.CharField(max_length=255 , default="")
    CREATION_DATE= models.DateTimeField(max_length=255 , default="")
    CREATED_BY= models.CharField(max_length=255 , default="")
    CREATED_IP= models.CharField(max_length=255 , default="")
    CREATED_MAC= models.CharField(max_length=255 , default="")
    LAST_UPDATE_DATE= models.DateTimeField(max_length=255 , default="")
    LAST_UPDATED_BY= models.CharField(max_length=255 , default="")
    LAST_UPDATE_IP= models.CharField(max_length=255 , default="")
    FLAG= models.CharField(max_length=255 , default="")
    DELIVERYADDRESS = models.CharField(max_length=255 , default="")
    REMARKS= models.CharField(max_length=255 , default="")
    DELIVERY_DATE= models.DateTimeField(max_length=255 , default="")
    UNDEL_ID = models.CharField(max_length=255 , default="")

    class Meta:
        db_table = 'WHR_CREATE_DISPATCH'  # Matches the database table name
        # ordering = ["id"]

class WHRDispatchRequest(models.Model):
    PICK_ID= models.CharField(max_length=255 , default="")
    REQ_ID= models.CharField(max_length=255 , default="")
    DATE= models.CharField(max_length=255 , default="")
    ASSIGN_PICKMAN= models.CharField(max_length=255 , default="")
    PHYSICAL_WAREHOUSE= models.CharField(max_length=255 , default="")
    ORG_ID= models.CharField(max_length=255 , default="")
    ORG_NAME= models.CharField(max_length=255 , default="")
    SALESMAN_NO= models.CharField(max_length=255 , default="")
    SALESMAN_NAME= models.CharField(max_length=255 , default="")
    MANAGER_NO= models.CharField(max_length=255 , default="")
    MANAGER_NAME= models.CharField(max_length=255 , default="")
    CUSTOMER_NUMBER= models.CharField(max_length=255 , default="")
    CUSTOMER_NAME= models.CharField(max_length=255 , default="")
    CUSTOMER_SITE_ID= models.CharField(max_length=255 , default="")
    INVOICE_DATE= models.DateTimeField(max_length=255 , default="")
    INVOICE_NUMBER= models.CharField(max_length=255 , default="")
    
    CUSTOMER_TRX_ID= models.CharField(max_length=255 , default="")
    CUSTOMER_TRX_LINE_ID= models.CharField(max_length=255 , default="")
    LINE_NUMBER= models.CharField(max_length=255 , default="")
    INVENTORY_ITEM_ID= models.CharField(max_length=255 , default="")
    ITEM_DESCRIPTION= models.CharField(max_length=255 , default="")
    TOT_QUANTITY= models.CharField(max_length=255 , default="")
    DISPATCHED_QTY= models.CharField(max_length=255 , default="")
    BALANCE_QTY= models.CharField(max_length=255 , default="")
    PICKED_QTY= models.CharField(max_length=255 , default="")
    SCANNED_QTY= models.CharField(max_length=255 , default="")
    STATUS= models.CharField(max_length=255 , default="")
    CREATION_DATE= models.DateTimeField(max_length=255 , default="")
    CREATED_BY= models.CharField(max_length=255 , default="")
    CREATED_IP= models.CharField(max_length=255 , default="")
    CREATED_MAC= models.CharField(max_length=255 , default="")
    LAST_UPDATE_DATE= models.DateTimeField(max_length=255 , default="")
    LAST_UPDATED_BY= models.CharField(max_length=255 , default="")
    LAST_UPDATE_IP= models.CharField(max_length=255 , default="")
    FLAG= models.CharField(max_length=255 , default="")
    UNDEL_ID = models.CharField(max_length=255 , default="")


    class Meta:
        db_table = 'WHR_DISPATCH_REQUEST'  # Matches the database table name
        ordering = ["id"] 


class Pickman_ScanModels(models.Model):    
    PICK_ID=  models.CharField(max_length=200, default="")
    REQ_ID=  models.CharField(max_length=200, default="")
    DATE=  models.DateTimeField(max_length=200, default="")
    ASSIGN_PICKMAN=  models.CharField(max_length=200, default="")
    PHYSICAL_WAREHOUSE=  models.CharField(max_length=200, default="")
    ORG_ID=  models.CharField(max_length=200, default="")
    ORG_NAME=  models.CharField(max_length=200, default="")
    SALESMAN_NO=  models.CharField(max_length=200, default="")
    SALESMAN_NAME=  models.CharField(max_length=200, default="")
    MANAGER_NO=  models.CharField(max_length=200, default="")
    MANAGER_NAME=  models.CharField(max_length=200, default="")
    PICKMAN_NO=  models.CharField(max_length=200, default="")
    PICKMAN_NAME=  models.CharField(max_length=200, default="")
    CUSTOMER_NUMBER=  models.CharField(max_length=200, default="")
    CUSTOMER_NAME=  models.CharField(max_length=200, default="")
    CUSTOMER_SITE_ID=  models.CharField(max_length=200, default="")
    INVOICE_DATE=  models.DateTimeField(max_length=200, default="")
    INVOICE_NUMBER=  models.CharField(max_length=200, default="")
     
    CUSTOMER_TRX_ID= models.CharField(max_length=255 , default="")
    CUSTOMER_TRX_LINE_ID= models.CharField(max_length=255 , default="")
    LINE_NUMBER=  models.CharField(max_length=200, default="")
    INVENTORY_ITEM_ID=  models.CharField(max_length=200, default="")
    ITEM_DESCRIPTION=  models.CharField(max_length=200, default="")
    TOT_QUANTITY=  models.CharField(max_length=200, default="")
    DISPATCHED_QTY=  models.CharField(max_length=200, default="")
    BALANCE_QTY=  models.CharField(max_length=200, default="")
    PICKED_QTY=  models.CharField(max_length=200, default="")
    PRODUCT_CODE=  models.CharField(max_length=200, default="")
    SERIAL_NO=  models.CharField(max_length=200, default="")
    CREATION_DATE=  models.DateTimeField(max_length=200, default="")
    CREATED_BY=  models.CharField(max_length=200, default="")
    CREATED_IP=  models.CharField(max_length=200, default="")
    CREATED_MAC=  models.CharField(max_length=200, default="")
    LAST_UPDATE_DATE=  models.DateTimeField(max_length=200, default="")
    LAST_UPDATED_BY=  models.CharField(max_length=200, default="")
    LAST_UPDATE_IP=  models.CharField(max_length=200, default="")
    FLAG=  models.CharField(max_length=200, default="")
    UNDEL_ID = models.CharField(max_length=255 , default="")

    class Meta:
        db_table = "WHR_PICKED_MAN"
        # ordering = ["id"] 


class Truck_scanModels(models.Model):   

    DISPATCH_ID=  models.CharField(max_length=200, default="")
    REQ_ID=  models.CharField(max_length=200, default="")
    PICK_ID=  models.CharField(max_length=200, default="")
    DATE=  models.CharField(max_length=200, default="")
    PHYSICAL_WAREHOUSE=  models.CharField(max_length=200, default="")
    ORG_ID=  models.CharField(max_length=200, default="")
    ORG_NAME=  models.CharField(max_length=200, default="")
    SALESMAN_NO=  models.CharField(max_length=200, default="")
    SALESMAN_NAME=  models.CharField(max_length=200, default="")
    MANAGER_NO=  models.CharField(max_length=200, default="")
    MANAGER_NAME=  models.CharField(max_length=200, default="")
    PICKMAN_NO=  models.CharField(max_length=200, default="")
    PICKMAN_NAME=  models.CharField(max_length=200, default="")
    STAFF_NO=  models.CharField(max_length=200, default="")
    STAFF_NAME=  models.CharField(max_length=200, default="")
    CUSTOMER_NUMBER=  models.CharField(max_length=200, default="")
    CUSTOMER_NAME=  models.CharField(max_length=200, default="")
    CUSTOMER_SITE_ID=  models.CharField(max_length=200, default="")
    TRANSPORTER_NAME=  models.CharField(max_length=200, default="")
    DRIVER_NAME=  models.CharField(max_length=200, default="")
    DRIVER_MOBILENO=  models.CharField(max_length=200, default="")
    VEHICLE_NO=  models.CharField(max_length=200, default="")
    TRUCK_DIMENSION=  models.CharField(max_length=200, default="")
    LOADING_CHARGES=  models.CharField(max_length=200, default="")
    TRANSPORT_CHARGES=  models.CharField(max_length=200, default="")
    MISC_CHARGES=  models.CharField(max_length=200, default="")
    DELIVERYADDRESS =  models.CharField(max_length=200, default="")
    SALESMANREMARKS =  models.CharField(max_length=200, default="")
    REMARKS=  models.CharField(max_length=200, default="")
    INVOICE_NO=  models.CharField(max_length=200, default="")
    CUSTOMER_TRX_ID= models.CharField(max_length=255 , default="")
    CUSTOMER_TRX_LINE_ID= models.CharField(max_length=255 , default="")
    LINE_NO= models.CharField(max_length=255 , default="")
    ITEM_CODE=  models.CharField(max_length=200, default="")
    ITEM_DETAILS=  models.CharField(max_length=200, default="")
    PRODUCT_CODE=  models.CharField(max_length=200, default="")
    SERIAL_NO=  models.CharField(max_length=200, default="")
    DISREQ_QTY=  models.CharField(max_length=200, default="")
    BALANCE_QTY=  models.CharField(max_length=200, default="")
    TRUCK_SEND_QTY=  models.CharField(max_length=200, default="")
    CREATION_DATE=  models.CharField(max_length=200, default="")
    CREATED_BY=  models.CharField(max_length=200, default="")
    CREATED_IP=  models.CharField(max_length=200, default="")
    CREATED_MAC=  models.CharField(max_length=200, default="")
    LAST_UPDATE_DATE=  models.CharField(max_length=200, default="")
    LAST_UPDATED_BY=  models.CharField(max_length=200, default="")
    LAST_UPDATE_IP=  models.CharField(max_length=200, default="")
    FLAG=  models.CharField(max_length=200, default="")
    DELIVERY_DATE=  models.DateTimeField(max_length=200, default="")
    UNDEL_ID = models.CharField(max_length=255 , default="")
    DELIVERY_STATUS = models.CharField(max_length=255 , default="")
    SCAN_PATH = models.CharField(max_length=255 , default="")
    class Meta:
        db_table = "WHR_TRUCK_SCAN_DETAILS"
        ordering = ["id"] 


class ToGetGenerateDispatch(models.Model):   

    dispatch_id=  models.CharField(max_length=200, default="")
    req_no=  models.CharField(max_length=200, default="")
    pick_id=  models.CharField(max_length=200, default="")
    salesman_no=  models.CharField(max_length=200, default="")
    salesman_name=  models.CharField(max_length=200, default="")
    manager_no=  models.CharField(max_length=200, default="")
    manager_name=  models.CharField(max_length=200, default="")
    pickman_no=  models.CharField(max_length=200, default="")
    pickman_name=  models.CharField(max_length=200, default="")
    loadman_no=  models.CharField(max_length=200, default="")
    loadman_name=  models.CharField(max_length=200, default="")
    invoice_no=  models.CharField(max_length=200, default="")
    Customer_no=  models.CharField(max_length=200, default="")
    Customer_name=  models.CharField(max_length=200, default="")
    Customer_Site=  models.CharField(max_length=200, default="")    
    Customer_trx_id=  models.CharField(max_length=200, default="")
    Customer_trx_line_id=  models.CharField(max_length=200, default="")
    line_no=  models.CharField(max_length=200, default="")    
    Item_code=  models.CharField(max_length=200, default="")
    Item_detailas=  models.CharField(max_length=200, default="")
    DisReq_Qty=  models.CharField(max_length=200, default="")
    Send_qty=  models.CharField(max_length=200, default="")
    Product_code=  models.CharField(max_length=200, default="")
    Serial_No=  models.CharField(max_length=200, default="")
    Udel_id= models.CharField(max_length=200, default="")
    SCAN_STATUS= models.CharField(max_length=200, default="")
    item_location_code= models.CharField(max_length=200, default="")
    item_location_barcode= models.CharField(max_length=200, default="")

    class Meta:
        db_table = "WHR_SAVE_TRUCK_DETAILS_TBL"
        ordering = ["id"] 

class ProductcodeGetModels(models.Model): 
    ORGANIZATION_ID=  models.CharField(max_length=200, default="",primary_key=True)
    ITEM_CODE=  models.CharField(max_length=200, default="")
    FRENCHISE_ID=  models.CharField(max_length=200, default="")
    DESCRIPTION=  models.CharField(max_length=200, default="")
    ENABLED_FLAG=  models.CharField(max_length=200, default="")
    FRANCHISE=  models.CharField(max_length=200, default="")
    FAMILY_ID=  models.CharField(max_length=200, default="")
    FAMILY=  models.CharField(max_length=200, default="")
    CLASS_ID=  models.CharField(max_length=200, default="")
    CLASS=  models.CharField(max_length=200, default="")
    SUBCLASS_ID=  models.CharField(max_length=200, default="")
    SUBCLASS=  models.CharField(max_length=200, default="")
    SASO_EXPIRY=  models.CharField(max_length=200, default="")


    GMARK_EXPIRY=  models.CharField(max_length=200, default="")
    SQM_EXPIRY=  models.CharField(max_length=200, default="")
    IECEE_EXPIRY=  models.CharField(max_length=200, default="")
    PROD_PART=  models.CharField(max_length=200, default="")
    CUSTOM_CAT_ID=  models.CharField(max_length=200, default="")
    CUSTOM_CATEGORY=  models.CharField(max_length=200, default="")

    

    BUYER_ID=  models.CharField(max_length=200, default="")
    BUYER_NAME=  models.CharField(max_length=200, default="")
    MIN_PRODUCT_MARGIN_PER=  models.CharField(max_length=200, default="")
    PSI_STATUS=  models.CharField(max_length=200, default="")
    PSI_LEAD_TIME=  models.CharField(max_length=200, default="")
    INVENTORY_ITEM_ID=  models.CharField(max_length=200, default="")

    SERIAL_STATUS=  models.CharField(max_length=200, default="")
    PRODUCT_BARCODE=  models.CharField(max_length=200, default="")

    class Meta:
        db_table = "[BUYP].[BUYP].[ALJE_ITEM_CATEGORIES_CPD_V]"

class LogReportsModels(models.Model): 

    datetime=  models.DateTimeField(max_length=200, default="")
    EmployeeId=  models.CharField(max_length=200, default="")
    EmployeeName=  models.CharField(max_length=200, default="")
    EmployeeRole=  models.CharField(max_length=200, default="")
    Org_id=  models.CharField(max_length=200, default="")
    WarehouseName=  models.CharField(max_length=200, default="")
    FormName=  models.CharField(max_length=200, default="")
    Action=  models.CharField(max_length=200, default="")

    class Meta:
        db_table = "WHR_LOG_REPORT"
        ordering = ["id"] 





class WHRTransactionDetail(models.Model):
    UNDEL_ID = models.CharField(max_length=255, default= "")
    TRANSACTION_DATE = models.DateTimeField(max_length=255, default= "")
    CUSTOMER_TRX_ID = models.CharField(max_length=255, default= "")
    CUSTOMER_TRX_LINE_ID = models.CharField(max_length=255, default= "")
    ITEM_ID = models.CharField(max_length=255, default= "")
    LINE_NO = models.CharField(max_length=255, default= "")
    QTY = models.CharField(max_length=255, default= "")
    SOURCE = models.CharField(max_length=255, default= "")
    TRANSACTION_TYPE = models.CharField(max_length=255, default= "")
    DISPATCH_ID = models.CharField(max_length=255, default= "")
    REFERENCE1 = models.CharField(max_length=255, default= "")
    REFERENCE2 = models.CharField(max_length=255, default= "")
    REFERENCE3 = models.CharField(max_length=255, default= "")
    REFERENCE4 = models.CharField(max_length=255,default= "")
    CREATION_DATE = models.DateTimeField(max_length=255,default= "")
    CREATED_BY = models.CharField(max_length=255, default= "")
    CREATED_IP = models.CharField(max_length=255, default= "")
    CREATED_MAC = models.CharField(max_length=255, default= "")
    LAST_UPDATE_DATE = models.DateTimeField(max_length=255, default= "")
    LAST_UPDATED_BY = models.CharField(max_length=255, default= "")
    LAST_UPDATE_IP = models.CharField(max_length=255, default= "")
    LAST_UPDATE_MAC = models.CharField(max_length=255, default= "")
    FLAG = models.CharField(max_length=255, default= "")


    class Meta:
        db_table = 'WHR_TRANSACTION_DETAILS'  # Matches the table name
        ordering = ["id"]



class WHRReturnDispatch(models.Model):
    
    RETURN_DIS_ID = models.CharField(max_length=255, default= "")
    DISPATCH_ID = models.CharField(max_length=255, default= "")
    REQ_ID = models.CharField(max_length=255, default= "")
    PICK_ID = models.CharField(max_length=255, default= "")
    DATE = models.DateTimeField(max_length=255, default= "")
    PHYSICAL_WAREHOUSE = models.CharField(max_length=255, default= "")
    ORG_ID = models.CharField(max_length=255, default= "")
    ORG_NAME = models.CharField(max_length=255, default= "")
    SALESMAN_NO= models.CharField(max_length=255, default= "")
    SALESMAN_NAME= models.CharField(max_length=255, default= "")
    MANAGER_NO = models.CharField(max_length=255, default= "")
    MANAGER_NAME = models.CharField(max_length=255, default= "")
    CUSTOMER_NUMBER = models.CharField(max_length=255, default= "")
    CUSTOMER_NAME = models.CharField(max_length=255, default= "")
    CUSTOMER_SITE_ID = models.CharField(max_length=255, default= "")
    CUSTOMER_TRX_ID = models.CharField(max_length=255, default= "")
    CUSTOMER_TRX_LINE_ID = models.CharField(max_length=255, default= "")
    LINE_NUMBER = models.CharField(max_length=255, default= "")
    TRANSPORTER_NAME = models.CharField(max_length=255, default= "")
    DRIVER_NAME = models.CharField(max_length=255, default= "")
    DRIVER_MOBILENO = models.CharField(max_length=255, default= "")
    VEHICLE_NO = models.CharField(max_length=255, default= "")
    TRUCK_DIMENSION = models.CharField(max_length=255, default= "")
    LOADING_CHARGES = models.CharField(max_length=255, default= "")
    TRANSPORT_CHARGES = models.CharField(max_length=255, default= "")
    MISC_CHARGES = models.CharField(max_length=255, default= "")
    REMARKS = models.CharField(max_length=255, default= "")
    INVOICE_NO = models.CharField(max_length=255, default= "")
    ITEM_CODE = models.CharField(max_length=255, default= "")
    ITEM_DETAILS = models.CharField(max_length=255, default= "")
    PRODUCT_CODE = models.CharField(max_length=255, default= "")
    SERIAL_NO = models.CharField(max_length=255, default= "")
    DISREQ_QTY = models.CharField(max_length=255, default= "")
    BALANCE_QTY = models.CharField(max_length=255, default= "")
    TRUCK_SEND_QTY = models.CharField(max_length=255, default= "")
    CREATION_DATE = models.DateTimeField(max_length=255, default= "")
    CREATED_BY = models.CharField(max_length=255, default= "")
    CREATED_IP = models.CharField(max_length=255, default= "")
    CREATED_MAC = models.CharField(max_length=255, default= "")
    LAST_UPDATE_DATE = models.DateTimeField(max_length=255, default= "")
    LAST_UPDATED_BY = models.CharField(max_length=255, default= "")
    LAST_UPDATE_IP = models.CharField(max_length=255, default= "")
    FLAG = models.CharField(max_length=255, default= "")
    UNDEL_ID = models.CharField(max_length=255 , default="")
    RETURN_REASON = models.CharField(max_length=255 , default="")
    RE_ASSIGN_STATUS = models.CharField(max_length=255 , default="")

    class Meta:
        db_table = 'WHR_RETURN_DISPATCH'


class DepartmentModel(models.Model):
   
    DEP_ID = models.CharField(max_length=255, default= "")
    DEP_NAME = models.CharField(max_length=255, default= "")
   
    class Meta:
        db_table = 'ORGANIZED_DEPARTMENT'
        ordering = ['id']
       
 
class DepRolesModel(models.Model):
   
    DEP_ID = models.CharField(max_length=255, default= "")
    DEP_ROLE_ID = models.CharField(max_length=255, default= "")
    DEP_ROLE_NAME = models.CharField(max_length=255, default= "")
 
    class Meta:
        db_table = 'DEPARTMENT_ROLE'


class DepRoleFormsModel(models.Model):
   
    DEP_ID = models.CharField(max_length=255, default= "")
    DEP_ROLE_ID = models.CharField(max_length=255, default= "")
    SUBMENU_ID = models.CharField(max_length=255, default= "")
    SUBMENU = models.CharField(max_length=255, default= "")
 
    class Meta:
        db_table = 'DEPARTMENT_ROLEWISE_SUBMENU'



class DepUserManagementModel(models.Model):
   
    DEP_ID = models.CharField(max_length=255, default= "")
    DEP_ROLE_ID = models.CharField(max_length=255, default= "")
    EMP_ID = models.CharField(max_length=255, default= "")
    SUBMENU = models.CharField(max_length=255, default= "")
    STATUS = models.CharField(max_length=255, default= "")
 
    class Meta:
        db_table = 'DEPARTMENT_USERMANAGEMENT'  



class ShimentDispatchModels(models.Model):
    shipment_id = models.CharField(max_length=255, blank=True, db_column='SHIPMENT_ID')
    
    warehouse_name = models.CharField(max_length=255, blank=True, db_column='WAREHOUSE_NAME')
    to_warehouse_name = models.CharField(max_length=255, blank=True, db_column='TO_WAREHOUSE_NAME')
    salesmanno = models.BigIntegerField(null=True, blank=True, db_column='SALESMANNO')
    salesmanname = models.CharField(max_length=255, null=True, blank=True, db_column='SALESMANAME')
    date = models.DateTimeField(null=True, blank=True, db_column='DATE')
    transporter_name = models.CharField(max_length=255, null=True, blank=True, db_column='TRANSPORTER_NAME')
    driver_name = models.CharField(max_length=255, null=True, blank=True, db_column='DRIVER_NAME')
    driver_mobileno = models.BigIntegerField(null=True, blank=True, db_column='DRIVER_MOBILENO')
    vehicle_no = models.CharField(max_length=255, null=True, blank=True, db_column='VEHICLE_NO')
    truck_dimension = models.CharField(max_length=255, null=True, blank=True, db_column='TRUCK_DIMENSION')
    loading_charges = models.BigIntegerField(null=True, blank=True, db_column='LOADING_CHARGES')
    transport_charges = models.BigIntegerField(null=True, blank=True, db_column='TRANSPORT_CHARGES')
    misc_charges = models.BigIntegerField(null=True, blank=True, db_column='MISC_CHARGES')
    deliveryaddress = models.TextField(null=True, blank=True, db_column='DELIVERYADDRESS')
    shipment_header_id = models.IntegerField(null=True, blank=True, db_column='SHIPMENT_HEADER_ID')
    shipment_line_id = models.IntegerField(null=True, blank=True, db_column='SHIPMENT_LINE_ID')
    line_num = models.IntegerField(null=True, blank=True, db_column='LINE_NUM')
    creation_date = models.DateField(null=True, blank=True, db_column='CREATION_DATE')
    created_by = models.IntegerField(null=True, blank=True, db_column='CREATED_BY')
    organization_id = models.IntegerField(null=True, blank=True, db_column='ORGANIZATION_ID')
    organization_code = models.CharField(max_length=255, null=True, blank=True, db_column='ORGANIZATION_CODE')
    organization_name = models.CharField(max_length=255, null=True, blank=True, db_column='ORGANIZATION_NAME')
    shipment_num = models.CharField(max_length=255, null=True, blank=True, db_column='SHIPMENT_NUM')
    receipt_num = models.CharField(max_length=255, null=True, blank=True, db_column='RECEIPT_NUM')
    shipped_date = models.DateField(null=True, blank=True, db_column='SHIPPED_DATE')
    to_orgn_id = models.IntegerField(null=True, blank=True, db_column='TO_ORGN_ID')
    to_orgn_code = models.CharField(max_length=255, null=True, blank=True, db_column='TO_ORGN_CODE')
    to_orgn_name = models.CharField(max_length=255, null=True, blank=True, db_column='TO_ORGN_NAME')
    quantity_shipped = models.IntegerField(null=True, blank=True, db_column='QUANTITY_SHIPPED')
    quantity_received = models.IntegerField(null=True, blank=True, db_column='QUANTITY_RECEIVED')
    unit_of_measure = models.CharField(max_length=255, null=True, blank=True, db_column='UNIT_OF_MEASURE')
    item_id = models.TextField(null=True, blank=True, db_column='ITEM_ID')
    description = models.TextField(null=True, blank=True, db_column='DESCRIPTION')
    franchise = models.CharField(max_length=255, null=True, blank=True, db_column='FRANCHISE')
    family = models.CharField(max_length=255, null=True, blank=True, db_column='FAMILY')
    class_field = models.CharField(max_length=255, null=True, blank=True, db_column='CLASS')
    subclass = models.CharField(max_length=255, null=True, blank=True, db_column='SUBCLASS')
    shipment_line_status_code = models.CharField(max_length=255, null=True, blank=True, db_column='SHIPMENT_LINE_STATUS_CODE')
    quantity_progress = models.IntegerField(null=True, blank=True, db_column='QUANTITY_PROGRESS')
    active_status = models.CharField(max_length=255, null=True, blank=True, db_column='ACTIVE_STATUS')
    remarks = models.CharField(max_length=255, null=True, blank=True, db_column='REMARKS')
    dispatchto = models.CharField(max_length=255, null=True, blank=True, db_column='DISPATCH_TO')

    customer_no = models.CharField(max_length=255, null=True, blank=True, db_column='CUSTOMER_NO')
    customer_name = models.CharField(max_length=255, null=True, blank=True, db_column='CUSTOMER_NAME')
    invoice_no = models.CharField(max_length=255, null=True, blank=True, db_column='INVOICE_NO')
    dlnumber = models.CharField(max_length=255, null=True, blank=True, db_column='DL_NUMBER')


    class Meta:
        db_table = 'WHR_INTER_ORG_DETIALS_TBL'
        ordering = ["id"]


# class DocNO_Models(models.Model):
#     DOC_NO=  models.CharField(max_length=200, default="",unique=True)
#     TOKEN=  models.CharField(max_length=200, default="")
#     class Meta:
#         db_table = "WHR_INBOUND_DOCNO"
#         ordering = ["id"]



class WMS_SoftwareVersionModels(models.Model):
    App_Name = models.CharField(max_length=200, default="")
    Current_Version = models.CharField(max_length=200, default="")
    MobileApp_Version = models.CharField(max_length=200, default="")
    Play_store_Warning = models.CharField(max_length=200, default="")
    Running_time = models.CharField(max_length=200, default="")
  
   
    class Meta:
        db_table = "WMS_VERSION_CONTROLE_TBL"
        ordering = ["id"] 



# Monitoring Models



# from django.db import models

# class RequestLog(models.Model):
#     timestamp = models.DateTimeField(auto_now_add=True)
#     ip = models.GenericIPAddressField()
#     path = models.CharField(max_length=500)
#     method = models.CharField(max_length=10)
#     status = models.IntegerField()
#     size_kb = models.DecimalField(max_digits=10, decimal_places=2)
#     duration_ms = models.DecimalField(max_digits=10, decimal_places=2)
#     error = models.TextField(blank=True, null=True)

#     class Meta:
#         managed = False
#         db_table = 'RequestLog'



class Uniq_ARG_tbl(models.Model):
    ARG = models.CharField(max_length=20, unique=True)
 
    def str(self):
        return self.ARG
 
    class Meta:
        db_table = "UNIQ_ARG_TBL"
        ordering = ["id"]



class PK_UniqId_tbl(models.Model):
    PK = models.CharField(max_length=20, unique=True)
 
    def str(self):
        return self.PK
 
    class Meta:
        db_table = "PK_UniqId_tbl"
        ordering = ["id"]