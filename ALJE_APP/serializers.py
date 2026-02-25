from rest_framework import serializers

from .models import *

class REQNO_serializers(serializers.ModelSerializer):
    class Meta:
        model = REQNO_Models
        fields = "__all__"


class PICKID_serializers(serializers.ModelSerializer):
    class Meta:
        model = REQNO_Models
        fields = "__all__"

class REQNO_serializers(serializers.ModelSerializer):
    class Meta:
        model = REQNO_Models
        fields = "__all__"

class RETURNID_serializers(serializers.ModelSerializer):
    class Meta:
        model = RETURNID_Models
        fields = "__all__"
    
class User_member_detailserializers(serializers.ModelSerializer):
    class Meta:
        model = User_member_detailsModels
        fields = "__all__"

    def to_internal_value(self, data):
        internal = super().to_internal_value(data)

        # Handle Base64 to bytes conversion
        password = data.get('EMP_PASSWORD')
        if password and isinstance(password, str):
            try:
                internal['EMP_PASSWORD'] = base64.b64decode(password)
            except Exception:
                raise serializers.ValidationError({
                    "EMP_PASSWORD": "Invalid base64 password encoding."
                })

        return internal

    def to_representation(self, instance):
        rep = super().to_representation(instance)

        # Decode EMP_PASSWORD from bytes → str (for frontend)
        try:
            emp_password = instance.EMP_PASSWORD
            if isinstance(emp_password, bytes):
                decoded = emp_password.decode('utf-8', errors='ignore')
                cleaned = decoded.replace('\x00', '').replace('\u0000', '')
                rep['EMP_PASSWORD'] = cleaned
        except Exception:
            rep['EMP_PASSWORD'] = str(instance.EMP_PASSWORD)

        # Optional: decode EMP_ACCESS_CONTROL if needed
        access_control_str = instance.EMP_ACCESS_CONTROL
        access_control_dict = {}
        if access_control_str and access_control_str.strip().lower() != "unknown":
            access_control_str = access_control_str.strip('{}')
            for field in access_control_str.split(','):
                if ':' in field:
                    key, value = field.split(':', 1)
                    key = key.strip().strip("'\"")
                    value = value.strip().strip("'\"")
                    if value.lower() == 'true':
                        value = True
                    elif value.lower() == 'false':
                        value = False
                    access_control_dict[key] = value
        rep['acess_control'] = access_control_dict
        return rep


class Physical_Warehouseserializers(serializers.ModelSerializer):
    class Meta:
        model = Physical_WarehouseModels
        fields = "__all__"

class UndeliveredDataSerializer(serializers.Serializer):
    undel_id = serializers.IntegerField()
    to_warehouse = serializers.CharField(max_length=100)
    org_id = serializers.CharField(max_length=200)  # Change to CharField
    org_name = serializers.CharField(max_length=255)
    salesrep_id = serializers.CharField(max_length=200)  # Change to CharField
    salesman_no = serializers.CharField(max_length=50)
    salesman_name = serializers.CharField(max_length=255)
    customer_id = serializers.CharField(max_length=200)  # Change to CharField
    customer_number = serializers.CharField(max_length=50)
    customer_name = serializers.CharField(max_length=255)
    sales_channel = serializers.CharField(max_length=100)
    customer_site_id = serializers.CharField(max_length=200)  # Change to CharField
    cus_location = serializers.CharField(max_length=255)
    customer_trx_id = serializers.CharField(max_length=200)  # Change to CharField
    customer_trx_line_id = serializers.CharField(max_length=200)  # Change to CharField
    invoice_date = serializers.DateTimeField()
    invoice_number = serializers.CharField(max_length=50)
    line_number = serializers.IntegerField()
    inventory_item_id = serializers.CharField(max_length=200)  # Change to CharField
    quantity = serializers.DecimalField(max_digits=10, decimal_places=2)
    dispatch_qty = serializers.DecimalField(max_digits=10, decimal_places=2)
    amount = serializers.DecimalField(max_digits=10, decimal_places=2)
    item_cost = serializers.DecimalField(max_digits=10, decimal_places=2)
    flag = serializers.CharField(max_length=10)
    reference1 = serializers.CharField(max_length=255)
    reference2 = serializers.CharField(max_length=255)
    attribute1 = serializers.CharField(max_length=255)
    attribute2 = serializers.CharField(max_length=255)
    attribute3 = serializers.CharField(max_length=255)
    attribute4 = serializers.CharField(max_length=255)
    attribute5 = serializers.CharField(max_length=255)
    freeze_status = serializers.CharField(max_length=50)
    last_update_date = serializers.DateTimeField()
    last_updated_by = serializers.CharField(max_length=200)  # Change to CharField
    creation_date = serializers.DateTimeField()
    created_by = serializers.CharField(max_length=200)  # Change to CharField
    last_update_login = serializers.CharField(max_length=200)  # Change to CharField
    warehouse_id = serializers.CharField(max_length=200)  # Change to CharField
    warehouse_name = serializers.CharField(max_length=255)
    legacy_ref = serializers.CharField(max_length=255)
    inv_row_id = serializers.CharField(max_length=200)  # Change to CharField


class Salesman_Listserializers(serializers.ModelSerializer):
    class Meta:
        model = Salesman_List
        fields = "__all__"

class SalesmanDataSerializer(serializers.Serializer):
    salesrep_id = serializers.CharField(max_length=200)  # Change to CharField
    salesman_no = serializers.CharField(max_length=50)
    salesman_name = serializers.CharField(max_length=255)

class create_Dispatchserializers(serializers.ModelSerializer):
    class Meta:
        model = WHRCreateDispatch
        fields = "__all__"


class TableDetailSerializer(serializers.ModelSerializer):
    class Meta:
        model = WHRCreateDispatch
        fields = [
            "INVOICE_NUMBER",
            "LINE_NUMBER",
            "INVENTORY_ITEM_ID",
            "ITEM_DESCRIPTION",
            "TOT_QUANTITY",
            "DISPATCHED_QTY",
            "BALANCE_QTY",
        ]

class FilteredDispatchRequestSerializer(serializers.ModelSerializer):
    TABLE_DETAILS = serializers.ListField(child=TableDetailSerializer(), allow_empty=True)

    class Meta:
        model = WHRCreateDispatch
        fields = [
            "REQ_ID",
            "PHYSICAL_WAREHOUSE",
            "ORG_ID",
            "ORG_NAME",
            "SALESMAN_NO",
            "SALESMAN_NAME",
            "CUSTOMER_NUMBER",
            "CUSTOMER_NAME",
            "CUSTOMER_SITE_ID",
            "INVOICE_DATE",
            "TABLE_DETAILS"
        ]



class returndispatchTableDetailSerializer(serializers.ModelSerializer):
    class Meta:
        model = WHRCreateDispatch
        fields = [
            
            "UNDEL_ID",
            "INVOICE_NO",
            "LINE_NUMBER",
            "ITEM_CODE",
            "ITEM_DETAILS",
            "DISREQ_QTY",
            "TRUCK_SEND_QTY"
        ]

class FilteredReturnDispatchSerializer(serializers.ModelSerializer):
    TABLE_DETAILS = serializers.ListField(child=returndispatchTableDetailSerializer(), allow_empty=True)

    class Meta:
        model = WHRReturnDispatch
        fields = [
            "RETURN_DIS_ID",
            "DISPATCH_ID",
            "REQ_ID",
            "DATE",
            "PHYSICAL_WAREHOUSE",
            "ORG_ID",
            "ORG_NAME",
            "SALESMAN_NO",
            "SALESMAN_NAME",            
            "MANAGER_NO",
            "MANAGER_NAME",
            "CUSTOMER_NUMBER",
            "CUSTOMER_NAME",
            "CUSTOMER_SITE_ID",
            "TABLE_DETAILS"
        ]


class InterORGReportTableDetailSerializer(serializers.ModelSerializer):
    class Meta:
        model = ShimentDispatchModels
        fields = [
            
            "line_num",
            "item_id",
            "description",
            "quantity_shipped",
            "quantity_received",
            "quantity_progress"
        ]

class FilteredInterOrgReporterializer(serializers.ModelSerializer):
    TABLE_DETAILS = serializers.ListField(child=InterORGReportTableDetailSerializer(), allow_empty=True)

    class Meta:
        model = ShimentDispatchModels
        fields = [
            "shipment_id",
            "salesmanno",
            "salesmanname",
            "date",
            "transporter_name",
            "driver_name",
            "driver_mobileno",
            "vehicle_no",
            "truck_dimension",            
            "loading_charges",
            "transport_charges",
            "misc_charges",
            "deliveryaddress",
            "shipment_header_id",
            
            "shipment_line_id",
            "shipment_num",
            "receipt_num",
            "organization_id",
            "organization_name",
            "organization_code",
            "to_orgn_id",
            "to_orgn_code",
            "to_orgn_name",
            "remarks",
            "TABLE_DETAILS"
        ]


class loginsalesmanwarehousedetailsSerializer(serializers.Serializer):
    salesrep_id = serializers.CharField(max_length=200) 
    salesman_no = serializers.CharField(max_length=50)
    salesman_name = serializers.CharField(max_length=255)
    
    salesman_channel = serializers.CharField(max_length=255)
    to_warehouse = serializers.CharField()
    org_id = serializers.CharField()
    org_name = serializers.CharField()
    customer_id = serializers.IntegerField()
    customer_number = serializers.CharField()
    customer_name = serializers.CharField()
    customer_site_id = serializers.CharField(max_length=200)
    customer_site_channel  = serializers.CharField()
    
class SalesmanInvoicedetailsSerializer(serializers.Serializer):

    invoice_number = serializers.CharField(max_length=50)

class invoicedetailsSerializer(serializers.Serializer):
    
    undel_id = serializers.CharField(max_length=20)
    invoice_number = serializers.CharField(max_length=20)
    customer_trx_id = serializers.CharField(max_length=50)
    customer_trx_line_id = serializers.CharField(max_length=50)
    line_number = serializers.IntegerField()
    inventory_item_id = serializers.CharField(max_length=50) 
    quantity = serializers.FloatField()
    dispatched_qty = serializers.FloatField()
    # dispatch_qty = serializers.FloatField()
    # truck_send_qty = serializers.FloatField()
    # return_qty = serializers.FloatField()
    description = serializers.CharField()
    item_code = serializers.CharField() 

class Dispatch_requestserializers(serializers.ModelSerializer):
    class Meta:
        model = WHRDispatchRequest
        fields = "__all__"

class FilteredpickformSerializer(serializers.ModelSerializer):
    TABLE_DETAILS = serializers.ListField(child=TableDetailSerializer(), allow_empty=True)

    class Meta:
        model = WHRDispatchRequest
        fields = [
            "PICK_ID",
            "ASS_PICKMAN",
            "REQ_ID",
            "TO_WAREHOUSE",
            "ORG_ID",
            "ORG_NAME",
            "SALESREP_ID",
            "SALESMAN_NO",
            "SALESMAN_NAME",
            "SALES_CHANNEL",
            "CUSTOMER_ID",
            "CUSTOMER_NUMBER",
            "CUSTOMER_NAME",
            "CUSTOMER_SITE_ID",
            "INVOICE_DATE",
            "TABLE_DETAILS"
        ]


class Pickman_ScanModelsserializers(serializers.ModelSerializer):
    class Meta:
        model = Pickman_ScanModels
        fields = "__all__"



class Truck_scanserializers(serializers.ModelSerializer):
    class Meta:
        model = Truck_scanModels
        fields = "__all__"




class ToGetGenerateDispatchserializers(serializers.ModelSerializer):
    class Meta:
        model = ToGetGenerateDispatch
        fields = "__all__"





class ProductcodeGetserializers(serializers.ModelSerializer):
    PRODUCT_BARCODE = serializers.SerializerMethodField()
    SERIAL_STATUS = serializers.SerializerMethodField()   # ✅ THIS WAS MISSING

    class Meta:
        model = ProductcodeGetModels
        fields = "__all__"

    def get_PRODUCT_BARCODE(self, obj):
        # NULL / empty → "0"
        if obj.PRODUCT_BARCODE is None or str(obj.PRODUCT_BARCODE).strip() == "":
            return "0"
        return str(obj.PRODUCT_BARCODE)

    def get_SERIAL_STATUS(self, obj):
        # NULL / empty / 'null' → "N"
        if obj.SERIAL_STATUS is None or str(obj.SERIAL_STATUS).strip().lower() == "null":
            return "N"
        return str(obj.SERIAL_STATUS)


class LogReportsserializers(serializers.ModelSerializer):
    class Meta:
        model = LogReportsModels
        fields = "__all__"



class WHRTransactionDetailserializers(serializers.ModelSerializer):
    class Meta:
        model = WHRTransactionDetail
        fields = "__all__"



class Truck_scanTableDetailSerializer(serializers.ModelSerializer):
    class Meta:
        model = Truck_scanModels
        fields ="__all__"

class FilteredTruckDetailsSerializer(serializers.ModelSerializer):
    TABLE_DETAILS = Truck_scanTableDetailSerializer(many=True, read_only=True)

    class Meta:
        model = Truck_scanModels
        fields = [
            "DISPATCH_ID",
            "REQ_ID",
            "PHYSICAL_WAREHOUSE",
            "ORG_ID",
            "ORG_NAME",
            "SALESMAN_NO",
            "SALESMAN_NAME",
            "CUSTOMER_NUMBER",
            "CUSTOMER_NAME",
            "CUSTOMER_SITE_ID",
            "INVOICE_DATE",
            "TABLE_DETAILS"
        ]

class Return_dispatchSerializer(serializers.ModelSerializer):
    class Meta:
        model = WHRReturnDispatch
        fields ="__all__"

class DepartmentSerializer(serializers.ModelSerializer):
    class Meta:
        model = DepartmentModel
        fields ="__all__"
 
class DepRolesSerializer(serializers.ModelSerializer):
    class Meta:
        model = DepRolesModel
        fields ="__all__"
 
class DepRoleFormsSerializer(serializers.ModelSerializer):
    class Meta:
        model = DepRoleFormsModel
        fields ="__all__"


class DepUserManagementSerializer(serializers.ModelSerializer):
    class Meta:
        model = DepUserManagementModel
        fields ="__all__"




class ShipmentDispatchSerializer(serializers.ModelSerializer):
    class Meta:
        model = ShimentDispatchModels
        fields ="__all__"