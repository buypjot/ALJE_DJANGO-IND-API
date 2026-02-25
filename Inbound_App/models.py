import json
from sqlite3 import IntegrityError
from django.db import models
from django.utils import timezone


class DocNO_Models(models.Model):
    DOC_NO=  models.CharField(max_length=200, default="",unique=True)
    TOKEN=  models.CharField(max_length=200, default="")
    class Meta:
        db_table = "WHR_INBOUND_DOCNO"
        ordering = ["id"]




class PICK_ID_Models(models.Model):
    PICK_ID=  models.CharField(max_length=200, default="",unique=True)
    TOKEN=  models.CharField(max_length=200, default="")
    class Meta:
        db_table = "WHR_INBOUND_PICK_ID"
        ordering = ["id"]

class BayanDocument(models.Model):
    document_name = models.CharField(max_length=255)
    file_path = models.TextField()
    upload_date = models.DateTimeField(auto_now_add=True)


    class Meta:
        db_table = "BayanDocument"   # 🔹 match your SQL table


class InboundContainerBayanDocument(models.Model):
    DOC_NO = models.CharField(max_length=50)
    BAYAN_NO = models.CharField(max_length=50)
    DOCUMENT_NAME = models.CharField(max_length=255)
    FILE_PATH = models.TextField()
    INITIATOR_NO = models.CharField(max_length=50)
    INITIATOR_NAME = models.CharField(max_length=100)
    CREATED_BY = models.CharField(max_length=100)
    CREATED_IP = models.CharField(max_length=50)
    UPLOAD_DATE = models.DateTimeField()

    class Meta:
        db_table = "InboundContainerDocument_tbl"   


class InboundPoDocument(models.Model):
    DOC_NO = models.CharField(max_length=50)
    DOCUMENT_NAME = models.CharField(max_length=255)
    PO_NUMBER = models.CharField(max_length=50, null=True, blank=True)  # Add this field
    FILE_PATH = models.TextField()
    INITIATOR_NO = models.CharField(max_length=50)
    INITIATOR_NAME = models.CharField(max_length=100)
    CREATED_BY = models.CharField(max_length=100)
    CREATED_IP = models.CharField(max_length=50)
    UPLOAD_DATE = models.DateTimeField()

    class Meta:
        db_table = "InboundPoDocument_tbl"   


# class WHR_MNG_UNIQID_Models(models.Model):
#     Uniq_Id = models.CharField(max_length=20, unique=True)
#     Tocken = models.PositiveIntegerField(default=0)

#     class Meta:
#         db_table = "WHR_MNG_UNIQID_tbl"
#         ordering = ["id"]

#     @staticmethod
#     def generate_unique_id():
#         today = timezone.now()
#         year = today.strftime("%y")   # last 2 digits of year
#         month = today.strftime("%m")  # 2-digit month

#         prefix = f"SMT{year}{month}"

#         # Only this month's records
#         last_record = (
#             WHR_MNG_UNIQID_Models.objects
#             .filter(Uniq_Id__startswith=prefix)
#             .select_for_update(skip_locked=True)
#             .order_by("-Tocken")
#             .first()
#         )

#         if last_record:
#             next_tocken = last_record.Tocken + 1
#         else:
#             next_tocken = 1

#         tocken_str = str(next_tocken).zfill(2)
#         uniq_id = f"{prefix}{tocken_str}"
#         return uniq_id, next_tocken

#     def save(self, *args, **kwargs):
#         if not self.Uniq_Id:
#             for _ in range(600):
#                 try:
#                     with transaction.atomic():
#                         self.Uniq_Id, self.Tocken = self.generate_unique_id()
#                         super().save(*args, **kwargs)
#                         return
#                 except IntegrityError:
#                     continue
#             raise IntegrityError("Failed to generate unique ID after retries.")
#         else:
#             super().save(*args, **kwargs)

#     def __str__(self):
#         return self.Uniq_Id


# Create your models here.
class Uniq_GatePass_tbl(models.Model):
    GatPass_No = models.CharField(max_length=20, unique=True)
 
    def __str__(self):
        return self.GatPass_No
 
    class Meta:
        db_table = "Uniq_GatePass_tbl"
        ordering = ["id"]