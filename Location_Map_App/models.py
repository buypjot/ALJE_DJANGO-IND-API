
from django.db import IntegrityError, transaction, connection

import json
from sqlite3 import IntegrityError
from django.db import models
from django.utils import timezone

from django.db import models

# Create your models here.



class WHR_MNG_UNIQID_Models(models.Model):
    Uniq_Id = models.CharField(max_length=20, unique=True)
    Tocken = models.PositiveIntegerField(default=0)

    class Meta:
        db_table = "WHR_MNG_UNIQID_tbl"
        ordering = ["id"]

    @staticmethod
    def generate_unique_id():
        today = timezone.now()
        year = today.strftime("%y")   # last 2 digits of year
        month = today.strftime("%m")  # 2-digit month

        prefix = f"SMT{year}{month}"

        # Only this month's records
        last_record = (
            WHR_MNG_UNIQID_Models.objects
            .filter(Uniq_Id__startswith=prefix)
            .select_for_update(skip_locked=True)
            .order_by("-Tocken")
            .first()
        )

        if last_record:
            next_tocken = last_record.Tocken + 1
        else:
            next_tocken = 1

        tocken_str = str(next_tocken).zfill(2)
        uniq_id = f"{prefix}{tocken_str}"
        return uniq_id, next_tocken

    def save(self, *args, **kwargs):
        if not self.Uniq_Id:
            for _ in range(600):
                try:
                    with transaction.atomic():
                        self.Uniq_Id, self.Tocken = self.generate_unique_id()
                        super().save(*args, **kwargs)
                        return
                except IntegrityError:
                    continue
            raise IntegrityError("Failed to generate unique ID after retries.")
        else:
            super().save(*args, **kwargs)

    def __str__(self):
        return self.Uniq_Id



class IDP_UniqId_tbl(models.Model):
    IDP = models.CharField(max_length=20, unique=True)
 
    def __str__(self):
        return self.IDP
 
    class Meta:
        db_table = "IDP_UniqId_tbl"
        ordering = ["id"]


class TIP_UniqId_tbl(models.Model):
    TIP = models.CharField(max_length=20, unique=True)
 
    def __str__(self):
        return self.TIP
 
    class Meta:
        db_table = "TIP_UniqId_tbl"
        ordering = ["id"]

