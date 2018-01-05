# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models


# Create your models here.
class DataFile(models.Model):
    fid = models.AutoField(primary_key=True)
    filename = models.CharField(max_length=100)
    chunk_size = models.IntegerField()
    time_stamp = models.CharField(max_length=20)
