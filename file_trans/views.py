# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.shortcuts import render
from django.shortcuts import render
from django.http import HttpResponse
from django.views.decorators.http import require_http_methods
from django.views.decorators.csrf import  csrf_exempt

from src.back_end.uploadFile_contr import upload_one_file
from src.back_end.downloadFile_contr import downloadFile
from models import DataFile

import os

# Create your views here.
def index(request,index):
    index=int(index)
    if index==0:
        return render(request,'src/view/index.html')
    if index==1:
        return render(request,'src/view/widgets.html')
    if index==2:
        return render(request, "src/view/forms.html")
    if index==3:
        file_list = [obj.filename for obj in DataFile.objects.all()]
        return render(request, 'src/view/download.html', {'data': file_list})
    if index==4:
        file_list = [obj.filename for obj in DataFile.objects.all()]
        return render(request,'src/view/tables.html',{'data': file_list})
    if index==5:
        return render((request,'src/view/login.html'))


@csrf_exempt
@require_http_methods({'POST'})
def upload(request):

    message=upload_one_file(request)
    return render(request,"src/view/forms.html")


@csrf_exempt
@require_http_methods({'GET'})
def show_file(request):
    file_list = [obj.filename for obj in DataFile.objects.all()]
    return render(request,'src/view/widgets.html',{'data': file_list})

@csrf_exempt
@require_http_methods({'GET'})
def download_file(request):
    filename=request.GET.get('filename')
    response=downloadFile(filename)

    return response

