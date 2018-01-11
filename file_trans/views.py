# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.shortcuts import render
from django.shortcuts import render
from django.http import HttpResponse
from django.views.decorators.http import require_http_methods
from django.views.decorators.csrf import csrf_exempt

from src.back_end.uploadFile_contr import upload_one_file, delete_one_file, update_one_file, show
from src.back_end.downloadFile_contr import downloadFile
from models import DataFile

import os


# Create your views here.
def index(request, index):
    index = int(index)
    if index == 0:
        return render(request, 'src/view/index.html')
    if index == 1:
        return render(request, 'src/view/show_file.html')
    if index == 2:
        return render(request, "src/view/upload.html")
    if index == 3:
        file_list = [obj.filename for obj in DataFile.objects.all()]
        doc_list = []
        video_list = []
        music_list = []
        for f in file_list:
            suffix = f.split('.')[-1]
            if suffix in ['mp4', 'avi', 'mkv', 'mov', 'flv']:
                video_list.append(f)
            elif suffix in ['mp3', 'wma', 'wav']:
                music_list.append(f)
            else:
                doc_list.append(f)
        return render(request, 'src/view/download.html',
                      {'doc': doc_list, 'music': music_list, 'video': video_list})
    if index == 4:
        file_list = [obj.filename for obj in DataFile.objects.all()]
        return render(request, 'src/view/copy.html', {'data': file_list})
    if index == 5:
        return render(request, 'src/view/charts.html')


@csrf_exempt
@require_http_methods({'POST'})
def upload(request):
    message = upload_one_file(request)
    return render(request, "src/view/upload.html")


@csrf_exempt
@require_http_methods({'GET'})
def show_file(request):
    file_list = [obj.filename for obj in DataFile.objects.all()]
    return render(request, 'src/view/show_file.html', {'data': file_list})


@csrf_exempt
@require_http_methods({'GET'})
def download_file(request):
    filename = request.GET.get('filename')
    response = downloadFile(filename)

    return response


@csrf_exempt
@require_http_methods({'POST'})
def update_file(request):
    update_one_file(request)
    filelist = show()
    return render(request, 'src/view/show_file.html', {'data': filelist})


@csrf_exempt
@require_http_methods({'POST'})
def delete_file(request):
    filename = request.GET.get('filename')
    print(filename)
    delete_one_file(request)
    filelist = show()
    return render(request, 'src/view/show_file.html', {'data': filelist})


@csrf_exempt
@require_http_methods({'GET'})
def show_charts(request, index):
    index = int(index)
    if index == 0:
        return render(request, 'src/view/answer1.html')
    if index == 1:
        return render(request, 'src/view/answer2.html')
    if index == 2:
        return render(request, 'src/view/answer3.html')
