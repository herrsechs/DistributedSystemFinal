from django.shortcuts import render
from django.http import HttpResponse
from django.views.decorators.http import require_http_methods
from django.views.decorators.csrf import  csrf_exempt

from src.back_end.uploadFile_contr import upload_one_file
from src.back_end.downloadFile_contr import downloadFile

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
        file_list = os.listdir('data_files')
        return render(request, 'src/view/panels.html', {'data': file_list})
    if index==4:
        file_list = os.listdir('data_files')
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
    file_list=os.listdir('data_files')
    filelist={}
    i=0

    for file in file_list:
        filelist[i]=file
        i+=1
    a=[1,2,3]
    return render(request,'src/view/widgets.html',{'data': file_list})

@csrf_exempt
@require_http_methods({'GET'})
def download_file(request):
    filename=request.GET.get('filename')
    response=downloadFile(filename)

    return response







