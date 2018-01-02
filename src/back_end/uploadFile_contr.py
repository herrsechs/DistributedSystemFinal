from src.models.uploadFile_model import uploadFile
import os
import datetime
def get_file(filename):
    file=uploadFile(filename)
    return file

def fbuffer(f, chunk_size=10000):

    while True:
        chunk = f.read(chunk_size)
        if not chunk: break
        yield chunk

def upload_one_file(request):
    file=request.FILES.get('file')
    if file.name:
        oldName=file.name
        fileName=oldName.split('.')[0]+datetime.datetime.now().strftime("%Y-%m-%d")+'.'+oldName.split('.')[1]
        dir_path = 'data_files'
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
        print(dir_path)
        file_path=os.path.join(dir_path, fileName)
        f = open(file_path, 'wb', 10000)
        for chunk in fbuffer(file):
            f.write(chunk)
            message = 'The file "%s" was uploaded successfully' % oldName
        f.close()
    else:
        message = 'No file was uploaded'

    return message






