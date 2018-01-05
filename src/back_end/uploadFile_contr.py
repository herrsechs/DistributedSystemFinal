from file_trans.models import DataFile
import os
import datetime


def get_file(filename):
    #f = UploadFile(filename=filename)
    #return f
    pass


def fbuffer(f, chunk_size=10000):
    while True:
        chunk = f.read(chunk_size)
        if not chunk: break
        yield chunk


def upload_one_file(request):
    fi = request.FILES.get('file')
    if fi.name:
        #oldName = fi.name
        #fileName = oldName.split('.')[0]+datetime.datetime.now().strftime("%Y-%m-%d")+'.'+oldName.split('.')[1]
        dir_path = 'data_files'
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
        print(dir_path)
        file_path = os.path.join(dir_path, fi.name)

        chunk_idx = 0
        for chunk in fbuffer(fi):
            with open(file_path + str(chunk_idx), 'wb', 10000) as f:
                f.write(chunk)
            chunk_idx += 1
        uf = DataFile(filename=fi.name,
                      time_stamp=datetime.datetime.now().strftime("%Y-%m-%d"),
                      chunk_size=chunk_idx)
        try:
            uf.save()
        except IOError:
            print('Fail to save upload_file info into database!')
        message = 'The upload_file "%s" was uploaded successfully' % fi.name

    else:
        message = 'No upload_file was uploaded'

    return message
