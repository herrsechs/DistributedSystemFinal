from file_trans.models import DataFile
import os
import datetime
from ip_mapping import map_chunk_to_slave, slave_ids, remote_dirs, get_ip, delete_chunk_from_slave


def get_file(filename):
    # f = UploadFile(filename=filename)
    # return f
    pass


def fbuffer(f, chunk_size=10000):
    while True:
        chunk = f.read(chunk_size)
        if not chunk: break
        yield chunk


def delete_one_file(request):
    filename = request.GET.get('filename')
    delete_chunk_from_slave(filename)
    DataFile.objects.filter(filename=filename).delete()


def show():
    filelist = [obj.filename for obj in DataFile.objects.all()]
    return filelist


def upload_one_file(request):
    local_ip = get_ip()
    fi = request.FILES.get('file')
    if fi.name:
        dir_path = remote_dirs[slave_ids.index(local_ip)]
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
        print(dir_path)
        file_path = os.path.join(dir_path, fi.name)

        chunk_idx = 0
        for chunk in fbuffer(fi):
            with open(file_path + str(chunk_idx), 'wb', 10000) as f:
                f.write(chunk)
            chunk_idx += 1

        # Save data file model
        uf = DataFile(filename=fi.name,
                      time_stamp=datetime.datetime.now().strftime("%Y-%m-%d"),
                      chunk_size=chunk_idx)
        try:
            uf.save()
        except IOError:
            print('Fail to save upload_file info into database!')

        # Map chunk to slaves
        for i in range(chunk_idx):
            map_chunk_to_slave(file_path + str(i), i, chunk_idx)

        message = 'The upload_file "%s" was uploaded successfully' % fi.name

    else:
        message = 'No upload_file was uploaded'

    return message


def update_one_file(request):
    delete_one_file(request)
    upload_one_file(request)
