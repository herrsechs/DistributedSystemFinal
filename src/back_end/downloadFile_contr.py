from django.http import StreamingHttpResponse
from file_trans.models import DataFile
from ip_mapping import get_chunk_from_slave
import os


def read_file(filename):
    df = DataFile.objects.filter(filename=filename)[0]
    chunk_num = df.chunk_size  # Number of chunks
    slice_num = chunk_num / 5

    slave_ids = ['10.60.45.10', '10.60.45.19', '10.60.45.22']
    remote_usrs = ['liuyajun', 'lyj', 'jishi511']
    remote_dirs = ['/home/liuyajun/data_files/',
                   '/home/lyj/data_files/',
                   '/large_disk/lyj/data_files/']

    for i in range(chunk_num):
        local_path = os.path.join('data_files', filename)

        if i < slice_num * 3:
            filepath = 'data_files/' + filename + str(i)
            with open(filepath, "rb") as fr:
                c = fr.read()
                yield c
        elif slice_num * 3 <= i < slice_num * 4:
            flag = False
            for j in [0,2]:
                flag = get_chunk_from_slave(slave_ids[j],
                                            local_path+str(i),
                                            os.path.join(remote_dirs[j], filename + str(i)),
                                            remote_usrs[j])
                if flag:
                    break
                else:
                    print('Cannot connect to server %s when requiring chunk %i' % (slave_ids[i], i))
            if flag:
                filepath = 'data_files/' + filename + str(i)
                with open(filepath, "rb") as fr:
                    c = fr.read()
                    yield c
                os.remove(filepath)
        elif slice_num * 4 <= i < slice_num * 5:
            flag = False
            for j in [1, 2]:
                flag = get_chunk_from_slave(slave_ids[j],
                                            local_path + str(i),
                                            os.path.join(remote_dirs[j], filename + str(i)),
                                            remote_usrs[j])
                if flag:
                    break
                else:
                    print('Cannot connect to server %s when requiring chunk %i' % (slave_ids[i], i))
            if flag:
                filepath = 'data_files/' + filename + str(i)
                with open(filepath, "rb") as fr:
                    c = fr.read()
                    yield c
                os.remove(filepath)


def downloadFile(filename):
    response = StreamingHttpResponse(read_file(filename))
    response['Content-Type'] = 'application/octet-stream'
    response['Content-Disposition'] = 'attachment;filename="{0}"'.format(filename)
    return response
