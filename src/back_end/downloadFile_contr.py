from django.http import StreamingHttpResponse
from file_trans.models import DataFile
from ip_mapping import get_chunk_from_slave, slave_ids, remote_dirs, remote_usrs, get_ip
import os


def read_file(filename):
    df = DataFile.objects.filter(filename=filename)[0]
    chunk_num = df.chunk_size  # Number of chunks
    slice_num = chunk_num / 5

    local_ip = get_ip()
    for i in range(chunk_num):
        local_path = os.path.join('data_files', filename)

        # Mapping server ip to chunk index
        server_list = []
        if i < slice_num:
            server_list.extend([0, 3])
        elif slice_num <= i < slice_num * 2:
            server_list.extend([0, 1])
        elif slice_num * 2 <= i < slice_num * 3:
            server_list.extend([0, 2])
        elif slice_num * 3 <= i < slice_num * 4:
            server_list.extend([1, 3])
        else:
            server_list.extend([2, 3])

        if local_ip == slave_ids[server_list[0]] or local_ip == slave_ids[server_list[1]]:
            filepath = os.path.join(remote_dirs[slave_ids.index(local_ip)], filename + str(i))
            with open(filepath, 'rb') as fr:
                c = fr.read()
                yield c
        else:
            flag = False
            for j in server_list:
                flag = get_chunk_from_slave(slave_ids[j],
                                            local_path + str(i),
                                            os.path.join(remote_dirs[j], filename + str(i)),
                                            remote_usrs[j])
                if flag:
                    break
                else:
                    print('Cannot connect to server %s when requiring chunk %i' % (slave_ids[i], i))
            if flag:
                filepath = os.path.join(remote_dirs[slave_ids.index(local_ip)], filename + str(i))
                with open(filepath, "rb") as fr:
                    c = fr.read()
                    yield c
                os.remove(filepath)


def downloadFile(filename):
    response = StreamingHttpResponse(read_file(filename))
    response['Content-Type'] = 'application/octet-stream'
    response['Content-Disposition'] = 'attachment;filename="{0}"'.format(filename)
    return response
