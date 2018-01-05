from file_trans.models import DataFile
import os
import datetime
import paramiko as pk


def get_file(filename):
    #f = UploadFile(filename=filename)
    #return f
    pass


def fbuffer(f, chunk_size=10000):
    while True:
        chunk = f.read(chunk_size)
        if not chunk: break
        yield chunk


def send_chunk_to_slave(slave_ip, local_path, remote_path, usr):
    private_key = pk.RSAKey.from_private_key_file('/home/liuyajun/.ssh/id_rsa')
    ssh = pk.SSHClient()
    ssh.set_missing_host_key_policy(pk.AutoAddPolicy())
    ssh.connect(hostname=slave_ip, port=22, username=usr, pkey=private_key)

    sftp = pk.SFTPClient.from_transport(ssh.get_transport())
    sftp.open()
    sftp.put(local_path, remote_path)
    sftp.close()
    ssh.close()


def map_chunk_to_slave(chunk_path, chunk_idx, chunk_number):
    slice_num = chunk_number / 5
    f_name = chunk_path.split('/')[-1]
    slave_ids = ['10.60.45.10', '10.60.45.19', '10.60.45.22']
    remote_usrs = ['liuyajun', 'lyj', 'jishi511']
    remote_dirs = ['/home/liuyajun/data_files/',
                   '/home/lyj/data_files/',
                   '/large_disk/lyj/data_files/']
    if chunk_idx < slice_num * 3:
        pass
    if slice_num <= chunk_idx < slice_num * 2 or \
       slice_num * 3 <= chunk_idx < slice_num * 4:
        send_chunk_to_slave(slave_ids[0], chunk_path,
                            os.path.join(remote_dirs[0], f_name),
                            remote_usrs[0])
    if slice_num * 2 <= chunk_idx < slice_num * 3 or \
       slice_num * 4 <= chunk_idx < chunk_number:
        send_chunk_to_slave(slave_ids[1], chunk_path,
                            os.path.join(remote_dirs[1], f_name),
                            remote_usrs[1])
    if chunk_idx < slice_num or slice_num * 3 <= chunk_idx < chunk_number:
        send_chunk_to_slave(slave_ids[2], chunk_path,
                            os.path.join(remote_dirs[2], f_name),
                            remote_usrs[2])

    # Delete chunks not mapped to this node
    if chunk_idx >= slice_num * 3:
        os.remove(chunk_path)


def upload_one_file(request):
    fi = request.FILES.get('file')
    if fi.name:
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
            map_chunk_to_slave(os.path.join(file_path, str(i)), i, chunk_idx)

        message = 'The upload_file "%s" was uploaded successfully' % fi.name

    else:
        message = 'No upload_file was uploaded'

    return message
