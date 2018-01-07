import socket
import paramiko as pk
from paramiko import AuthenticationException
import pickle
import os

slave_ids = ['10.60.45.11', '10.60.45.10', '10.60.45.19', '10.60.45.22']
remote_usrs = ['liuyajun', 'liuyajun', 'lyj', 'jishi511']
remote_dirs = ['/home/liuyajun/django_server/DistributedSystemFinal/data_files',
               '/home/liuyajun/data_files/',
               '/home/lyj/data_files/',
               '/large_disk/lyj/data_files/']


def get_ip():
    s = socket.socket(socket.AF_INET, socket.SDK_DGRAM)
    try:
        s.connect(('8.8.8.8', 80))
        return s.getsockname()[0]
    finally:
        s.close()


def send_chunk_to_slave(slave_ip, local_path, remote_path, usr):
    psd_f = open('~/django_server/psd.pkl', 'r')
    psd_dict = pickle.load(psd_f)
    psd_f.close()
    try:
        ssh = pk.SSHClient()
        ssh.set_missing_host_key_policy(pk.AutoAddPolicy())
        ssh.connect(hostname=slave_ip, port=22, username=usr, password=psd_dict[slave_ip])

        sftp = pk.SFTPClient.from_transport(ssh.get_transport())
        sftp.put(local_path, remote_path)
        sftp.close()
        ssh.close()
    except AuthenticationException:
        print('Exception when visiting %s' % slave_ip)


def get_chunk_from_slave(slave_ip, local_path, remote_path, usr):
    psd_f = open('~/django_server/psd.pkl', 'r')
    psd_dict = pickle.load(psd_f)
    psd_f.close()
    try:
        ssh = pk.SSHClient()
        ssh.set_missing_host_key_policy(pk.AutoAddPolicy())
        ssh.connect(hostname=slave_ip, port=22, username=usr, password=psd_dict[slave_ip])

        sftp = pk.SFTPClient.from_transport(ssh.get_transport())
        sftp.get(remote_path, local_path)
        sftp.close()
        ssh.close()
    except AuthenticationException:
        return False
    return True


def map_chunk_to_slave(chunk_path, chunk_idx, chunk_number):
    slice_num = chunk_number / 5
    f_name = chunk_path.split('/')[-1]
    local_ip = get_ip()

    if chunk_idx < slice_num * 3 and not local_ip == slave_ids[0]:
        send_chunk_to_slave(slave_ids[0], chunk_path,
                            os.path.join(remote_dirs[0], f_name),
                            remote_usrs[0])
    if not local_ip == slave_ids[1] and \
       slice_num <= chunk_idx < slice_num * 2 or \
       slice_num * 3 <= chunk_idx < slice_num * 4:
        send_chunk_to_slave(slave_ids[1], chunk_path,
                            os.path.join(remote_dirs[1], f_name),
                            remote_usrs[1])
    if not local_ip == slave_ids[2] and \
       slice_num * 2 <= chunk_idx < slice_num * 3 or \
       slice_num * 4 <= chunk_idx < chunk_number:
        send_chunk_to_slave(slave_ids[2], chunk_path,
                            os.path.join(remote_dirs[2], f_name),
                            remote_usrs[2])
    if not local_ip == slave_ids[3] and \
            chunk_idx < slice_num or slice_num * 3 <= chunk_idx < chunk_number:
        send_chunk_to_slave(slave_ids[3], chunk_path,
                            os.path.join(remote_dirs[3], f_name),
                            remote_usrs[3])

    # Delete chunks not mapped to this node
    if chunk_idx >= slice_num * 3:
        os.remove(chunk_path)
