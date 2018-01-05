from django.http import StreamingHttpResponse
from file_trans.models import DataFile


def read_file(filename):
    df = DataFile.objects.filter(filename=filename)[0]
    chunk_num = df.chunk_size  # Number of chunks

    for i in range(chunk_num):
        filepath = 'data_files/' + filename + str(i)
        chunksize = 1000
        with open(filepath, "rb") as fr:
            while True:
                c = fr.read(chunksize)
                if c:
                    yield c
                else:
                    break


def downloadFile(filename):
    response = StreamingHttpResponse(read_file(filename))
    response['Content-Type'] = 'application/octet-stream'
    response['Content-Disposition'] = 'attachment;filename="{0}"'.format(filename)
    return response
