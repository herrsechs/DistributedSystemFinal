from django.http import StreamingHttpResponse
def read_file(filename):
    filepath = 'data_files/' + filename
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