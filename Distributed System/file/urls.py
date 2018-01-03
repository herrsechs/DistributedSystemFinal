from django.conf.urls import url

from . import views
urlpatterns = [
    url(r'^(?P<index>[0-9]+)/$', views.index,name='index'),
    url(r'^upload_file/$',views.upload,name='upload_file'),
    url(r'^show_file/$',views.show_file,name='show_file'),
    url(r'^download/$',views.download_file,name='download')


]
