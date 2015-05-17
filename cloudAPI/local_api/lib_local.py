import argparse
import httplib2
import os
import sys
import json
import io
import os.path
from os import listdir
from os.path import isfile,join

# simulate the 'Cloud' in local storage
ROOT_DIR = '/home/ubuntu/tmp/'

def upload_file(service,from_file_name,to_file_name):
	# try delete it first
	try:
		delete_file(service,'',"/" + to_file_name)
	except Exception as e:
		pass
	# The BytesIO object may be replaced with any io.Base instance.
	f = open(from_file_name,'r')
	out_folder_name = ROOT_DIR + service['folder'] + '/'
	out_f = open(out_folder_name + to_file_name,'w')
	out_f.write(f.read())
	f.close()
	out_f.close()

def upload_string(service, str_to_upload,to_file_name):
	# The BytesIO object may be replaced with any io.Base instance.
	out_folder_name = ROOT_DIR + service['folder'] + '/'
	out_f = open(out_folder_name + to_file_name,'w')
	out_f.write(str_to_upload);
	out_f.close()

def delete_file(service,object_name):
	out_folder_name = ROOT_DIR + service['folder'] + '/'
	os.remove(out_folder_name + object_name);
	
def download_file(service ,object_name, to_file_name):
	in_folder_name = ROOT_DIR + service['folder'] + '/'
	f_in = open(in_folder_name + object_name, 'r');
	f_out = open(to_file_name,'w');
	f_out.write(f_in.read());
	return None

def get_all_file_names(service):
	folder_name = ROOT_DIR + service['folder'] + '/'
	file_names = [(f,os.stat(join(folder_name,f)).st_size) for f in os.listdir(folder_name) if isfile(join(folder_name,f)) ]
	return file_names

def create_service_object(extra_info):
	service = {'folder':extra_info}
	return service
 
if __name__ == "__main__":
	s = create_service_object()
	print get_all_file_names(s)
