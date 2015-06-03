# -*- coding: utf-8 -*-
from BaseHTTPServer import HTTPServer, BaseHTTPRequestHandler  
from urlparse import parse_qs

import file_handler
import meta_puller
import config

def convert_chunk_to_str(chunk):
	''' chunk = [file_name, index, id, server_id_of_chunk, storage_engining]
	convert it to file_name:index:id:num_xxx:server_id_of_chunk:.. num_xxx_enging:0,1,2
	the begining and end are without the ':' '''
	mystr = [chunk['file_name'], chunk['chunk_index'],chunk['chunk_id']]
	mystr.append(str(len(chunk['server_id_of_chunk'])))
	
	for i in range(len(chunk['server_id_of_chunk'])):
		mystr.append(str(chunk['server_id_of_chunk'][i]))
	mystr = [str(i) for i in mystr]
	return ':'.join(mystr)	

def convert_file_to_str(one_server_file):
	mystr = [one_server_file['file_name'], one_server_file['size'],one_server_file['is_folder']]
	mystr = [str(i) for i in mystr]
	return ':'.join(mystr)	
	
def handle_get_all_files(param):
	folder_name = param['folder_name'][0]

	buf = [str(0)]
	all_files = file_handler.get_all_files(folder_name)
	buf.append(str(len(all_files)))
	for i in range(len(all_files)):
		buf.append(convert_file_to_str(all_files[i]))
	return ':'.join(buf)	

def handle_meta_file_info(param):
	file_name = param['file_name'][0]
	request_chunk_index = param['request_chunk_index'][0]
	#print request_chunk_index,'LALALAL'		
	
	#find a file in the FILES
	find_one = False
	FILES = meta_puller.get_all_file_names()
	for index in range(len(FILES)):
		if file_name == FILES[index]:
			find_one = True
			buf = [0] * 4
			buf[1:] = meta_puller.get_file_meta_info(file_name)
			all_chunks = file_handler.get_all_chunks_of_file(file_name)
			buf.append(str(len(all_chunks)))
			for one_chunk in all_chunks:
				buf.append(convert_chunk_to_str(one_chunk))
			buf = [str(i) for i in buf]
			buf = ':'.join(buf)
	if not find_one:
		buf = '-1:Cannot find filename ' + file_name
	return buf

def handle_create_file(param):
	# the input param is { file_name = xxx }
	file_name = param['file_name'][0]
	return file_handler.request_create_file(file_name)
	
def handle_write_file(param):
	file_name = param['file_name'][0]
	chunks = param['chunk_ids'][0]
	chunks = [int(i) for i in chunks.split(',')]
	chunk_sizes = param['chunk_size'][0]
	chunk_sizes = [int(i) for i in chunk_sizes.split(',')]
	if not all([i == config.FILE_CHUNK_SIZE for i in chunk_sizes]):
		return '-3:chunk size should be' + str(config.FILE_CHUNK_SIZE)
	return file_handler.request_write_file(file_name,chunks,chunk_sizes)
	
# manually 'make' the server fail
def handle_fail_server(param):
	server_id = int(param['server_id'][0])
	return file_handler.request_fail_server(server_id)
	
# manually 'make' the server resume
def handle_ok_server(param):
	server_id = int(param['server_id'][0])
	return file_handler.request_ok_server(server_id)

def handle_read_file(param):
	file_name = param['file_name'][0]
	chunks = param['chunk_ids'][0]
	chunks = [int(i) for i in chunks.split(',')]
	return file_handler.request_read_file(file_name,chunks)

def handle_del_file(param):
	file_name = param['file_name'][0]
	return file_handler.del_file(file_name)
	
def handle_commit_trans(param):
	trans_id = int(param['id'][0])
	return file_handler.handle_commit(trans_id,' ')

class TestHTTPHandle(BaseHTTPRequestHandler):   
    def do_GET(self):
		global FILES
		a = self.path
		#print a		
			
		cmd = 'invalid'
		param = ''
		buf = '-1:Unknown Command'
		try:
			cmd,param = a[1:].split('?')		
			param = parse_qs(param)
		except Exception as e:
			#print a,e
			return
			
		if cmd == 'meta_file_info': # get the meta by file_name
			buf = handle_meta_file_info(param)
		if cmd == 'get_all_files':
			buf = handle_get_all_files(param)
		if cmd == 'create_file':
			buf = handle_create_file(param)
		if cmd == 'commit_trans':
			buf = handle_commit_trans(param)
		if cmd == 'del_file':
			buf = handle_del_file(param)
		if cmd == 'write_file':
			buf = handle_write_file(param)
		if cmd == 'read_file':
			buf = handle_read_file(param)
		if cmd == 'fail_server':
			buf = handle_fail_server(param)
		if cmd == 'ok_server':
			buf = handle_ok_server(param)
	
		self.protocal_version = "HTTP/1.1"
		#buf = convert_files_info_to_xml(FILES)
		self.send_response(200)  
  
		self.send_header("Welcome", "Contect")         
  
		self.end_headers()  
  
		self.wfile.write(buf)  
  
def start_server(port):  
    http_server = HTTPServer(('0.0.0.0', int(port)), TestHTTPHandle)  
    http_server.serve_forever() 
    
from lxml import etree
def convert_files_info_to_xml(filelist):
	root = etree.Element('root')
	nums = etree.Element('nums')
	nums.text = str(len(filelist))
	root.append(nums)
	for i in range(len(filelist)):
		filename,engine_type = filelist[i]
		filenode = etree.Element('file' + str(i))
		filenode.text = filename + " FROM " + engine_type
		root.append(filenode)
	return etree.tostring(root, pretty_print = True)	

if __name__ == "__main__":
	

	

	start_server(12345)
	
	
