/*
  FUSE: Filesystem in Userspace
  Copyright (C) 2001-2007  Miklos Szeredi <miklos@szeredi.hu>

  This program can be distributed under the terms of the GNU GPL.
  See the file COPYING.

  gcc -Wall hello.c `pkg-config fuse --cflags --libs` -o hello
*/
#define FUSE_USE_VERSION 26

#include <iostream>
#include <string>
#include <vector>
#include <stdio.h>
#include <sys/types.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <fuse.h>
//#include "http_lib.h"
//#include "meta_info.h"
#include "python_wrapper.h"

static const char *hello_str = "Hello World!\n";
static const char *hello_path = "/hello";

using namespace std;

static int hello_getattr(const char *path, struct stat *stbuf)
{
	int res = 0;
	
	cout <<"getattr" << string(path) << " " << endl;
	StoreEngine* se = StoreEngine::get_instance();
	vector<FileSize> file_names = se->list_all_files();
	
	memset(stbuf, 0, sizeof(struct stat));
	
	if (strcmp(path, "/") == 0) {
		stbuf->st_mode = S_IFDIR | 0755;
		stbuf->st_nlink = 2;
	} else {
		string pp = string(path);
		if(pp[0] == '/') pp = pp.substr(1);
		for(int i = 0; i < file_names.size();i++) {
			if(pp == string(file_names[i].file_name)) {
				//cout << "target is " << pp << " we have " << string(file_names[i].file_name) << endl;
				stbuf->st_mode = S_IFREG | 0666;
				stbuf->st_nlink = 1;
				stbuf->st_size = file_names[i].file_size;
				return 0;
			}
		}
		res = -ENOENT;
	}
	return res;
}

static int hello_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
			 off_t offset, struct fuse_file_info *fi)
{
	(void) offset;
	(void) fi;
	cout << "readdir " << string(path) << endl;
	if (strcmp(path, "/") != 0)
		return -ENOENT;

	filler(buf, ".", NULL, 0);
	filler(buf, "..", NULL, 0);
	StoreEngine* se = StoreEngine::get_instance();
	vector<FileSize> file_names = se->list_all_files();
	for(FileSize fs : file_names) {
		filler(buf, fs.file_name, NULL, 0);
	}		
	//filler(buf, hello_path + 1, NULL, 0);
	/*for(SimpeFile sf : g_all_files) {
		filler(buf, sf.file_name.c_str(), NULL,0);
	}*/

	return 0;
}
struct FILE_OPEN_STRUCT {
	FILE_OPEN_STRUCT(){ fh = NULL; };
	
	string file_name;
	string mode;
	FILE* fh;
};
static int hello_open(const char *path, struct fuse_file_info *fi)
{
	cout << "File Open " << string(path) << endl;
	
	StoreEngine* se = StoreEngine::get_instance();
	vector<FileSize> file_names = se->list_all_files();
	
	string pp = string(path);
	if(pp[0] == '/') pp = pp.substr(1);
	
	for(int i = 0; i < file_names.size();i++) {
		string file_name = string(file_names[i].file_name);
		if(pp == file_name) {			
			string mode = "";
			if ((fi->flags & 3) == O_RDONLY) mode = "r";
			if ((fi->flags & 3) == O_WRONLY) mode = "w";
			if ((fi->flags & 3) == O_RDWR) mode = "r+";
			FILE* fh = se->open_file(file_name,mode);
			//cout << "1st fh " << fh << endl;
			if(fh == NULL) return -ENOENT;
			FILE_OPEN_STRUCT* fos = new FILE_OPEN_STRUCT();
			fos->file_name = file_name;
			fos->mode = mode;
			fos->fh = fh;
			fi->fh = (uint64_t)fos;
			cout << "File Open Success" << endl;
			return 0;
		}
	}
	return -ENOENT;
}

static int hello_read(const char *path, char *buf, size_t size, off_t offset,
		      struct fuse_file_info *fi)
{
	size_t len;
	(void) fi;
	
	StoreEngine* se = StoreEngine::get_instance();
	
	FILE_OPEN_STRUCT* fos = (FILE_OPEN_STRUCT*)fi->fh;
	//cout << "fos " << fos << endl;
	//cout << "new path " << string(path) << endl;
	if(fos == NULL) return -ENOENT;
	
	FILE* f = fos->fh;
	//cout << "f " << f << endl;
	fseek(f,offset,SEEK_SET);
	int ret_size = fread(buf,1,size,f);
	cout << "Size : " << ret_size << endl;
	return ret_size;
}

static int hello_write(const char *path, const char *buf, size_t size, off_t offset,
		      struct fuse_file_info *fi)
{
	cout << "Write path " << string(path) << endl;
	
	StoreEngine* se = StoreEngine::get_instance();	
	FILE_OPEN_STRUCT* fos = (FILE_OPEN_STRUCT*)fi->fh;
	//cout << "fos " << fos << endl;
	//cout << "new path " << string(path) << endl;
	if(fos == NULL) return -ENOENT;
	
	FILE* f = fos->fh;
	//cout << "f " << f << endl;
	fseek(f,offset,SEEK_SET);
	int ret_size = fwrite(buf,1,size,f);
	se->make_dirty(fos->file_name,offset,size);
	cout << "Write Size : " << ret_size << endl;
	return ret_size;
}

// Our implementation cares about the release instead of release...., 
static int hello_release(const char* path, struct fuse_file_info* fi)
{
	cout << "In release ..." << endl;
	FILE_OPEN_STRUCT* fos = (FILE_OPEN_STRUCT*)fi->fh;
	if(fos == NULL) return -ENOENT;	
	FILE* f = fos->fh;
	string file_name = fos->file_name;
	StoreEngine* se = StoreEngine::get_instance();
	
	se->close_file(f,file_name);
	return 0;
}


static struct fuse_operations hello_oper;
int main(int argc, char *argv[])
{
	/* Init the python wrapper */
	StoreEngine* se = StoreEngine::get_instance();
	se->Init(argc,argv);
	se->Mount();
	
	/* Init file operations */
	hello_oper.getattr	= hello_getattr;
	hello_oper.readdir	= hello_readdir;
	hello_oper.open		= hello_open;
	hello_oper.read		= hello_read;
	hello_oper.release  = hello_release;
	hello_oper.write    = hello_write;  
	
	/* Init the libcurl */
	//HTTPClient::InitHTTPConnection(MASTER_SERVER_URL,false);
	//test_main_meta_info();
	// Get all the file_names first 
	//get_all_file_from_server(g_all_files);
	//return 0;
	return fuse_main(argc, argv, &hello_oper, NULL);
}