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
#include <map>
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

const std::string LOG_H = "FUSE:: ";

using namespace std;

static int hello_getattr(const char *path, struct stat *stbuf)
{
	int res = 0;
	
	//cout << LOG_H << "getattr" << string(path) << " " << endl;
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
	//cout << LOG_H << "readdir " << string(path) << endl;
	if (strcmp(path, "/") != 0)
		return -ENOENT;

	filler(buf, ".", NULL, 0);
	filler(buf, "..", NULL, 0);
	StoreEngine* se = StoreEngine::get_instance();
	vector<FileSize> file_names = se->list_all_files();
	for(FileSize fs : file_names) {
		filler(buf, fs.file_name, NULL, 0);
	}		
	
	return 0;
}
struct FILE_OPEN_STRUCT {
	FILE_OPEN_STRUCT(){ fh = NULL; is_append = false; is_truncted = false; };	
	string file_name;
	string mode;
	FILE* fh;
	/* For append */
	bool is_append;
	int true_file_length;
	int fake_file_length;
	/* for Truncate */
	bool is_truncted;
};
// Manipulate opened files
map<string,vector<FILE_OPEN_STRUCT*> > g_opened_files;

static int hello_open(const char *path, struct fuse_file_info *fi)
{
	cout << LOG_H << "File Open " << string(path) ;
	
	StoreEngine* se = StoreEngine::get_instance();
	vector<FileSize> file_names = se->list_all_files();
	
	string pp = string(path);
	if(pp[0] == '/') pp = pp.substr(1);
	
	for(int i = 0; i < file_names.size();i++) {
		string file_name = string(file_names[i].file_name);
		if(pp == file_name) {			
			string mode = "";
			if ((fi->flags & 3) == O_RDONLY) mode = "r";
			if ((fi->flags & 3) == O_WRONLY) mode = "r+";
			if ((fi->flags & 3) == O_RDWR) mode = "r+";
			cout << "with mode" << mode << endl;
			if (fi->flags & O_APPEND) cout << LOG_H << "APEND mode " << endl;
			
			FILE* fh = se->open_file(file_name,mode);
			//cout << "1st fh " << fh << endl;
			if(fh == NULL) return -ENOENT;
			FILE_OPEN_STRUCT* fos = new FILE_OPEN_STRUCT();
			fos->file_name = file_name;
			fos->mode = mode;
			fos->fh = fh;
			if (fi->flags & O_APPEND) {
				fos->is_append = true;
				fos->fake_file_length = -1;
				fos->true_file_length = se->get_true_length(file_name);
				cout << LOG_H << " The true file len of " << file_name << " is " << fos->true_file_length << endl;
				for(int i = 0; i < file_names.size();i++) {
					if(file_name == file_names[i].file_name) {
						fos->fake_file_length = file_names[i].file_size;
						break;
					}
				}
				//if(g_opened_files.find(file_name) == g_opened_files.end()) {
				assert(fos->fake_file_length != -1);
			}
			g_opened_files[file_name].push_back(fos);
			fi->fh = (uint64_t)fos;
			
			cout << LOG_H << "File Open Success" << endl;
			return 0;
		}
	}
	return -ENOENT;
}
static int hello_unlink(const char* path)
{
	cout << LOG_H << "Remove " << string(path) << endl;
	string file_name = string(path); if(file_name[0] == '/') file_name = file_name.substr(1);
	StoreEngine* se = StoreEngine::get_instance();
	if(se->del_file(file_name) < 0)
		return -EACCES;
	se->force_update();
	return 0;
}

static int hello_create(const char* path, mode_t mode, struct fuse_file_info* fi)
{
	cout << LOG_H << "Create " << string(path) << " mode " << (mode&3) << " " << ((fi->flags) & 3) << endl;
	string file_name = string(path); if(file_name[0] == '/') file_name = file_name.substr(1);
	StoreEngine* se = StoreEngine::get_instance();
	int ret = se->create_file(file_name);
	if(ret < 0)
		return -EACCES;
		
	se->force_update();
	/* open it */
	return hello_open(path,fi);
}

static int hello_read(const char *path, char *buf, size_t size, off_t offset,
		      struct fuse_file_info *fi)
{
	size_t len;
	(void) fi;
	cout << LOG_H << "read offset " << offset << " size " << size << endl;
	StoreEngine* se = StoreEngine::get_instance();
	
	FILE_OPEN_STRUCT* fos = (FILE_OPEN_STRUCT*)fi->fh;
	//cout << "fos " << fos << endl;
	//cout << "new path " << string(path) << endl;
	if(fos == NULL) return -ENOENT;
	
	FILE* f = fos->fh;
	//cout << "f " << f << endl;
	fseek(f,offset,SEEK_SET);
	int ret_size = fread(buf,1,size,f);
	cout << LOG_H << " Readed Size : " << ret_size << endl;
	//cout << "----------------------------" << endl;
	//for(int i = 0; i < ret_size; i++) cout << buf[i];
	//cout << endl << "-----------------------------" << endl;
	return ret_size;
}

static int hello_write(const char *path, const char *buf, size_t size, off_t offset,
		      struct fuse_file_info *fi)
{
	cout << LOG_H << "Write path " << string(path) << " off " << offset << endl;
	
	StoreEngine* se = StoreEngine::get_instance();	
	FILE_OPEN_STRUCT* fos = (FILE_OPEN_STRUCT*)fi->fh;
	//cout << "fos " << fos << endl;
	//cout << "new path " << string(path) << endl;
	if(fos == NULL) return -ENOENT;
	
	FILE* f = fos->fh;
	//cout << "f " << f << endl;
	if(fos->is_append) {
		offset -= (fos->fake_file_length - fos->true_file_length);
		cout << "New offset " << fos->fake_file_length << " true:" << fos->true_file_length;
	}
	fseek(f,offset,SEEK_SET);
	int ret_size = fwrite(buf,1,size,f);
	se->make_dirty(fos->file_name,offset,size);
	cout << LOG_H << "Write Size : " << ret_size << " offset: " << offset << endl;
	return ret_size;
}

// Our implementation cares about the release instead of release...., 
static int hello_release(const char* path, struct fuse_file_info* fi)
{
	cout << LOG_H << "In release ..." << endl;
	FILE_OPEN_STRUCT* fos = (FILE_OPEN_STRUCT*)fi->fh;
	if(fos == NULL) return -ENOENT;	
	FILE* f = fos->fh;
	string file_name = fos->file_name;
	StoreEngine* se = StoreEngine::get_instance();
	
	se->close_file(f,file_name);
	
	int index = -1;
	for(int i = 0;i < g_opened_files[file_name].size();i++) {
		if(g_opened_files[file_name][i] == fos) {
			index = i; 
			g_opened_files[file_name].erase(g_opened_files[file_name].begin() + index);
			return 0;
		}
	}
	
	return 0;
}

static int hello_truncate(const char* path, off_t target_size)
{
	StoreEngine* se = StoreEngine::get_instance();
	cout << LOG_H << "Truncate " << string(path) << target_size << endl;
	string file_name(path); if(file_name[0] == '/') file_name = file_name.substr(1);
	if(g_opened_files[file_name].size() > 1) {
		cerr << LOG_H << " truncate when multiple opens " << endl;
		return -EACCES;
	}
	for(FILE_OPEN_STRUCT* fos : g_opened_files[file_name]) {
		fos->is_truncted = true; 
		fclose(fos->fh);
		se->del_file(file_name);
		se->create_file(file_name);
		fos->fh = se->open_file(file_name,"w"); // fake truncated
	}
	cout << LOG_H << "Truncate " << "Finishes " << endl;
	return 0;
}

static int hello_utime(const char *, utimbuf* bufs)
{
	cout << LOG_H << "Utime .." << endl;	return 0;
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
	hello_oper.create   = hello_create;
	hello_oper.open		= hello_open;
	hello_oper.read		= hello_read;
	hello_oper.release  = hello_release;
	hello_oper.write    = hello_write;  
	hello_oper.truncate = hello_truncate;
	hello_oper.utime    = hello_utime;
	hello_oper.unlink   = hello_unlink;
	
	/* Init the libcurl */
	//HTTPClient::InitHTTPConnection(MASTER_SERVER_URL,false);
	//test_main_meta_info();
	// Get all the file_names first 
	//get_all_file_from_server(g_all_files);
	//return 0;
	return fuse_main(argc, argv, &hello_oper, NULL);
}