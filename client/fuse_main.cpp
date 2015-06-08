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
#include <sstream>
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

const char FOLDER_SPLIT = '@';
const string FOLDER_SPLIT_STR = "@";
const string FOLDER_FAKE_FILE = ".holder";

struct FILE_DES {
	FILE_DES() { file_name = "NotInitialized"; is_folder = false;  file_size = 4;}
	string file_name;
	bool is_folder;
	int file_size;
	vector<string> folder; // where the file resides
};


//helper function to manipulate the file_name and directory name
std::vector<std::string> &split(const std::string &s, char delim, std::vector<std::string> &elems) {
    std::stringstream ss(s);
    std::string item;
    while (std::getline(ss, item, delim)) {
        elems.push_back(item);
    }
    return elems;
}
std::vector<std::string> split(const std::string &s, char delim) {
    std::vector<std::string> elems;
    split(s, delim, elems);
    return elems;
}
std::string join_str(const std::vector<string>& strs, string fill)
{
	stringstream ss;
	if(strs.size() == 0) return "";
	ss << strs[0];
	for(int i = 1; i < strs.size(); i++) {
		ss << fill << strs[i];
	}
	return ss.str();
}
FILE_DES convert_raw_file_name2des(const string& file_name)
{
	vector<string> strings = split(file_name,FOLDER_SPLIT);
	FILE_DES fd;
	fd.file_name = strings[strings.size()-1];
	strings.pop_back();
	fd.folder = strings;
	return fd;
}
string convert_local_to_remote(string str)
{
	if(str[0] == '/') str = str.substr(1);
	for(int i = 0;i < str.size();i++) {
		if(str[i] == '/') str[i] = FOLDER_SPLIT;
	}
	return str;
}
vector<FILE_DES> get_all_file_under_a_path(StoreEngine* se, string path)
{
	vector<FILE_DES> result;
	vector<FileSize> file_names = se->list_all_files();
	for(FileSize fs : file_names) {
		string file_name = fs.file_name;
		int file_size = fs.file_size;
		FILE_DES fd = convert_raw_file_name2des(file_name);
		// whether the paths is identical
		if(path[0] == '/') path = path.substr(1);
		vector<string> paths_to_find = split(path,'/');
		
		if(paths_to_find.size() > fd.folder.size())
			continue;
		if(fd.folder.size() - paths_to_find.size() >= 2) continue;
		/* whether it is a folder */
		if(fd.file_name == FOLDER_FAKE_FILE) {
			if(fd.folder.size() != paths_to_find.size() + 1) continue;
			bool same_folder = true;
			for(int i = 0; i < paths_to_find.size();i++) {
				if(paths_to_find[i] != fd.folder[i]) { same_folder = false; break; }
			}
			if(same_folder == false) continue;
			// add this folder
			string folder_name = fd.folder[paths_to_find.size()];
			fd.file_name = folder_name;
			fd.folder = paths_to_find;
			fd.is_folder = true; fd.file_size = 4;
			cout << LOG_H << "query : " << path << " folder: " << folder_name << endl;
			result.push_back(fd);
			continue;			
		}
		// whether the fd is same,
		bool same_folder = true;
		if(fd.folder.size() != paths_to_find.size()) continue;
		for(int i = 0; i < paths_to_find.size(); i++) {
			if(paths_to_find[i] != fd.folder[i]) {
				same_folder = false;
				break;
			}
		}
		if(!same_folder) continue;
		// add the file to the final result
		fd.file_size = file_size;
		fd.is_folder = false;
		cout << LOG_H << "query : " << path << " file: " << file_name << endl;
		result.push_back(fd);
	}
	return result;
}

FILE_DES get_a_file_by_path(StoreEngine* se, string path, bool& exist) {
	// The path looks like /aa/bb
	if(path[0] == '/') path = path.substr(1);
	vector<string> paths = split(path,'/');
	string file_name = paths[paths.size()-1];
	paths.pop_back();
	string path_new = "/" + join_str(paths,"/");

	vector<FILE_DES> file_names = get_all_file_under_a_path(se,path_new);
	for(FILE_DES f : file_names) {
		if(file_name == f.file_name) {
			exist = true;
			return f;
		}
	}
	exist = false;
	return FILE_DES();
}

static int hello_getattr(const char *path, struct stat *stbuf)
{
	int res = 0;
	if(strcmp(path,"/folder/mm2/mm3/mm4/mm5") == 0) {
		res = 0;
	}
	if (strcmp(path, "/") == 0) {
		stbuf->st_mode = S_IFDIR | 0755;
		stbuf->st_nlink = 2;
		return 0;
	}
	
	cout << LOG_H << "getattr" << string(path) << " " << endl;
	StoreEngine* se = StoreEngine::get_instance();
	bool exist = false;
	FILE_DES file_name = get_a_file_by_path(se, string(path), exist);
	
	memset(stbuf, 0, sizeof(struct stat));
	
	if(exist == false) return -ENOENT;
	stbuf->st_mode = S_IFREG | 0777;
	if(file_name.is_folder == true)
	stbuf->st_mode = S_IFDIR | 0777;
	stbuf->st_nlink = 1;
	stbuf->st_size = file_name.file_size;
	return 0;
}

static int hello_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
			 off_t offset, struct fuse_file_info *fi)
{
	(void) offset;
	(void) fi;
	cout << LOG_H << "readdir " << string(path) << endl;
	
	filler(buf, ".", NULL, 0);
	filler(buf, "..", NULL, 0);
	StoreEngine* se = StoreEngine::get_instance();
	vector<FILE_DES> file_names = get_all_file_under_a_path(se, string(path));
	for(FILE_DES fs : file_names) {
		if(fs.is_folder == true) {
			struct stat fake_stat;
			fake_stat.st_mode = S_IFDIR | 0777;
			fake_stat.st_nlink = 1;
			fake_stat.st_size = fs.file_size;
			filler(buf, fs.file_name.c_str(), &fake_stat, 0);
		}
		else {
			filler(buf, fs.file_name.c_str(), NULL, 0);
		}
	}
	return 0;
}
static int hello_rmdir(const char* path)
{
	cout << LOG_H << "RMDIR " << string(path) << endl;
	string folder_name(path);
	if(strcmp(path,"/") == 0) return -EACCES;
	if(folder_name[0] == '/') folder_name = folder_name.substr(1);
	folder_name = convert_local_to_remote(folder_name);
	folder_name += FOLDER_SPLIT_STR + FOLDER_FAKE_FILE;
	
	StoreEngine* se = StoreEngine::get_instance();
	if(se->del_file(folder_name) < 0)
		return -EACCES;
	se->force_update();	
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
	pp = convert_local_to_remote(pp);
	
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
	file_name = convert_local_to_remote(file_name);
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
	file_name = convert_local_to_remote(file_name);
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
/*
static int hello_opendir(const char * path, struct fuse_file_info* fi)
{
	cout << LOG_H << "Open Dir ..." << endl;
	return 0;
}*/
int hello_mkdir(const char * file_name, mode_t m)
{
	cout << LOG_H << " MKDIR " << string(file_name) << endl;
	//just create a file name, FOLDER_FAKE_FILE
	string folder_name = string(file_name); if(folder_name[0] == '/') folder_name = folder_name.substr(1);
	for(int i = 0;i < folder_name.size();i++) {
		if(folder_name[i] == '/') {
			folder_name[i] = FOLDER_SPLIT;
		}
	}
	folder_name += "" + FOLDER_SPLIT_STR + FOLDER_FAKE_FILE;
	cout << LOG_H << " MKDIR the true name is" << string(folder_name) << endl;
	// create the fake file
	StoreEngine* se = StoreEngine::get_instance();
	int ret = se->create_file(folder_name);
	if(ret < 0)
		return -EACCES;		
	se->force_update();
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
	hello_oper.create   = hello_create;
	hello_oper.open		= hello_open;
	hello_oper.read		= hello_read;
	hello_oper.release  = hello_release;
	hello_oper.write    = hello_write;  
	hello_oper.truncate = hello_truncate;
	hello_oper.utime    = hello_utime;
	hello_oper.unlink   = hello_unlink;
	hello_oper.mkdir    = hello_mkdir;
	hello_oper.rmdir    = hello_rmdir;
	
	/* Init the libcurl */
	//HTTPClient::InitHTTPConnection(MASTER_SERVER_URL,false);
	//test_main_meta_info();
	// Get all the file_names first 
	//get_all_file_from_server(g_all_files);
	//return 0;
	return fuse_main(argc, argv, &hello_oper, NULL);
}