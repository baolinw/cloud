// This file mainly implements the Requester in the client
#include <stdlib.h>
#include <assert.h>
#include "meta_info.h"
#include "http_lib.h"

using namespace std;

//helper function to split string
static std::vector<std::string> &split(const std::string &s, char delim, std::vector<std::string> &elems) {
    std::stringstream ss(s);
    std::string item;
    while (std::getline(ss, item, delim)) {
        elems.push_back(item);
    }
    return elems;
}


void ChunkInfo::rebuild_from_msg(MsgParser& mp)
{
	file_name = mp.get_next();
	chunk_index = mp.get_next_int();
	chunk_id = mp.get_next_int();
	
	int num_of_server = mp.get_next_int();
	server_id_of_chunk.clear();
	
	for(int i = 0; i < num_of_server; i++) {
		string server_id = mp.get_next();
		server_id_of_chunk.push_back(server_id);
	}	
}

string ChunkInfo::toStr()
{
	stringstream ss;
	ss << " ChunkInfo --------" << endl;
	ss << " filename: " << file_name << endl;
	ss << " chunk_index: " << chunk_index << endl;
	ss << " chunk_id: " << chunk_id << endl;
	ss << " number_server: " << server_id_of_chunk.size() << endl;
	ss << " Server: " << endl;
	for(int i = 0; i < (int)server_id_of_chunk.size(); i ++) {
		ss << server_id_of_chunk[i] << " ";
	}
	ss << endl << "End Chunk --------------" << endl;
	return ss.str();
}

string MetaFileInfo::convert_to_url()
{
	stringstream ss;
	ss << "meta_file_info?file_name=" << file_name << "&request_chunk_index=" << request_chunk_index;
	return ss.str();
}

void MetaFileInfo::rebuild_from_msg(MsgParser& mp)
{
	/*MsgParser mp(res.get_raw_str()); 
	int error_code = mp.get_next_int();
	if(error_code < 0)
		cerr << "ERROR " << error_code << mp.get_next() << endl;*/
		
	file_name = mp.get_next();
	file_size = mp.get_next_int();
	is_folder = (mp.get_next_int() != 0);
	all_chunks.clear();
	
	int num_of_chunk_info = mp.get_next_int();
	for(int i = 0; i < num_of_chunk_info; i++) {
		ChunkInfo ci;
		ci.rebuild_from_msg(mp);
		all_chunks.push_back(ci);
	}	
}

string MetaFileInfo::toStr()
{
	stringstream ss;
	ss << "MetaInfoFile for name:" << file_name << " size: " << file_size << endl;
	for (ChunkInfo ci : all_chunks) {
		ss << ci.toStr() << endl;
	}
	ss << "MetaInfo ends --------------------" << endl;
	return ss.str();		
}
	
//Parse the result returned from server
//the message looks like:
//3:filename:1024:2:google:chunkid:dbox:chunkid:filename:...:
MsgParser::MsgParser(string result) 
{
	split(result, ':', str);
	cur_index = 0;
	is_err_ = ((str.size() == 0) || (get_next_int() != 0));
	if(is_err_) {
		cerr << "MsgParser initializtion Error!" << endl;
	}
}
bool MsgParser::has_next()
{
	return cur_index < (int)str.size();
}
string MsgParser::get_next()
{
	assert(has_next());
	string ret = str[cur_index++];
	return ret;
}
int MsgParser::get_next_int()
{
	assert(has_next());
	string s = get_next();
	return atoi(s.c_str());
}

//Functions Implementation

// The MetaFileInfo is partially set, the name, size and 
vector<MetaFileInfo> get_all_files(string folder_name)
{
	//build the http request	
	stringstream http_request_ss;
	http_request_ss << "get_all_files?folder_name=" << folder_name;
	
	HttpResponse response = HTTPClient::Request(http_request_ss.str());
	
	//the returned string
	string returned_result = response.get_raw_str();
	MsgParser msg(returned_result);
	
	int num_of_files = msg.get_next_int();
	
	vector<MetaFileInfo> ret_result;
	
	for(int i = 0; i < (int)num_of_files; i++) {
		MetaFileInfo mfi;
		mfi.file_name = msg.get_next();
		mfi.file_size = msg.get_next_int();
		mfi.is_folder = (msg.get_next_int() == 1)?true:false;
		ret_result.push_back(mfi);
	}
	return ret_result;
};

//if chunk_index is -1, meaning to get all chunk index
vector<ChunkInfo> get_chunk_info(string file_name, int chunk_index)
{
	// fill in param
	MetaFileInfo fake;
	fake.file_name = file_name;
	fake.request_chunk_index = chunk_index;
	
	//Send request
	string response = HTTPClient::Request(fake.convert_to_url()).get_raw_str();
	MsgParser mp(response);
	fake.rebuild_from_msg(mp);
	return fake.all_chunks;
};

int test_main_meta_info()
{	
	vector<MetaFileInfo> all_file_info = get_all_files("/");
	printf("The get_all_files rets \n");
	for(MetaFileInfo mfi : all_file_info) {
		printf("%s\n", mfi.toStr().c_str());
		
	}
	printf("------------------------\n");
	
	vector<ChunkInfo> chunks = get_chunk_info("tuzi.txt", -1);
	printf("get the info of tuzi...\n");
	for(ChunkInfo ci : chunks) {
		printf("%s\n", ci.toStr().c_str());
	}
	printf("---------------------------\n");
	/*//get single ChunkInfo
	printf("get tuzi.txt 's 0 chunk\n");
	chunks = get_chunk_info("tuzi.txt", 0);
	for(ChunkInfo ci : chunks) {
		printf("%s\n", ci.toStr().c_str());
	}
	printf("---------------------------\n");
	// get non-exist ChunkInfo
	printf("get non-exist.txt 's 0 chunk\n");
	chunks = get_chunk_info("non-exist.txt", 0);
	for(ChunkInfo ci : chunks) {
		printf("%s\n", ci.toStr().c_str());
	}
	printf("---------------------------\n");*/
	return 0;
}