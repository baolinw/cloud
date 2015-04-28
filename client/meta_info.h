#ifndef _META_INFO_H_
#define _META_INFO_H_

#include <string>
#include <sstream>
#include <vector>
#include <iostream>

//Indicate the location of the chunk
class MsgParser;
struct ChunkInfo
{
	std::string file_name;
	int chunk_index;
	int chunk_id; // Id is the only valid identifier
	std::vector<std::string> server_id_of_chunk;
	
	std::string convert_to_url();
	std::string toStr();
	void rebuild_from_msg(MsgParser& res);
};

// This file includes the metainfo and some functions manipulating it
//Shared between server and client
struct MetaFileInfo
{
	std::string file_name;
	int file_size;
	bool is_folder;
	std::vector<ChunkInfo> all_chunks;	
	int request_chunk_index;
	
	//Functions	
	// send the request
	std::string convert_to_url();
	//parse the result of the result
	void rebuild_from_msg(MsgParser& res);	
	std::string toStr();
};

//helper function to handle response from server
class MsgParser
{
public:
	MsgParser(std::string result);
	bool has_next();
	std::string get_next();
	int get_next_int();
	int is_error() {
		return is_err_;
	}
private:
	std::vector<std::string> str;
	int cur_index;
	int is_err_;
};

//Functions
int test_main_meta_info(int argc,char** argv); //Test function
// get only file_names and size, info , to get the chunkinfo, use get_chunk_info instead
std::vector<MetaFileInfo> get_all_files(std::string folder_name);
std::vector<ChunkInfo> get_chunk_info(std::string file_name, int chunk_index);


#endif