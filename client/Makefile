all: fuse_main.o http_lib.o meta_info.o
	g++ -o main $^ -pthread -lfuse -L/usr/lib/x86_64-linux-gnu -lcurl -std=c++11
	
fuse_main.o: fuse_main.cpp
	g++ -Wall $< -D_FILE_OFFSET_BITS=64 -I/usr/include/fuse  -c -o $@ -std=c++11

http_lib.o: http_lib.cpp http_lib.h
	g++ -Wall $< -c -o $@ -std=c++11
	
meta_info.o: meta_info.cpp meta_info.h http_lib.h
	g++ -Wall $< -c -o $@ -std=c++11
	
clean:
	rm *.o main