all: fuse_main.o http_lib.o meta_info.o python_wrapper.o
	g++ -g -w -o main $^ -pthread -lfuse -L/usr/lib/x86_64-linux-gnu -lcurl -std=c++11 -L/usr/lib/python2.7/config-x86_64-linux-gnu/ -lpython2.7
	
fuse_main.o: fuse_main.cpp python_wrapper.h
	g++ -g -w $< -D_FILE_OFFSET_BITS=64 -I/usr/include/fuse -I/usr/include/python2.7 -c -o $@ -std=c++11

http_lib.o: http_lib.cpp http_lib.h
	g++ -w -g $< -c -o $@ -std=c++11
	
meta_info.o: meta_info.cpp meta_info.h http_lib.h
	g++ -w $< -c -o $@ -std=c++11
python_wrapper.o: python_wrapper.cpp python_wrapper.h
	g++ -w -g $< -c -o $@ -std=c++11 -I/usr/include/python2.7
	
clean:
	rm *.o main
