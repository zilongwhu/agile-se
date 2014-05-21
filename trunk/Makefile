agile-se: ./src/*.cpp ./src/*.c ./include/*.h
	g++ -g -Wall -DLOG_LEVEL=2 -Iinclude -Iproto -I../conflib/include -I../lsnet/include -I/usr/protobuf/include ./src/*.cpp ./src/*.c ./proto/*.cc -lpthread -lssl -Xlinker "-(" ../conflib/libconf.a ../lsnet/liblsnet.a /usr/protobuf/lib/libprotobuf.a -Xlinker "-)" -o agile-se

clean:
	rm -rf agile-se
