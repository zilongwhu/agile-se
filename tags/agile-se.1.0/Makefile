agile-se: ./src/*.cpp ./src/*.c ./include/*.h
	g++ -g -Wall -Iinclude -I../conflib/include -I../lsnet/include ./src/*.cpp ./src/*.c -lpthread -lssl -Xlinker "-(" ../conflib/libconf.a ../lsnet/liblsnet.a -Xlinker "-)" -o agile-se

clean:
	rm -rf agile-se
