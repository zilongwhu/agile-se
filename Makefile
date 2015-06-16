agile-se: ./src/*.cpp ./src/*.c ./include/*.h
	g++ -g -Wall -Wno-deprecated -fPIC -shared -DLOG_LEVEL=2 -Iinclude -I../conflib/include -I../lsnet/include ./src/*.cpp ./src/*.c -lpthread -lssl -o agile-se.so

clean:
	rm -rf agile-se.so
