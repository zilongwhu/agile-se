agile-se: ./src/*.cpp ./src/*.c ./include/*.h
	g++ -g -Wall -Iinclude -I../conflib/include ./src/*.cpp ./src/*.c -lpthread -Xlinker "-(" ../conflib/libconf.a -Xlinker "-)" -o agile-se

clean:
	rm -rf agile-se
