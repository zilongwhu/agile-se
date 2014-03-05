agile-se: ./src/*.cpp ./src/*.c ./include/*.h
	g++ -Iinclude ./src/*.cpp ./src/*.c -lpthread -o agile-se

clean:
	rm -rf agile-se
