agile-se: ./src/*.cpp
	g++ -Iinclude ./src/*.cpp -o agile-se

clean:
	rm -rf agile-se
