CXXFLAGS=-g -Wall -Wno-deprecated
INCLUDES=-Iinclude -I../conflib/include -I../cutility/include -I../lsnet/include
.PHONY:all
all: inc_builder.o inc_reader.o\
		const_index.o forward_index.o index.o invert_index.o invert_type.o level_index.o signdict.o\
		parser.o arraylist.o init.o
inc_builder.o: src/inc/inc_builder.cpp
	g++ $(CXXFLAGS) $(INCLUDES) -c $<  -o src/$@
inc_reader.o: src/inc/inc_reader.cpp
	g++ $(CXXFLAGS) $(INCLUDES) -c $<  -o src/$@
const_index.o: src/index/const_index.cpp
	g++ $(CXXFLAGS) $(INCLUDES) -c $<  -o src/$@
forward_index.o: src/index/forward_index.cpp
	g++ $(CXXFLAGS) $(INCLUDES) -c $<  -o src/$@
index.o: src/index/index.cpp
	g++ $(CXXFLAGS) $(INCLUDES) -c $<  -o src/$@
invert_index.o: src/index/invert_index.cpp
	g++ $(CXXFLAGS) $(INCLUDES) -c $<  -o src/$@
invert_type.o: src/index/invert_type.cpp
	g++ $(CXXFLAGS) $(INCLUDES) -c $<  -o src/$@
level_index.o: src/index/level_index.cpp
	g++ $(CXXFLAGS) $(INCLUDES) -c $<  -o src/$@
signdict.o: src/index/signdict.cpp
	g++ $(CXXFLAGS) $(INCLUDES) -c $<  -o src/$@
parser.o: src/parse/parser.cpp
	g++ $(CXXFLAGS) $(INCLUDES) -c $<  -o src/$@
arraylist.o: src/search/arraylist.cpp
	g++ $(CXXFLAGS) $(INCLUDES) -c $<  -o src/$@
init.o: src/init.cpp
	g++ $(CXXFLAGS) $(INCLUDES) -c $<  -o src/$@
clean:
	rm -rf src/*.o
	rm -rf libagile-se.so
