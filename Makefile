CXXFLAGS=-g -Wall -Wno-deprecated
INCLUDES=-Iinclude -I../conflib/include -I../cutility/include -I../lsnet/include
OBJECTS=src/inc/inc_builder.o\
		src/inc/inc_reader.o\
		src/index/const_index.o\
		src/index/forward_index.o\
		src/index/index.o\
		src/index/invert_index.o\
		src/index/invert_type.o\
		src/index/level_index.o\
		src/index/signdict.o\
		src/parse/parser.o\
		src/pool/delaypool.o\
		src/search/arraylist.o\
		src/init.o

.PHONY:all
all: libagile-se.a tester

tester: test/main.o libagile-se.a
	g++ $(CXXFLAGS) $(INCLUDES) test/main.o -Xlinker "-(" libagile-se.a ../conflib/libconf.a ../cutility/libutils.a ../lsnet/liblsnet.a -Xlinker "-)" -lssl -lcrypto -lprotobuf -lpthread -o tester
test/main.o: test/main.cpp
	g++ $(CXXFLAGS) $(INCLUDES) -c $<  -o $@

libagile-se.a: $(OBJECTS)
	ar crs $@ $^
src/inc/inc_builder.o: src/inc/inc_builder.cpp
	g++ $(CXXFLAGS) $(INCLUDES) -c $<  -o $@
src/inc/inc_reader.o: src/inc/inc_reader.cpp
	g++ $(CXXFLAGS) $(INCLUDES) -c $<  -o $@
src/index/const_index.o: src/index/const_index.cpp
	g++ $(CXXFLAGS) $(INCLUDES) -c $<  -o $@
src/index/forward_index.o: src/index/forward_index.cpp
	g++ $(CXXFLAGS) $(INCLUDES) -c $<  -o $@
src/index/index.o: src/index/index.cpp
	g++ $(CXXFLAGS) $(INCLUDES) -c $<  -o $@
src/index/invert_index.o: src/index/invert_index.cpp
	g++ $(CXXFLAGS) $(INCLUDES) -c $<  -o $@
src/index/invert_type.o: src/index/invert_type.cpp
	g++ $(CXXFLAGS) $(INCLUDES) -c $<  -o $@
src/index/level_index.o: src/index/level_index.cpp
	g++ $(CXXFLAGS) $(INCLUDES) -c $<  -o $@
src/index/signdict.o: src/index/signdict.cpp
	g++ $(CXXFLAGS) $(INCLUDES) -c $<  -o $@
src/parse/parser.o: src/parse/parser.cpp
	g++ $(CXXFLAGS) $(INCLUDES) -c $<  -o $@
src/pool/delaypool.o: src/pool/delaypool.cpp
	g++ $(CXXFLAGS) $(INCLUDES) -c $<  -o $@
src/search/arraylist.o: src/search/arraylist.cpp
	g++ $(CXXFLAGS) $(INCLUDES) -c $<  -o $@
src/init.o: src/init.cpp
	g++ $(CXXFLAGS) $(INCLUDES) -c $<  -o $@

clean:
	rm -rf $(OBJECTS) libagile-se.a
	rm -rf test/main.o tester
