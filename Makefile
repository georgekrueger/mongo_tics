mongo_load: mongo_load.C
	g++ -std=c++11 -I/usr/include/libmongoc-1.0 -I/usr/include/libbson-1.0 -L/usr/lib/x86_64-linux-gnu -o mongo_load mongo_load.C -lboost_iostreams -lmongoc-1.0
