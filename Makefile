mongo_load: mongo_load.C
	g++ -std=c++11 -L/usr/lib/x86_64-linux-gnu -o mongo_load mongo_load.C -lboost_iostreams -pthread -lmongoclient -lboost_thread -lboost_system -lboost_regex
