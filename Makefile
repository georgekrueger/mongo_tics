mongo_load: mongo_load.C
	g++ -g -O0 -std=c++17 -I/usr/include/mongocxx/v_noabi -I/usr/include/bsoncxx/v_noabi -o mongo_load mongo_load.C -lboost_iostreams -lmongocxx -lbsoncxx
