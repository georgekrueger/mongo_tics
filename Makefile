mongo_load: mongo_load.C
	g++ -std=c++11 -I/usr/include/mongocxx/v_noabi -I/usr/include/bsoncxx/v_noabi -o mongo_load mongo_load.C -lboost_iostreams -lmongocxx -lbsoncxx
