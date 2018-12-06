#include <iostream>
#include <fstream>
#include <sstream>
#include <algorithm>
#include <vector>
#include <memory>
#include <map>
#include <boost/iostreams/filtering_stream.hpp>
#include <boost/iostreams/filter/gzip.hpp>
#include <boost/iostreams/copy.hpp>
#include <mongo/client/dbclient.h>
#include <mongo/bson/bson.h>

std::map<std::string, std::string> ticks;

int main(int argc, char** argv)
{
    std::string tick_file_name = "/data/2018/11/13/tics/cme_11_F.0.gz";
    std::ifstream file_stream(tick_file_name, std::ios_base::in | std::ios_base::binary);
    if(file_stream.is_open() == false)
    {
        std::cout << " couldn't open file" << tick_file_name << std::endl;
        return 1;
    }

    boost::iostreams::filtering_istream tick_file;
    tick_file.push(boost::iostreams::basic_gzip_decompressor<>());
    tick_file.push(file_stream);

    std::string line;
    unsigned long long total_char_count = 0;
    unsigned long long total_line_count = 0;
    unsigned int partition = 0;

    mongo::client::initialize();
    mongo::DBClientConnection c;

    try {
        c.connect("nutmeg");
        std::cout << "connected ok" << std::endl;
    } catch( const mongo::DBException &e ) {
        std::cout << "caught " << e.what() << std::endl;
    }

    mongo::BSONObjBuilder b;
    b.append("name", "Joe");
    b.append("age", 33);
    mongo::BSONObj p = b.obj();

    c.insert("tutorial.persons", p);
    string e = c.getLastError();
    if (!e.empty()) {
        cout << "insert failed: " << e << endl;
    }


    /*
    try
    {
        // skip first line that is version number
        std::getline(tick_file, line);

        while (std::getline(tick_file, line))
        {
            ++total_line_count;
            total_char_count += line.size();

            std::istringstream line_stream(line);
            std::string token;

            // get update time
            std::getline(line_stream, token, '\001');
            std::string seconds_str;
            std::string sub_seconds_str;
            std::istringstream time_stream(token);
            std::getline(time_stream, seconds_str, '.');
            std::getline(time_stream, sub_seconds_str, '.');

            unsigned long seconds = stoul(seconds_str);
            unsigned long sub_seconds = stoul(sub_seconds_str);
//            auto mongo_date_time = mongo::Date_t((seconds * 1000) + (sub_seconds / 1000));

            // get symbol
            std::string symbol;
            std::getline(line_stream, symbol, '\001');

            // get update type
            std::getline(line_stream, token, '\001');
            if (token == "S") {
                //std::cout << "snapshot" << std::endl;
            }
            else {
                //std::cout << "update" << std::endl;
            }

            // get attributes
            std::string attr;
            while (std::getline(line_stream, attr, '\001')) {
                std::string value;
                std::getline(line_stream, value, '\001');
                //std::cout << attr << ": " << value << std::endl;
            }

            if (total_line_count % 1000000 == 0) {
                std::cout << (total_line_count / 1000000.0) << "M" << std::endl;
            }

//            if (total_line_count % 15000000 == 0) {
////                mongo::DBClientConnection c;
////                try {
////                    std::cout << "connect to mongodb and upload data" << std::endl;
////                    c.connect("localhost");
////                } catch( const mongo::DBException &e ) {
////                    std::cout << "caught " << e.what() << std::endl;
////                }

//                for (auto& kv : ticks)
//                {
//                    auto symbol = kv.first;
//                    auto data = kv.second;
//                    std::cout << symbol << ": " << data.size() << std::endl;

//                    std::stringstream in_stream;
//                    in_stream << data;
//                    std::ostringstream out_stream;
//                    //std::ofstream out_stream("data/" + symbol + ".gz", std::ios_base::out);
//                    boost::iostreams::filtering_streambuf< boost::iostreams::input> in;
//                    in.push( boost::iostreams::gzip_compressor());
//                    in.push( in_stream );
//                    boost::iostreams::copy(in, out_stream);

////                    mongo::BSONObjBuilder b;
////                    b.genOID();
////                    b.append("symbol", symbol);
////                    b.append("date", "20181113");
////                    b.append("partition", partition);
////                    b.append("data", out_stream.str());
////                    mongo::BSONObj p = b.obj();

////                    c.insert("tics.tics", p);
//                }
//                ticks.clear();
//                partition++;
//            }
        }

        std::cout << "total char count: " << total_char_count << std::endl;
        std::cout << "total line count: " << total_line_count << std::endl;
        std::cout << "average line length: " << total_char_count / total_line_count << std::endl;
    }
    catch(const boost::iostreams::gzip_error& exception)
    {
        std::cerr << "error reading compressed file " << tick_file_name << std::endl;
    }
    */

    return 0;
}
