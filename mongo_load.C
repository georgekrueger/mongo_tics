
#include <iostream>
#include <fstream>
#include <sstream>
#include <algorithm>
#include <vector>
#include <memory>
#include <map>
#include <string>
#include <utility>
#include <boost/iostreams/filtering_stream.hpp>
#include <boost/iostreams/filter/gzip.hpp>
#include <boost/iostreams/copy.hpp>

#include <bsoncxx/builder/stream/document.hpp>
#include <bsoncxx/json.hpp>
#include <mongocxx/client.hpp>
#include <mongocxx/instance.hpp>

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

    //std::map<std::string, std::map<std::string, bsoncxx::builder::basic::document*>> docs;
    std::map<std::string, std::map<std::string, std::vector<std::pair<int64_t, std::string>>>> values;

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
            int64_t microseconds = seconds * 1000000 + sub_seconds;

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

            auto& attr_map = values[symbol];

            // get attributes
            std::string attr;
            while (std::getline(line_stream, attr, '\001')) {
                std::string value;
                std::getline(line_stream, value, '\001');
                /*bool int_val_set = false;
                try {
                    int int_val = stoi(value);
                    int_val_set = true;
                }
                catch(std::invalid_argument& iae) {
                }*/
                auto attr_iter = attr_map.find(attr);
                if (attr_iter == attr_map.end()) {
                    //auto* new_doc = new bsoncxx::builder::basic::document;
                    //attr_map.insert(std::make_pair(attr, new_doc));
                    //new_doc->append(bsoncxx::builder::basic::kvp("contract", symbol));
                    //new_doc->append(bsoncxx::builder::basic::kvp("attr", attr));
                    attr_map[attr].push_back(std::make_pair(microseconds, value));
                }
            }

            if (total_line_count % 1000000 == 0) {
                std::cout << (total_line_count / 1000000.0) << "M" << std::endl;
            }
        }

        std::cout << "total char count: " << total_char_count << std::endl;
        std::cout << "total line count: " << total_line_count << std::endl;
        std::cout << "average line length: " << total_char_count / total_line_count << std::endl;
    }
    catch(const boost::iostreams::gzip_error& exception)
    {
        std::cerr << "error reading compressed file " << tick_file_name << std::endl;
    }

    mongocxx::instance inst{};
    mongocxx::client conn{mongocxx::uri{"mongodb://nutmeg:27017"}};

    for (auto& kv : values)
    {
        auto& symbol = kv.first;
        auto& attr_map = kv.second;
        for (auto& attr_kv : attr_map)
        {
            auto& attr = attr_kv.first;
            bsoncxx::builder::basic::document doc;
            doc.append(bsoncxx::builder::basic::kvp("contract", symbol));
            doc.append(bsoncxx::builder::basic::kvp("attr", attr));
            bsoncxx::builder::basic::array value_arr;
            for (auto& value : attr_kv.second) {
                value_arr.append(value.first);
                value_arr.append(value.second);
            }
            doc.append(bsoncxx::builder::basic::kvp("values", value_arr));
        }
    }

    return 0;
}
