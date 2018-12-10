
#include <iostream>
#include <fstream>
#include <sstream>
#include <algorithm>
#include <vector>
#include <memory>
#include <map>
#include <string>
#include <utility>
#include <variant>
#include <boost/iostreams/filtering_stream.hpp>
#include <boost/iostreams/filter/gzip.hpp>
#include <boost/iostreams/copy.hpp>

#include <bsoncxx/builder/stream/document.hpp>
#include <bsoncxx/json.hpp>
#include <mongocxx/client.hpp>
#include <mongocxx/instance.hpp>

using std::string;

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
    typedef std::variant<std::string, int, long> Value;
    typedef std::map<std::string, std::vector<std::pair<int64_t, Value>>> AttrMap;
    std::map<std::string, AttrMap> values;

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
                string str_value;
                std::getline(line_stream, str_value, '\001');
                Value value;
                try {
                    int int_val = stoi(str_value);
                    value = int_val;
                }
                catch(std::exception& exception) {
                    try {
                        long long_val = stol(str_value);
                        value = long_val;
                    }
                    catch(std::exception& exception) {
                        value = str_value;
                    }
                }
                attr_map[attr].push_back(std::make_pair(microseconds, value));
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

    std::cout << "connect to mongodb and insert values" << std::endl;

    mongocxx::instance inst{};
    mongocxx::client conn{mongocxx::uri{"mongodb://nutmeg:27017"}};
    auto coll = conn["tics2"]["tics2"];
    auto bulk = coll.create_bulk_write();
    unsigned int max_bulk = 1000;
    unsigned int bulk_count = 0;

    for (auto& kv : values)
    {
        auto& symbol = kv.first;
        auto& attr_map = kv.second;
        std::cout << "insert data for " << symbol << std::endl;
        for (auto& attr_kv : attr_map)
        {
            auto& attr = attr_kv.first;
            auto& attr_values = attr_kv.second;

            //std::cout << " " << attr << " " << attr_values.size() << " values" << std::endl;
            int partition_size = 100000;
            for (int partition = 0; ; ++partition) {
                if (partition * partition_size >= attr_values.size()) {
                    break;
                }
                bsoncxx::builder::basic::document doc;
                doc.append(bsoncxx::builder::basic::kvp("contract", symbol));
                doc.append(bsoncxx::builder::basic::kvp("attr", attr));
                doc.append(bsoncxx::builder::basic::kvp("partition", partition));
                bsoncxx::builder::basic::array ts_arr;
                bsoncxx::builder::basic::array value_arr;
                for (int i = partition * partition_size; i < (partition + 1) * partition_size && i < attr_values.size(); ++i) {
                    ts_arr.append(attr_values[i].first);
                    Value& value = attr_values[i].second;
                    std::visit([&value_arr](auto&& arg) { value_arr.append(arg); }, value);
                }
                doc.append(bsoncxx::builder::basic::kvp("values", value_arr));
                doc.append(bsoncxx::builder::basic::kvp("timestamps", ts_arr));
                mongocxx::model::insert_one insert_op(doc.view());
                bulk.append(insert_op);
                bulk_count++;
                if (bulk_count >= max_bulk) {
                    std::cout << "write batch" << std::endl;
                    auto result = coll.bulk_write(bulk);
                    if (result) {
                        std::cout << "inserted: " << result->inserted_count() << std::endl;
                    }
                    bulk_count = 0;
                    bulk = coll.create_bulk_write();
                }
            }
        }
    }

    std::cout << "write final batch" << std::endl;
    auto result = coll.bulk_write(bulk);
    if (result) {
        std::cout << "inserted: " << result->inserted_count() << std::endl;
    }

    std::cout << "done" << std::endl;

    return 0;
}

//void write_values(std::string contract, std::string attr, std:)