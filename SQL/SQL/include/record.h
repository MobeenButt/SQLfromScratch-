#pragma once
#include <string>
#include <vector>
#include <cstring>
#include <stdexcept>

class Record {
public:
    static const size_t MAX_VALUES = 10;
    static const size_t MAX_STRING_LENGTH = 256;
    std::vector<std::string> values;
    int rid;

    Record() : rid(-1) {}
    ~Record() = default;
    
    // Copy constructor
    Record(const Record& other) : values(other.values), rid(other.rid) {}
    
    // Copy assignment operator
    Record& operator=(const Record& other) {
        if (this != &other) {
            values = other.values;
            rid = other.rid;
        }
        return *this;
    }

    bool operator==(const Record& other) const {
        return values == other.values && rid == other.rid;
    }

    size_t getSize() const {
        size_t size = sizeof(int) + sizeof(size_t);  // rid + number of values
        for (const auto& value : values) {
            size += sizeof(size_t) + value.length();  // length + string content
        }
        return size;
    }

    void serialize(char* buffer) const {
        size_t pos = 0;
        
        // Write RID
        memcpy(buffer + pos, &rid, sizeof(int));
        pos += sizeof(int);
        
        // Write number of values
        size_t num_values = values.size();
        memcpy(buffer + pos, &num_values, sizeof(size_t));
        pos += sizeof(size_t);
        
        // Write each value
        for (const auto& value : values) {
            size_t len = value.length();
            memcpy(buffer + pos, &len, sizeof(size_t));
            pos += sizeof(size_t);
            memcpy(buffer + pos, value.c_str(), len);
            pos += len;
        }
    }

    bool deserialize(const char* buffer, size_t buffer_size) {
        try {
            if (buffer_size < sizeof(int) + sizeof(size_t)) {
                return false;
            }

            size_t pos = 0;
            
            // Read RID
            memcpy(&rid, buffer + pos, sizeof(int));
            pos += sizeof(int);
            
            // Read number of values
            size_t num_values;
            memcpy(&num_values, buffer + pos, sizeof(size_t));
            pos += sizeof(size_t);
            
            if (num_values > MAX_VALUES) {
                std::cerr << "Too many values in record: " << num_values << std::endl;
                return false;
            }
            
            values.clear();
            values.reserve(num_values);
            
            // Read each value
            for (size_t i = 0; i < num_values; i++) {
                if (pos + sizeof(size_t) > buffer_size) {
                    std::cerr << "Buffer overflow reading string length" << std::endl;
                    return false;
                }
                
                size_t len;
                memcpy(&len, buffer + pos, sizeof(size_t));
                pos += sizeof(size_t);
                
                if (len > MAX_STRING_LENGTH || pos + len > buffer_size) {
                    std::cerr << "String too long or buffer overflow: " << len << std::endl;
                    return false;
                }
                
                values.push_back(std::string(buffer + pos, len));
                pos += len;
            }
            
            return true;
        } catch (const std::exception& e) {
            std::cerr << "Error deserializing record: " << e.what() << std::endl;
            return false;
        }
    }

    // Helper functions
    static size_t getRecordSize(const Record& record) {
        return record.getSize();
    }

    static bool serializeRecord(const Record& record, char* buffer, size_t buffer_size) {
        if (buffer_size < record.getSize()) {
            return false;
        }
        record.serialize(buffer);
        return true;
    }

    static bool deserializeRecord(Record& record, const char* buffer, size_t buffer_size) {
        return record.deserialize(buffer, buffer_size);
    }

    // Helper to parse a column value as double (for SUM/AVG)
    double getNumericValue(size_t column_index) const {
        if (column_index >= values.size()) {
            throw std::out_of_range("Column index out of range");
        }
        try {
            return std::stod(values[column_index]);
        } catch (...) {
            throw std::runtime_error("Non-numeric value");
        }
    }
}; 