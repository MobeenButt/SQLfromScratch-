#include <vector>
#include <string>
#include <cstring>
#include <stdexcept>

class Record {
public:
    std::vector<std::string> values;

    void serialize(char* buffer) const {
        size_t pos = 0;
        for (const auto& value : values) {
            std::strcpy(buffer + pos, value.c_str());
            pos += value.length() + 1;  // +1 for null terminator
        }
    }

    void deserialize(const char* buffer, size_t size) {
        values.clear();
        size_t pos = 0;
        while (pos < size) {
            std::string value(buffer + pos);
            values.push_back(value);
            pos += value.length() + 1;  // +1 for null terminator
        }
    }

    size_t getSize() const {
        size_t total_size = 0;
        for (const auto& value : values) {
            total_size += value.length() + 1;  // +1 for null terminator
        }
        return total_size;
    }
    
}; 