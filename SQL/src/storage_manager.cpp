#include "storage_manager.h"
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sys/stat.h>
#include <system_error>
#include <cstring>

namespace fs = std::filesystem;

StorageManager::StorageManager() {
    try {
        fs::create_directories("data");
    } catch (const std::exception& e) {
        std::cerr << "Error creating data directory: " << e.what() << std::endl;
    }
}

StorageManager::~StorageManager() {
    // No file handle member, so nothing to close
}

bool StorageManager::createDatabase(const std::string& db_name) {
    std::string db_path = "./data/" + db_name;
    
    // Create data directory if it doesn't exist
    if (!fs::exists("./data")) {
        fs::create_directory("./data");
    }
    
    // Create database directory if it doesn't exist
    if (!fs::exists(db_path)) {
        return fs::create_directory(db_path);
    }
    
    return true;
}

std::string StorageManager::getTablePath(const std::string& db_name, const std::string& table_name) const {
    return "./data/" + db_name + "/" + table_name + ".dat";
}

std::string StorageManager::getIndexPath(const std::string& db_name, const std::string& index_name) const {
    return "data/" + db_name + "/" + index_name;
}

bool StorageManager::createTable(const std::string& db_name, const std::string& table_name) {
    std::string file_path = "./data/" + db_name + "/" + table_name + ".dat";
    
    // Create empty file
    std::ofstream file(file_path, std::ios::binary);
    if (!file) {
        std::cerr << "Failed to create file: " << file_path << std::endl;
        return false;
    }
    
    file.close();
    
    std::cout << "Successfully created file: " << file_path << std::endl;
    return true;
}

bool StorageManager::createFile(const std::string& filename) {
    try {
        // Ensure the directory exists
        fs::path filepath(filename);
        fs::create_directories(filepath.parent_path());

        // Create the file
        std::ofstream file(filename, std::ios::binary);
        if (!file) {
            std::cerr << "Failed to create file: " << filename << std::endl;
            return false;
        }
        
        // Initialize with empty page
        Page empty_page;
        empty_page.setFreeSpace(PAGE_SIZE_BYTES);
        empty_page.clear();
        
        file.write(reinterpret_cast<const char*>(&empty_page), sizeof(Page));
        
        file.close();
        
        // Verify the file was created
        if (!fs::exists(filename)) {
            std::cerr << "File creation verified failed: " << filename << std::endl;
            return false;
        }
        
        std::cout << "Successfully created file: " << filename << std::endl;
        return true;
    } catch (const std::exception& e) {
        std::cerr << "Error creating file " << filename << ": " << e.what() << std::endl;
        return false;
    }
}

bool StorageManager::dropTable(const std::string& db_name, const std::string& table_name) {
    std::string file_path = "./data/" + db_name + "/" + table_name + ".dat";
    
    if (std::remove(file_path.c_str()) != 0) {
        std::cerr << "Failed to remove file: " << file_path << std::endl;
        return false;
    }
    
    return true;
}

bool StorageManager::writePage(const std::string& filename, int page_id, const Page& page) {
    try {
        std::cout << "Writing page " << page_id << " to " << filename << std::endl;
        std::fstream file(filename, std::ios::binary | std::ios::in | std::ios::out);
        if (!file) {
            std::cerr << "Failed to open file for writing: " << filename << std::endl;
            return false;
        }

        file.seekp(page_id * sizeof(Page));
        file.write(reinterpret_cast<const char*>(&page), sizeof(Page));

        if (!file.good()) {
            std::cerr << "Error writing to file: " << filename << std::endl;
            return false;
        }

        return true;
    } catch (const std::exception& e) {
        std::cerr << "Error writing page: " << e.what() << std::endl;
        return false;
    }
}

bool StorageManager::readPage(const std::string& filename, int page_id, Page& page) {
    try {
        std::ifstream file(filename, std::ios::binary);
        if (!file) {
            std::cerr << "Failed to open file for reading: " << filename << std::endl;
            return false;
        }

        // Get file size
        file.seekg(0, std::ios::end);
        std::streampos file_size = file.tellg();
        
        // Check if page_id is beyond file size
        if (static_cast<std::streampos>(page_id * sizeof(Page)) >= file_size) {
            return false;  // End of file reached
        }

        // Seek to the requested page
        file.seekg(page_id * sizeof(Page));
        if (!file.good()) {
            return false;
        }

        // Read the page
        file.read(reinterpret_cast<char*>(&page), sizeof(Page));
        return file.good();
    } catch (const std::exception& e) {
        std::cerr << "Error reading page: " << e.what() << std::endl;
        return false;
    }
}

bool StorageManager::insertRecord(const std::string& db_name, 
                                 const std::string& table_name, 
                                 const Record& record) {
    std::string file_path = "./data/" + db_name + "/" + table_name + ".dat";
    
    // Calculate record size
    size_t record_size = sizeof(size_t);  // For the total record size
    record_size += sizeof(size_t);        // For the number of values
    
    for (const auto& value : record.values) {
        record_size += sizeof(size_t);    // For each string length
        record_size += value.length();    // For string content
    }
    
    // Create a buffer for the record
    std::vector<char> buffer(record_size);
    size_t pos = 0;
    
    // Write total record size (including this size field)
    memcpy(buffer.data() + pos, &record_size, sizeof(size_t));
    pos += sizeof(size_t);
    
    // Write number of values
    size_t num_values = record.values.size();
    memcpy(buffer.data() + pos, &num_values, sizeof(size_t));
    pos += sizeof(size_t);
    
    // Write each value
    for (const auto& value : record.values) {
        // Write string length
        size_t len = value.length();
        memcpy(buffer.data() + pos, &len, sizeof(size_t));
        pos += sizeof(size_t);
        
        // Write string content
        memcpy(buffer.data() + pos, value.c_str(), len);
        pos += len;
    }
    
    // Open file for appending
    std::ofstream file(file_path, std::ios::binary | std::ios::app);
    if (!file) {
        std::cerr << "Failed to open file for writing: " << file_path << std::endl;
        return false;
    }
    
    // Write the buffer to the file
    file.write(buffer.data(), record_size);
    
    // Check for errors
    if (!file) {
        std::cerr << "Error writing to file: " << file_path << std::endl;
        file.close();
        return false;
    }
    
    file.close();
    std::cout << "Writing page 0 to " << file_path << std::endl;
    return true;
}

std::vector<Record> StorageManager::getAllRecords(const std::string& file_path) {
    std::vector<Record> records;
    
    std::ifstream file(file_path, std::ios::binary);
    if (!file) {
        std::cerr << "Reading records from file: " << file_path << std::endl;
        std::cerr << "Total records read: 0" << std::endl;
        return records;
    }
    
    std::cout << "Reading records from file: " << file_path << std::endl;
    
    // Get file size
    file.seekg(0, std::ios::end);
    size_t file_size = file.tellg();
    file.seekg(0, std::ios::beg);
    
    if (file_size > 0) {
        std::cout << "Reading page 0 with " << file_size << " bytes used" << std::endl;
        
        // Read the entire file into a buffer
        std::vector<char> buffer(file_size);
        file.read(buffer.data(), file_size);
        
        size_t pos = 0;
        while (pos < file_size) {
            // First, check if we have at least the size of a record
            if (pos + sizeof(size_t) > file_size) break;
            
            // Read record size
            size_t record_size;
            memcpy(&record_size, buffer.data() + pos, sizeof(size_t));
            
            // Validate record size
            if (record_size == 0 || pos + record_size > file_size) break;
            
            // Create a record
            Record record;
            record.values.clear();
            
            // Deserialize the record manually
            size_t record_pos = pos + sizeof(size_t); // Skip the record size
            
            // Read number of values
            size_t num_values;
            memcpy(&num_values, buffer.data() + record_pos, sizeof(size_t));
            record_pos += sizeof(size_t);
            
            // Read each value
            for (size_t i = 0; i < num_values && record_pos < file_size; i++) {
                // Read string length
                size_t str_len;
                if (record_pos + sizeof(size_t) > file_size) break;
                memcpy(&str_len, buffer.data() + record_pos, sizeof(size_t));
                record_pos += sizeof(size_t);
                
                // Read string content
                if (record_pos + str_len > file_size) break;
                std::string value(buffer.data() + record_pos, str_len);
                record.values.push_back(value);
                record_pos += str_len;
            }
            
            // Add the record to our list if it has values
            if (!record.values.empty()) {
                records.push_back(record);
            }
            
            // Move to the next record
            pos += record_size;
        }
    }
    
    file.close();
    std::cout << "Total records read: " << records.size() << std::endl;
    return records;
}

bool StorageManager::getRecord(const std::string& db_name, 
                               const std::string& table_name, 
                               int key, Record& record) {
    std::string file_path = "./data/" + db_name + "/" + table_name + ".dat";
    std::cout << "Getting record with key " << key << " from " << file_path << std::endl;
    
    // Read all records
    std::vector<Record> all_records = getAllRecords(file_path);
    
    // Try to find the record with the matching key
    for (const auto& rec : all_records) {
        if (!rec.values.empty()) {
            try {
                if (std::stoi(rec.values[0]) == key) {
                    record = rec;
                    std::cout << "Found record with key " << key << std::endl;
                    return true;
                }
            } catch (...) {
                // Skip if not a valid integer
            }
        }
    }
    
    std::cout << "Record with key " << key << " not found" << std::endl;
    return false;
}

bool StorageManager::writeAllRecords(const std::string& filename, 
                                   const std::vector<Record>& records) {
    try {
        // Create or truncate the file
        std::ofstream file(filename, std::ios::binary | std::ios::trunc);
        if (!file) {
            std::cerr << "Failed to open file for writing: " << filename << std::endl;
            return false;
        }
        file.close();

        // Write records page by page
        Page page;
        size_t offset = 0;
        size_t current_page = 0;
        
        for (const auto& record : records) {
            size_t record_size = record.getSize();
            
            // If this record won't fit in current page, write current page and start new one
            if (offset + sizeof(size_t) + record_size > PAGE_SIZE_BYTES) {
                page.setFreeSpace(PAGE_SIZE_BYTES - offset);
                if (!writePage(filename, current_page, page)) {
                    std::cerr << "Failed to write page " << current_page << std::endl;
                    return false;
                }
                
                // Start new page
                page.clear();
                offset = 0;
                current_page++;
            }
            
            // Write record size
            page.writeData(offset, &record_size, sizeof(size_t));
            offset += sizeof(size_t);
            
            // Write record data
            char buffer[PAGE_SIZE_BYTES];
            record.serialize(buffer);
            page.writeData(offset, buffer, record_size);
            offset += record_size;
        }

        // Write final page if it contains any data
        if (offset > 0) {
            page.setFreeSpace(PAGE_SIZE_BYTES - offset);
            if (!writePage(filename, current_page, page)) {
                std::cerr << "Failed to write final page" << std::endl;
                return false;
            }
        }

        return true;
    } catch (const std::exception& e) {
        std::cerr << "Error writing records: " << e.what() << std::endl;
        return false;
    }
}
