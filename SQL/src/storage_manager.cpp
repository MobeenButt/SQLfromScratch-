#include "storage_manager.h"
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sys/stat.h>
#include <system_error>

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
    try {
        std::string db_path = "data/" + db_name;
    return fs::create_directories(db_path);
    } catch (const std::exception& e) {
        std::cerr << "Error creating database directory: " << e.what() << std::endl;
        return false;
    }
}

std::string StorageManager::getTablePath(const std::string& db_name, const std::string& table_name) const {
    return "./data/" + db_name + "/" + table_name + ".dat";
}

std::string StorageManager::getIndexPath(const std::string& db_name, const std::string& index_name) const {
    return "data/" + db_name + "/" + index_name;
}

bool StorageManager::createTable(const std::string& db_name, const std::string& table_name) {
    try {
        // Ensure database directory exists
        std::string db_path = "data/" + db_name;
        fs::create_directories(db_path);

        // Create the file path
        std::string filepath;
        if (table_name.size() >= 4 && table_name.substr(table_name.size()-4) == ".idx") {
            filepath = getIndexPath(db_name, table_name);
        } else {
            filepath = getTablePath(db_name, table_name);
        }

        // Create the file
        return createFile(filepath);
    } catch (const std::exception& e) {
        std::cerr << "Error in createTable: " << e.what() << std::endl;
        return false;
    }
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
    try {
        std::string filepath;
        if (table_name.size() >= 4 && table_name.substr(table_name.size()-4) == ".idx") {
            filepath = getIndexPath(db_name, table_name);
        } else {
            filepath = getTablePath(db_name, table_name);
        }
        
        return fs::remove(filepath);
    } catch (const std::exception& e) {
        std::cerr << "Error dropping table/index: " << e.what() << std::endl;
        return false;
    }
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
    try {
        std::string filename = getTablePath(db_name, table_name);
        std::fstream file(filename, std::ios::binary | std::ios::in | std::ios::out);
        if (!file) {
            std::cerr << "Failed to open file for inserting record: " << filename << std::endl;
            return false;
        }

        // Read pages until we find one with enough space
        Page page;
        int page_id = 0;
        bool found_space = false;

        while (readPage(filename, page_id, page)) {
            if (page.getFreeSpace() >= record.getSize()) {
                found_space = true;
                break;
            }
            page_id++;
        }

        if (!found_space) {
            // Create a new page if no space found
            page.clear();
            page.setFreeSpace(PAGE_SIZE_BYTES);
        }

        // Calculate offset for new record
        size_t offset = PAGE_SIZE_BYTES - page.getFreeSpace();
        
        // Serialize and write the record
        char buffer[PAGE_SIZE_BYTES];
        record.serialize(buffer);
        page.writeData(offset, buffer, record.getSize());
        page.setFreeSpace(page.getFreeSpace() - record.getSize());

        // Write the page back to disk
        return writePage(filename, page_id, page);
    } catch (const std::exception& e) {
        std::cerr << "Error inserting record: " << e.what() << std::endl;
        return false;
    }
}

std::vector<Record> StorageManager::getAllRecords(const std::string& filename) {
    std::vector<Record> records;
    try {
        std::cout << "Reading records from file: " << filename << std::endl;
        
        Page page;
        int page_id = 0;
        
        while (readPage(filename, page_id, page)) {
            size_t free_space = page.getFreeSpace();
            size_t used_space = PAGE_SIZE_BYTES - free_space;
            size_t offset = 0;

            std::cout << "Reading page " << page_id << " with " << used_space << " bytes used" << std::endl;

            while (offset < used_space) {
                Record record;
                char buffer[PAGE_SIZE_BYTES];
                
                if (!page.readData(offset, buffer, PAGE_SIZE_BYTES - offset)) {
                    break;
                }

                if (record.deserialize(buffer, PAGE_SIZE_BYTES - offset)) {
                    records.push_back(record);
                    offset += record.getSize();
                } else {
                    break;
                }
            }
            page_id++;
        }

        std::cout << "Total records read: " << records.size() << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "Error reading records: " << e.what() << std::endl;
    }
    return records;
}

bool StorageManager::getRecord(const std::string& db_name,
                             const std::string& table_name,
                             int key,
                             Record& record) {
    try {
        std::string filename = getTablePath(db_name, table_name);
        std::cout << "Getting record with key " << key << " from " << filename << std::endl;

        std::vector<Record> all_records = getAllRecords(filename);
        
        // Find record with matching key (assuming first column is key)
        for (const auto& r : all_records) {
            if (!r.values.empty()) {
                try {
                    if (std::stoi(r.values[0]) == key) {
                        record = r;
                        std::cout << "Found record with key " << key << std::endl;
                        return true;
                    }
                } catch (const std::exception& e) {
                    std::cerr << "Error converting key value: " << e.what() << std::endl;
                    continue;
                }
            }
        }
        
        std::cout << "Record with key " << key << " not found" << std::endl;
        return false;
    } catch (const std::exception& e) {
        std::cerr << "Error getting record: " << e.what() << std::endl;
        return false;
    }
}
