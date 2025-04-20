#include "storage_manager.h"
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sys/stat.h>
#include <system_error>

namespace fs = std::filesystem;

StorageManager::StorageManager(const std::string& data_dir) : data_dir(data_dir) {
    try {
        fs::create_directories(data_dir);
    } catch (const std::exception& e) {
        std::cerr << "Error creating data directory: " << e.what() << std::endl;
    }
}

StorageManager::~StorageManager() {
    // No file handle member, so nothing to close
}

bool StorageManager::createDatabase(const std::string& db_name) {
    try {
        std::string db_path = data_dir + "/" + db_name;
        return fs::create_directories(db_path);
    } catch (const std::exception& e) {
        std::cerr << "Error creating database directory: " << e.what() << std::endl;
        return false;
    }
}

std::string StorageManager::getTablePath(const std::string& db_name, const std::string& table_name) const {
    return data_dir + "/" + db_name + "/" + table_name + ".dat";
}

std::string StorageManager::getIndexPath(const std::string& db_name, const std::string& index_name) const {
    return data_dir + "/" + db_name + "/" + index_name;
}

bool StorageManager::createTable(const std::string& db_name, const std::string& table_name) {
    try {
        // Ensure database directory exists
        std::string db_path = data_dir + "/" + db_name;
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
        empty_page.free_space = PAGE_SIZE;
        memset(empty_page.data, 0, PAGE_SIZE);
        
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

bool StorageManager::writePage(const std::string& filename, int page_id, Page& page) {
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