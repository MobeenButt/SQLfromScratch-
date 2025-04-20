#pragma once
#include <string>
#include "page.h"

class StorageManager {
public:
    StorageManager(const std::string& data_dir);
    ~StorageManager();

    bool createDatabase(const std::string& db_name);
    bool createTable(const std::string& db_name, const std::string& table_name);
    bool dropTable(const std::string& db_name, const std::string& table_name);
    bool readPage(const std::string& filename, int page_id, Page& page);
    bool writePage(const std::string& filename, int page_id, Page& page);
    
    // Add these methods if they don't exist
    std::string getTablePath(const std::string& db_name, const std::string& table_name) const;
    std::string getIndexPath(const std::string& db_name, const std::string& index_name) const;

private:
    std::string data_dir;
    bool createFile(const std::string& filename);
}; 