#pragma once
#include <string>
#include <vector>
#include <fstream>
#include <iostream>
#include <cstring>
#include "page.h"
#include "record.h"

const size_t PAGE_SIZE = 4096;  // 4KB pages

class StorageManager {
public:
    StorageManager();
    ~StorageManager();

    bool createTable(const std::string& db_name, const std::string& table_name);
    bool dropTable(const std::string& db_name, const std::string& table_name);
    bool insertRecord(const std::string& db_name, const std::string& table_name, const Record& record);
    bool getRecord(const std::string& db_name, const std::string& table_name, int key, Record& record);
    std::vector<Record> getAllRecords(const std::string& file_path);
    std::string getTablePath(const std::string& db_name, const std::string& table_name) const;
    
    // Add missing methods
    bool updateRecord(const std::string& db_name, const std::string& table_name, const Record& old_record, const Record& new_record);
    bool deleteRecord(const std::string& db_name, const std::string& table_name, const Record& record);
    std::vector<Record> selectRecords(const std::string& db_name, const std::string& table_name, const std::string& condition);
    
    // Add page management methods
    bool writePage(const std::string& filename, int page_id, const Page& page);
    bool readPage(const std::string& filename, int page_id, Page& page);
    bool writeAllRecords(const std::string& filename, const std::vector<Record>& records);
    
    // Add database management methods
    bool createDatabase(const std::string& db_name);
    std::string getIndexPath(const std::string& db_name, const std::string& index_name) const;
    bool createFile(const std::string& filename);
    
    // Add helper methods
    bool compareValues(const std::string& record_value, const std::string& search_value, const std::string& op);

private:
    static const int PAGE_SIZE_BYTES = 4096;
    static const int MAX_RECORDS_PER_PAGE = 100;
}; 