#pragma once
#include <string>
#include <vector>
#include "storage_manager.h"
#include "catalog_manager.h"

// Simple index record structure
struct IndexRecord {
    int key;
    int page_id;
};

class IndexManager {
public:
    IndexManager(StorageManager* storage_manager);
    ~IndexManager();

    bool createIndex(const std::string& db_name,
                    const std::string& table_name,
                    const std::string& column_name);
    bool dropIndex(const std::string& db_name,
                  const std::string& table_name,
                  const std::string& column_name);
    bool insert(const std::string& index_file, int key, const Record& record);
    bool exists(const std::string& index_file, int key);
    std::vector<Record> search(const std::string& index_file, int key);
    bool search(const std::string& index_file,
                const std::string& op,
                int value,
                std::vector<int>& result);
    bool remove(const std::string& index_file, int key);

private:
    StorageManager* storage_manager;
    
    // Helper methods
    bool writeIndexRecord(const std::string& index_name, const IndexRecord& record);
    bool readIndexRecords(const std::string& index_name, std::vector<IndexRecord>& records);
    void sortIndexRecords(std::vector<IndexRecord>& records);
    void searchEqual(const std::vector<IndexRecord>& records,
                    int value,
                    std::vector<int>& result);
    
    void searchGreaterThan(const std::vector<IndexRecord>& records,
                          int value,
                          std::vector<int>& result,
                          bool include_equal);
    
    void searchLessThan(const std::vector<IndexRecord>& records,
                       int value,
                       std::vector<int>& result,
                       bool include_equal);
    
    void searchNotEqual(const std::vector<IndexRecord>& records,
                       int value,
                       std::vector<int>& result);
};