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

    bool createIndex(const std::string& table_name, 
                    const std::string& column_name,
                    TableInfo* table_info);
    bool dropIndex(const std::string& table_name, 
                  const std::string& column_name);
    bool insertEntry(const std::string& index_name, int key, int page_id);
    int searchEntry(const std::string& index_name, int key);
    bool exists(const std::string& index_name, int key);

private:
    StorageManager* storage_manager;
    
    // Helper methods
    bool writeIndexRecord(const std::string& index_name, const IndexRecord& record);
    bool readIndexRecords(const std::string& index_name, std::vector<IndexRecord>& records);
    void sortIndexRecords(std::vector<IndexRecord>& records);
}; 