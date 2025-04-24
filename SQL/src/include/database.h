#pragma once
#include <string>
#include <vector>
#include "storage_manager.h"
#include "catalog_manager.h"
#include "index_manager.h"

class Database {
public:
    Database(const std::string& name);
    ~Database();

    bool createTable(const std::string& table_name, 
                    const std::vector<ColumnInfo>& columns);
    bool dropTable(const std::string& table_name);
    bool createIndex(const std::string& table_name, 
                    const std::string& column_name);
    bool dropIndex(const std::string& table_name, 
                  const std::string& column_name);
    bool insert(const std::string& table_name, 
               const std::vector<std::string>& values);
    std::vector<Record> select(const std::string& table_name, 
                             const std::string& where_clause);
    bool update(const std::string& table_name, 
               const std::string& set_clause,
               const std::string& where_clause);
    bool remove(const std::string& table_name, 
               const std::string& where_clause);

private:
    std::string db_name;
    std::string data_path;
    StorageManager* storage_manager;
    CatalogManager* catalog_manager;
    IndexManager* index_manager;
    
    void reloadCatalog();
}; 