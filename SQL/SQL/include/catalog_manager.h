#pragma once
#include <string>
#include <vector>
#include <unordered_map>
#include <memory>
#include "page.h"

// Forward declarations
struct ColumnInfo {
    std::string name;
    std::string type;
    int size;
    bool is_primary_key;
    bool is_foreign_key;
    std::string references_table;
    std::string references_column;

    ColumnInfo() : size(0), is_primary_key(false), is_foreign_key(false) {}
};

struct TableInfo {
    std::string name;
    std::vector<ColumnInfo> columns;
    std::string data_file;
    std::vector<std::string> index_files;
    std::string primary_key_column;  // Store the name of primary key column
    std::string primary_key;
    std::vector<std::string> foreign_keys;
};

class CatalogManager {
public:
    CatalogManager(const std::string& db_name);
    ~CatalogManager();

    bool createTable(const std::string& table_name, 
                    const std::vector<ColumnInfo>& columns);
    bool dropTable(const std::string& table_name);
    bool addIndex(const std::string& table_name, 
                 const std::string& column_name);
    bool removeIndex(const std::string& table_name, 
                    const std::string& column_name);
    TableInfo* getTableInfo(const std::string& table_name);
    bool validateForeignKeyReference(const std::string& foreign_table,
                                   const std::string& foreign_column,
                                   const std::string& primary_table,
                                   const std::string& primary_column);
    bool tableExists(const std::string& table_name) const;
    const TableInfo* getTableInfo(const std::string& table_name) const;
    std::string getColumnName(const std::string& table_name, int column_index) const;
    bool saveCatalog() const;
    bool loadCatalog();
    void printCatalog() const;

private:
    std::string db_name;  
    const std::string catalog_file;
    std::unordered_map<std::string, TableInfo> tables;
    
    std::string getCatalogPath() const;
};