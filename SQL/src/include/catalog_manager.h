#pragma once
#include <string>
#include <vector>
#include <unordered_map>

// Forward declarations
struct ColumnInfo {
    std::string name;
    std::string type;
    int size;
};

struct TableInfo {
    std::string name;
    std::vector<ColumnInfo> columns;
    std::string data_file;
    std::vector<std::string> index_files;
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

private:
    std::string db_name;  
    const std::string catalog_file;
    std::unordered_map<std::string, TableInfo> tables;
    
    void loadCatalog();
    void saveCatalog();
};