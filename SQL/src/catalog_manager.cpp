#include "catalog_manager.h"
#include <fstream>
#include <sstream>
#include <iostream>
#include <unordered_map>
#include <algorithm>

CatalogManager::CatalogManager(const std::string& db_name) 
    : db_name(db_name), 
      catalog_file("./data/" + db_name + "/catalog.dat") {
    loadCatalog();
}

CatalogManager::~CatalogManager() {
    saveCatalog();
}

bool CatalogManager::createTable(const std::string& table_name,
                               const std::vector<ColumnInfo>& columns) {
    // Check if table already exists
    if (tables.find(table_name) != tables.end()) {
        return false;
    }

    TableInfo table_info;
    table_info.name = table_name;
    table_info.columns = columns;
    table_info.data_file = "./data/" + db_name + "/" + table_name + ".dat";
    
    tables[table_name] = table_info;
    saveCatalog();
    return true;
}

bool CatalogManager::dropTable(const std::string& table_name) {
    auto it = tables.find(table_name);
    if (it != tables.end()) {
        tables.erase(it);
        saveCatalog();
        return true;
    }
    return false;
}

bool CatalogManager::addIndex(const std::string& table_name,
                            const std::string& column_name) {
    auto it = tables.find(table_name);
    if (it != tables.end()) {
        std::string index_file = table_name + "_" + column_name + ".idx";
        it->second.index_files.push_back(index_file);
        saveCatalog();
        return true;
    }
    return false;
}

TableInfo* CatalogManager::getTableInfo(const std::string& table_name) {
    auto it = tables.find(table_name);
    if (it != tables.end()) {
        return &(it->second);
    }
    return nullptr;
}

void CatalogManager::loadCatalog() {
    std::ifstream file(catalog_file);
    if (!file) return;

    std::string line;
    while (std::getline(file, line)) {
        std::istringstream iss(line);
        TableInfo table;
        
        // Read table name
        iss >> table.name;
        
        // Read columns
        int col_count;
        iss >> col_count;
        for (int i = 0; i < col_count; i++) {
            ColumnInfo col;
            iss >> col.name >> col.type >> col.size;
            table.columns.push_back(col);
        }
        
        // Read data file
        iss >> table.data_file;
        
        // Read index files
        int idx_count;
        iss >> idx_count;
        for (int i = 0; i < idx_count; i++) {
            std::string idx_file;
            iss >> idx_file;
            table.index_files.push_back(idx_file);
        }
        
        tables[table.name] = table;
    }
}

void CatalogManager::saveCatalog() {
    std::ofstream file(catalog_file);
    if (!file) return;

    for (const auto& [table_name, table] : tables) {
        file << table.name << " ";
        file << table.columns.size() << " ";
        
        for (const auto& col : table.columns) {
            file << col.name << " " 
                 << col.type << " " 
                 << col.size << " ";
        }
        
        file << table.data_file << " ";
        file << table.index_files.size() << " ";
        
        for (const auto& idx : table.index_files) {
            file << idx << " ";
        }
        
        file << "\n";
    }
}

bool CatalogManager::removeIndex(const std::string& table_name, 
                               const std::string& column_name) {
    auto it = tables.find(table_name);
    if (it != tables.end()) {
        std::string index_file = table_name + "_" + column_name + ".idx";
        auto& index_files = it->second.index_files;
        auto index_it = std::find(index_files.begin(), index_files.end(), index_file);
        if (index_it != index_files.end()) {
            index_files.erase(index_it);
            saveCatalog();
            return true;
        }
    }
    return false;
}