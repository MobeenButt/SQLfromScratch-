#include "catalog_manager.h"
#include <fstream>
#include <sstream>
#include <iostream>
#include <unordered_map>
#include <algorithm>
#include <filesystem>

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

bool CatalogManager::loadCatalog() {
    try {
        std::cout << "Loading catalog from: " << catalog_file << std::endl;
        std::ifstream file(catalog_file);
        if (!file) {
            std::cout << "No existing catalog file found" << std::endl;
            return true;
        }

        tables.clear();
        std::string line;

        // Read number of tables
        std::getline(file, line);
        int num_tables = std::stoi(line);
        std::cout << "Expected number of tables: " << num_tables << std::endl;

        for (int t = 0; t < num_tables; t++) {
            // Read table name
            std::getline(file, line);
            std::string table_name = line;
            std::cout << "Loading table: " << table_name << std::endl;

            TableInfo table;
            table.name = table_name;

            // Read number of columns
            std::getline(file, line);
            int num_columns = std::stoi(line);
            std::cout << "Number of columns: " << num_columns << std::endl;

            // Read columns
            for (int i = 0; i < num_columns; i++) {
                ColumnInfo col;
                std::getline(file, col.name);
                std::getline(file, col.type);
                std::getline(file, line); col.size = std::stoi(line);
                std::getline(file, line); col.is_primary_key = (line == "1");
                std::getline(file, line); col.is_foreign_key = (line == "1");

                if (col.is_foreign_key) {
                    std::getline(file, col.references_table);
                    std::getline(file, col.references_column);
                }

                table.columns.push_back(col);
                std::cout << "  Loaded column: " << col.name 
                         << " (Type: " << col.type 
                         << ", PK: " << col.is_primary_key 
                         << ", FK: " << col.is_foreign_key << ")" << std::endl;
            }

            // Set the data file path
            table.data_file = "./data/" + db_name + "/" + table_name + ".dat";
            
            // Add any index files
            if (table.columns.size() > 0) {
                for (const auto& col : table.columns) {
                    if (col.is_primary_key || col.is_foreign_key) {
                        std::string index_file = table_name + "_" + col.name + ".idx";
                        table.index_files.push_back(index_file);
                    }
                }
            }

            tables[table_name] = table;
        }

        std::cout << "Successfully loaded " << tables.size() << " tables from catalog" << std::endl;
        return true;
    } catch (const std::exception& e) {
        std::cerr << "Error loading catalog: " << e.what() << std::endl;
        return false;
    }
}

bool CatalogManager::saveCatalog() const {
    try {
        std::cout << "Saving catalog to: " << catalog_file << std::endl;
        
        // Create the directory if it doesn't exist
        std::filesystem::create_directories(std::filesystem::path(catalog_file).parent_path());
        
        std::ofstream file(catalog_file);
        if (!file) {
            std::cerr << "Failed to open catalog file for writing" << std::endl;
            return false;
        }

        // Write number of tables first
        file << tables.size() << "\n";

        for (const auto& [table_name, table] : tables) {
            std::cout << "Saving table: " << table_name << std::endl;
            
            // Write table name
            file << table_name << "\n";
            
            // Write columns
            file << table.columns.size() << "\n";
            for (const auto& col : table.columns) {
                file << col.name << "\n"
                     << col.type << "\n"
                     << col.size << "\n"
                     << (col.is_primary_key ? "1" : "0") << "\n"
                     << (col.is_foreign_key ? "1" : "0") << "\n";
                
                if (col.is_foreign_key) {
                    file << col.references_table << "\n"
                         << col.references_column << "\n";
                }
            }
        }
        
        std::cout << "Successfully saved catalog with " << tables.size() << " tables" << std::endl;
        return true;
    } catch (const std::exception& e) {
        std::cerr << "Error saving catalog: " << e.what() << std::endl;
        return false;
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

bool CatalogManager::validateForeignKeyReference(
    const std::string& /* foreign_table */,
    const std::string& /* foreign_column */,
    const std::string& primary_table,
    const std::string& primary_column) {
    
    // Check if referenced table exists
    auto primary_table_it = tables.find(primary_table);
    if (primary_table_it == tables.end()) {
        return false;
    }

    // Check if referenced column exists and is a primary key
    const auto& primary_columns = primary_table_it->second.columns;
    for (const auto& col : primary_columns) {
        if (col.name == primary_column) {
            return col.is_primary_key;
        }
    }
    
    return false;
}

std::string CatalogManager::getColumnName(const std::string& table_name, int column_index) const {
    auto it = tables.find(table_name);
    if (it != tables.end() && column_index >= 0 && 
        static_cast<size_t>(column_index) < it->second.columns.size()) {
        return it->second.columns[column_index].name;
    }
    return "";
}