#include "database.h"
#include <iostream>
#include <sstream>
#include <algorithm>
#include <cstring>  // for memset
#include <unordered_set>
#include "page.h"
#include "catalog_manager.h" // Add include for CatalogManager

// Forward declare helper function
std::vector<std::string> readRecord(const char* data, const std::vector<ColumnInfo>& columns);

Database::Database(const std::string& name) : db_name(name) {
    try {
        storage_manager = new StorageManager();
        catalog_manager = new CatalogManager(name);
        index_manager = new IndexManager(storage_manager);
        
        // Create database directory if it doesn't exist
    storage_manager->createDatabase(name);
    
        // Load the catalog
        reloadCatalog();
        
        std::cout << "Database initialized: " << name << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "Error initializing database: " << e.what() << std::endl;
        throw;
    }
}

Database::~Database() {
    delete storage_manager;
    delete catalog_manager;
    delete index_manager;
}

void Database::reloadCatalog() {
    try {
        // Force reload of catalog
        delete catalog_manager;
        catalog_manager = new CatalogManager(db_name);
        std::cout << "Catalog reloaded for database: " << db_name << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "Error reloading catalog: " << e.what() << std::endl;
        throw;
    }
}

bool Database::createTable(const std::string& table_name, 
                         const std::vector<ColumnInfo>& columns) {
    if (!storage_manager->createTable(db_name, table_name)) {
        return false;
    }

    // Create indexes for primary and foreign keys
    for (const auto& col : columns) {
        if (col.is_primary_key) {
            if (!index_manager->createIndex(db_name, table_name, col.name)) {
                return false;
            }
        }
        if (col.is_foreign_key) {
            if (!index_manager->createIndex(db_name, table_name, col.name)) {
                return false;
            }
        }
    }

    return catalog_manager->createTable(table_name, columns);
}

bool Database::dropTable(const std::string& table_name) {
    TableInfo* table = catalog_manager->getTableInfo(table_name);
    if (!table) return false;

    // Drop the table file
    if (!storage_manager->dropTable(db_name, table_name)) {
        return false;
    }

    // Drop associated indexes
    for (const auto& index_file : table->index_files) {
        storage_manager->dropTable(db_name, index_file);
    }

    return catalog_manager->dropTable(table_name);
}

bool Database::createIndex(const std::string& table_name, 
                         const std::string& column_name) {
    TableInfo* table = catalog_manager->getTableInfo(table_name);
    if (!table) {
        std::cerr << "Table not found: " << table_name << std::endl;
        return false;
    }

    // Check if column exists
    bool column_found = false;
    for (const auto& col : table->columns) {
        if (col.name == column_name) {
            column_found = true;
            break;
        }
    }

    if (!column_found) {
        std::cerr << "Column not found: " << column_name << std::endl;
        return false;
    }

    // Create the index
    if (!index_manager->createIndex(db_name, table_name, column_name)) {
        std::cerr << "Failed to create index" << std::endl;
        return false;
    }

    // Add index file to table info
    std::string index_file = table_name + "_" + column_name + ".idx";
    table->index_files.push_back(index_file);

    return true;
}

bool Database::insert(const std::string& table_name, 
                     const std::vector<std::string>& values) {
    TableInfo* table = catalog_manager->getTableInfo(table_name);
    if (!table || values.size() != table->columns.size()) {
        std::cerr << "Invalid table or number of values" << std::endl;
        return false;
    }

    // Check and update indexes
    for (size_t i = 0; i < table->columns.size(); i++) {
        const ColumnInfo& col = table->columns[i];
        if (col.is_primary_key) {
            try {
                int key = std::stoi(values[i]);
                std::string index_file = db_name + "/" + table_name + "_" + col.name + ".idx";
                std::cout << "Checking primary key constraint in: " << index_file << std::endl;
                
                if (index_manager->exists(index_file, key)) {
                    std::cerr << "Error: Duplicate primary key value: " << key << std::endl;
                    return false;
                }
            } catch (...) {
                std::cerr << "Error: Invalid primary key value" << std::endl;
                return false;
            }
        }
    }

    // Insert the record
    Record record;
    record.values = values;
    if (!storage_manager->insertRecord(db_name, table_name, record)) {
        std::cerr << "Failed to insert record into table" << std::endl;
        return false;
    }

    // Update all relevant indexes
    for (size_t i = 0; i < table->columns.size(); i++) {
        const ColumnInfo& col = table->columns[i];
        if (col.is_primary_key) {
            try {
                int key = std::stoi(values[i]);
                std::string index_file = db_name + "/" + table_name + "_" + col.name + ".idx";
                std::cout << "Updating index: " << index_file << std::endl;
                
                if (!index_manager->insert(index_file, key, record)) {
                    std::cerr << "Failed to update index: " << index_file << std::endl;
                    // You might want to roll back the record insertion here
                    return false;
                }
            } catch (const std::exception& e) {
                std::cerr << "Error updating index: " << e.what() << std::endl;
                    return false;
            }
        }
    }

    return true;
}

// Simple select without conditions
std::vector<Record> Database::select(const std::string& table_name,
                                   const std::string& where_clause) {
    std::vector<Record> result;
    if (where_clause.empty()) {
        // Simple SELECT * case
        result = storage_manager->getAllRecords(db_name + "/" + table_name);
        
        // Display results
    TableInfo* table = catalog_manager->getTableInfo(table_name);
        if (table) {
            std::cout << "\nQuery Results (" << result.size() << " records):\n";
            std::cout << "----------------------------------------\n";
            
            // Print header
            for (const auto& col : table->columns) {
                std::cout << std::setw(15) << std::left << col.name;
            }
            std::cout << "\n----------------------------------------\n";

            // Print records
            for (const auto& record : result) {
                for (const auto& value : record.values) {
                    std::cout << std::setw(15) << std::left << value;
                }
                std::cout << "\n";
            }
            std::cout << "----------------------------------------\n";
        }
    }
    return result;
}

// Select with conditions
bool Database::selectWithCondition(const std::string& table_name,
                                 const std::string& column_name,
                                 const std::string& op,
                                 const std::string& value,
                                 std::vector<Record>& result) {
    try {
        TableInfo* table = catalog_manager->getTableInfo(table_name);
        if (!table) {
            std::cerr << "Table not found: " << table_name << std::endl;
            return false;
        }

        // Find the column index and type
        int col_index = -1;
        for (size_t i = 0; i < table->columns.size(); i++) {
            if (table->columns[i].name == column_name) {
                col_index = i;
                break;
            }
        }

        if (col_index == -1) {
            std::cerr << "Column not found: " << column_name << std::endl;
            return false;
        }

        result.clear();  // Clear any existing results

        // Check if we can use an index for this query
        bool has_index = table->columns[col_index].is_primary_key;
        std::string index_file = db_name + "/" + table_name + "_" + column_name + ".idx";

        bool success;
        if (has_index) {
            std::cout << "Using index for search on " << column_name << std::endl;
            success = selectUsingIndex(table_name, index_file, op, value, result);
        } else {
            std::cout << "Performing full table scan on " << table_name << std::endl;
            success = selectUsingTableScan(table_name, col_index, op, value, result);
        }

        // Display results
        if (success) {
            std::cout << "\nQuery Results (" << result.size() << " records):\n";
            std::cout << "----------------------------------------\n";
            
            // Print header
            for (const auto& col : table->columns) {
                std::cout << std::setw(15) << std::left << col.name;
            }
            std::cout << "\n----------------------------------------\n";

            // Print records
            for (const auto& record : result) {
                for (const auto& value : record.values) {
                    std::cout << std::setw(15) << std::left << value;
                }
                std::cout << "\n";
            }
            std::cout << "----------------------------------------\n";
        }

        return success;
    } catch (const std::exception& e) {
        std::cerr << "Error in select: " << e.what() << std::endl;
        return false;
    }
}

// Helper method to read a record
std::vector<std::string> readRecord(const char* data, const std::vector<ColumnInfo>& columns) {
    std::vector<std::string> record;
    size_t offset = 0;
    
    for (const auto& col : columns) {
        if (col.type == "INT") {
            int val;
            memcpy(&val, data + offset, sizeof(int));
            record.push_back(std::to_string(val));
            offset += sizeof(int);
        } else if (col.type == "VARCHAR") {
            std::string str(data + offset, col.size);
            // Remove null termination and trailing spaces
            str.erase(std::find(str.begin(), str.end(), '\0'), str.end());
            record.push_back(str);
            offset += col.size;
        }
    }
    
    return record;
}

bool Database::update(const std::string& table_name, 
                     const std::string& set_clause,
                     const std::string& where_clause) {
    std::vector<Record> records_to_update;
    if (!selectWithCondition(table_name, "id", "=", where_clause, records_to_update)) {
        return false;
    }

    TableInfo* table = catalog_manager->getTableInfo(table_name);
    if (!table) {
        return false;
    }

    // Parse set clause (format: "column_name = new_value")
    std::istringstream set_iss(set_clause);
    std::string update_col, equals, new_value;
    set_iss >> update_col >> equals >> new_value;

    if (equals != "=") return false;

    // Find update column index and validate new value
    int update_col_idx = -1;
    for (size_t i = 0; i < table->columns.size(); i++) {
        if (table->columns[i].name == update_col) {
            update_col_idx = i;
            // Validate new value type
            if (table->columns[i].type == "INT") {
                try {
                    std::stoi(new_value);
                } catch (...) {
                    return false;
                }
            } else if (table->columns[i].type == "VARCHAR") {
                if (new_value.length() > static_cast<size_t>(table->columns[i].size)) {
                    return false;
                }
            }
            break;
        }
    }

    if (update_col_idx == -1) return false;

    // Update records
    Page page;
    int page_id = 0;
    bool success = true;

    while (storage_manager->readPage(table->data_file, page_id, page)) {
        bool page_modified = false;
        size_t offset = 0;

        while (offset < static_cast<size_t>(PAGE_SIZE_BYTES - page.getFreeSpace())) {
            Record current_record;
            page.readData(offset, &current_record, sizeof(Record));

            // Check if this record matches any record to update
            for (const auto& record : records_to_update) {
                if (record.values == current_record.values) {
                    // Update the record
                    Record updated_record;
                    updated_record.values = record.values;
                    if (table->columns[update_col_idx].type == "INT") {
                        int new_val = std::stoi(new_value);
                        updated_record.values[update_col_idx] = std::to_string(new_val);
                    }
                    else if (table->columns[update_col_idx].type == "VARCHAR") {
                        std::string padded_value = new_value;
                        padded_value.append(
                            table->columns[update_col_idx].size - new_value.length(), 
                            '\0');
                        updated_record.values[update_col_idx] = padded_value;
                    }
                    page.writeData(offset, &updated_record, sizeof(Record));
                    page_modified = true;
                }
            }

            offset += sizeof(Record);
        }

        if (page_modified) {
            if (!storage_manager->writePage(table->data_file, page_id, page)) {
                success = false;
                break;
            }
        }
        page_id++;
    }

    return success;
}

bool Database::remove(const std::string& table_name, 
                     const std::string& where_clause) {
    std::vector<Record> records_to_delete;
    if (!selectWithCondition(table_name, "id", "=", where_clause, records_to_delete)) {
        return false;
    }

    TableInfo* table = catalog_manager->getTableInfo(table_name);
    if (!table) {
        return false;
    }

    Page page;
    int page_id = 0;
    bool success = true;

    while (storage_manager->readPage(table->data_file, page_id, page)) {
        bool page_modified = false;
        size_t read_offset = 0;
        size_t write_offset = 0;

        while (read_offset < static_cast<size_t>(PAGE_SIZE_BYTES - page.getFreeSpace())) {
            Record current_record;
            page.readData(read_offset, &current_record, sizeof(Record));

            // Check if this record should be deleted
            bool should_delete = false;
            for (const auto& record : records_to_delete) {
                if (record.values == current_record.values) {
                    should_delete = true;
                    break;
                }
            }

            if (!should_delete) {
                // Keep this record by moving it to write_offset if necessary
                if (write_offset != read_offset) {
                    page.moveData(write_offset, read_offset, sizeof(Record));
                    page_modified = true;
                }
                write_offset += sizeof(Record);
            } else {
                page_modified = true;
            }

            read_offset += sizeof(Record);
        }

        if (page_modified) {
            // Update free space
            page.setFreeSpace(PAGE_SIZE_BYTES - write_offset);
            
            // Clear the rest of the page
            if (write_offset < PAGE_SIZE_BYTES) {
                memset(page.getData() + write_offset, 0, PAGE_SIZE_BYTES - write_offset);
            }

            if (!storage_manager->writePage(table->data_file, page_id, page)) {
                success = false;
                break;
            }
        }
        page_id++;
    }

    return success;
}

void updateRecord(Page& page, size_t offset, const Record& new_record) {
    size_t record_size = sizeof(Record);
    page.writeData(offset, &new_record, record_size);
    page.setNumKeys(page.getNumKeys() + 1);
}

void removeRecord(Page& page, size_t offset, size_t record_size) {
    size_t remaining = page.getFreeSpace() - (offset + record_size);
    if (remaining > 0) {
        page.moveData(offset, offset + record_size, remaining);
    }
    
    page.setFreeSpace(page.getFreeSpace() + record_size);
    memset(page.getData() + (PAGE_SIZE_BYTES - page.getFreeSpace()), 
           0, 
           record_size);
    
    page.setNumKeys(page.getNumKeys() - 1);
}

bool Database::selectUsingIndex(const std::string& table_name,
                              const std::string& index_file,
                              const std::string& op,
                              const std::string& value,
                              std::vector<Record>& result) {
    try {
        // Add ./data/ prefix to index file path
        std::string full_index_path = "./data/" + index_file;
        std::cout << "Using index file: " << full_index_path << std::endl;

        std::vector<int> matching_keys;
        if (!index_manager->search(full_index_path, op, std::stoi(value), matching_keys)) {
            std::cerr << "Index search failed" << std::endl;
            return false;
        }

        std::cout << "Found " << matching_keys.size() << " matching keys" << std::endl;

        // Retrieve records for matching keys
        for (int key : matching_keys) {
            Record record;
            if (storage_manager->getRecord(db_name, table_name, key, record)) {
                result.push_back(record);
                std::cout << "Added record with key " << key << " to results" << std::endl;
            }
        }

        return true;
    } catch (const std::exception& e) {
        std::cerr << "Error in index search: " << e.what() << std::endl;
        return false;
    }
}

bool Database::selectUsingTableScan(const std::string& table_name,
                                  int col_index,
                                  const std::string& op,
                                  const std::string& value,
                                  std::vector<Record>& result) {
    try {
        std::string data_file = getTablePath(table_name);
        std::cout << "Performing table scan on: " << data_file << std::endl;

        std::vector<Record> all_records = storage_manager->getAllRecords(data_file);
        
        for (const Record& record : all_records) {
            if (col_index >= 0 && col_index < static_cast<int>(record.values.size())) {
                if (compareValues(record.values[col_index], value, op)) {
                    result.push_back(record);
                }
            }
        }

        std::cout << "Found " << result.size() << " matching records" << std::endl;
        return true;
    } catch (const std::exception& e) {
        std::cerr << "Error in table scan: " << e.what() << std::endl;
        return false;
    }
}

bool Database::compareValues(const std::string& record_value,
                           const std::string& search_value,
                           const std::string& op) {
    try {
        // Try numeric comparison first
        int record_num = std::stoi(record_value);
        int search_num = std::stoi(search_value);

        if (op == "=") return record_num == search_num;
        if (op == ">") return record_num > search_num;
        if (op == "<") return record_num < search_num;
        if (op == ">=") return record_num >= search_num;
        if (op == "<=") return record_num <= search_num;
        if (op == "!=") return record_num != search_num;
    } catch (...) {
        // Fall back to string comparison
        if (op == "=") return record_value == search_value;
        if (op == ">") return record_value > search_value;
        if (op == "<") return record_value < search_value;
        if (op == ">=") return record_value >= search_value;
        if (op == "<=") return record_value <= search_value;
        if (op == "!=") return record_value != search_value;
    }

    return false;
}

// Helper method to get the full table path
std::string Database::getTablePath(const std::string& table_name) {
    return "./data/" + db_name + "/" + table_name + ".dat";
} 