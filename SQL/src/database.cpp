#include "database.h"
#include <iostream>
#include <sstream>
#include <algorithm>
#include <cstring>  // for memset
#include <unordered_set>
#include "page.h"
#include "catalog_manager.h"

// Forward declarations
size_t getRecordSize(const Record& record);
std::string getIndexPath(const std::string& db_name, const std::string& table_name, const std::string& column_name);

// Forward declare helper function
std::vector<std::string> readRecord(const char* data, const std::vector<ColumnInfo>& columns);

// Add these helper functions at the top of database.cpp
size_t getRecordSize(const Record& record) {
    size_t size = 0;
    size += sizeof(size_t);  // For number of values
    
    for (const auto& value : record.values) {
        size += sizeof(size_t);  // For string length
        size += value.length();  // For string content
    }
    return size;
}

bool serializeRecord(const Record& record, char* buffer, size_t buffer_size) {
    size_t pos = 0;
    
    // Write number of values
    size_t num_values = record.values.size();
    if (pos + sizeof(size_t) > buffer_size) return false;
    memcpy(buffer + pos, &num_values, sizeof(size_t));
    pos += sizeof(size_t);
    
    // Write each value
    for (const auto& value : record.values) {
        // Write string length
        size_t len = value.length();
        if (pos + sizeof(size_t) > buffer_size) return false;
        memcpy(buffer + pos, &len, sizeof(size_t));
        pos += sizeof(size_t);
        
        // Write string content
        if (pos + len > buffer_size) return false;
        memcpy(buffer + pos, value.c_str(), len);
        pos += len;
    }
    
    return true;
}

bool deserializeRecord(Record& record, const char* buffer, size_t buffer_size) {
    size_t pos = 0;
    
    // Read number of values
    size_t num_values;
    if (pos + sizeof(size_t) > buffer_size) return false;
    memcpy(&num_values, buffer + pos, sizeof(size_t));
    pos += sizeof(size_t);
    
    record.values.clear();
    record.values.reserve(num_values);
    
    // Read each value
    for (size_t i = 0; i < num_values; i++) {
        // Read string length
        size_t len;
        if (pos + sizeof(size_t) > buffer_size) return false;
        memcpy(&len, buffer + pos, sizeof(size_t));
        pos += sizeof(size_t);
        
        // Read string content
        if (pos + len > buffer_size) return false;
        record.values.push_back(std::string(buffer + pos, len));
        pos += len;
    }
    
    return true;
}

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
        std::string data_file = getTablePath(table_name);
        result = storage_manager->getAllRecords(data_file);
        
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
    try {
        // Parse where clause to get the value
        std::istringstream where_iss(where_clause);
        std::string column, op, value;
        where_iss >> column >> op >> value;
        
        // Remove any quotes from the value if present
        if (!value.empty() && (value[0] == '\'' || value[0] == '"')) {
            value = value.substr(1, value.length() - 2);
        }
        
        std::vector<Record> records_to_update;
        if (!selectWithCondition(table_name, column, op, value, records_to_update)) {
            std::cerr << "Failed to find records to update" << std::endl;
            return false;
        }

        TableInfo* table = catalog_manager->getTableInfo(table_name);
        if (!table) {
            std::cerr << "Table not found" << std::endl;
            return false;
        }

        // Parse set clause (format: "column_name = new_value")
        std::istringstream set_iss(set_clause);
        std::string update_col, equals, new_value;
        set_iss >> update_col >> equals >> new_value;

        if (equals != "=") {
            std::cerr << "Invalid SET clause format" << std::endl;
            return false;
        }

        // Remove any quotes from the new value if present
        if (!new_value.empty() && (new_value[0] == '\'' || new_value[0] == '"')) {
            new_value = new_value.substr(1, new_value.length() - 2);
        }

        // Find update column index
        int update_col_idx = -1;
        for (size_t i = 0; i < table->columns.size(); i++) {
            if (table->columns[i].name == update_col) {
                update_col_idx = i;
                // Validate new value type
                if (table->columns[i].type == "INT") {
                    try {
                        std::stoi(new_value);
                    } catch (...) {
                        std::cerr << "Invalid integer value" << std::endl;
                        return false;
                    }
                } else if (table->columns[i].type == "VARCHAR") {
                    if (new_value.length() > static_cast<size_t>(table->columns[i].size)) {
                        std::cerr << "String value too long" << std::endl;
                        return false;
                    }
                }
                break;
            }
        }

        if (update_col_idx == -1) {
            std::cerr << "Update column not found" << std::endl;
            return false;
        }

        std::string data_file = getTablePath(table_name);
        std::vector<Record> all_records = storage_manager->getAllRecords(data_file);
        
        // Update matching records
        for (auto& record : all_records) {
            for (const auto& update_record : records_to_update) {
                if (record.values == update_record.values) {
                    // Update the value
                    record.values[update_col_idx] = new_value;
                }
            }
        }

        // Write back all records
        Page page;
        size_t offset = 0;
        
        for (const auto& record : all_records) {
            if (offset + sizeof(size_t) + record.getSize() > PAGE_SIZE_BYTES) {
                // Write current page and create new one
                if (!storage_manager->writePage(data_file, 0, page)) {
                    std::cerr << "Failed to write page during update" << std::endl;
                    return false;
                }
                page.clear();
                offset = 0;
            }
            
            // Write record size
            size_t record_size = record.getSize();
            page.writeData(offset, &record_size, sizeof(size_t));
            offset += sizeof(size_t);
            
            // Write record data
            char buffer[PAGE_SIZE_BYTES];
            record.serialize(buffer);
            page.writeData(offset, buffer, record_size);
            offset += record_size;
        }

        // Write final page
        page.setFreeSpace(PAGE_SIZE_BYTES - offset);
        if (!storage_manager->writePage(data_file, 0, page)) {
            std::cerr << "Failed to write final page during update" << std::endl;
            return false;
        }

        // Update indexes if necessary
        if (table->columns[update_col_idx].is_primary_key) {
            std::string index_file = getIndexPath(db_name, table_name, update_col);
            for (const auto& record : records_to_update) {
                try {
                    int old_key = std::stoi(record.values[update_col_idx]);
                    int new_key = std::stoi(new_value);
                    
                    if (!index_manager->remove(index_file, old_key)) {
                        std::cerr << "Failed to remove old index entry" << std::endl;
                        return false;
                    }
                    
                    Record updated_record = record;
                    updated_record.values[update_col_idx] = new_value;
                    if (!index_manager->insert(index_file, new_key, updated_record)) {
                        std::cerr << "Failed to insert new index entry" << std::endl;
                        return false;
                    }
                } catch (const std::exception& e) {
                    std::cerr << "Error updating index: " << e.what() << std::endl;
                    return false;
                }
            }
        }

        return true;
    } catch (const std::exception& e) {
        std::cerr << "Error in update: " << e.what() << std::endl;
        return false;
    }
}

bool Database::remove(const std::string& table_name, 
                     const std::string& where_clause) {
    try {
        // Parse where clause to get the value
        std::istringstream where_iss(where_clause);
        std::string column, op, value;
        where_iss >> column >> op >> value;
        
        // Remove any quotes from the value if present
        if (!value.empty() && (value[0] == '\'' || value[0] == '"')) {
            value = value.substr(1, value.length() - 2);
        }
        
        std::vector<Record> records_to_delete;
        if (!selectWithCondition(table_name, column, op, value, records_to_delete)) {
            std::cerr << "Failed to find records to delete" << std::endl;
            return false;
        }

        TableInfo* table = catalog_manager->getTableInfo(table_name);
        if (!table) {
            std::cerr << "Table not found: " << table_name << std::endl;
            return false;
        }

        std::string data_file = getTablePath(table_name);
        std::vector<Record> all_records = storage_manager->getAllRecords(data_file);
        std::vector<Record> remaining_records;

        // Keep records that don't match deletion criteria
        for (const auto& record : all_records) {
            bool should_delete = false;
            for (const auto& del_record : records_to_delete) {
                if (record.values == del_record.values) {
                    should_delete = true;
                    break;
                }
            }
            if (!should_delete) {
                remaining_records.push_back(record);
            }
        }

        // Write back remaining records
        Page page;
        size_t offset = 0;
        
        for (const auto& record : remaining_records) {
            if (offset + sizeof(size_t) + record.getSize() > PAGE_SIZE_BYTES) {
                // Write current page and create new one
                if (!storage_manager->writePage(data_file, 0, page)) {
                    std::cerr << "Failed to write page during delete" << std::endl;
                    return false;
                }
                page.clear();
                offset = 0;
            }
            
            // Write record size
            size_t record_size = record.getSize();
            page.writeData(offset, &record_size, sizeof(size_t));
            offset += sizeof(size_t);
            
            // Write record data
            char buffer[PAGE_SIZE_BYTES];
            record.serialize(buffer);
            page.writeData(offset, buffer, record_size);
            offset += record_size;
        }

        // Write final page
        page.setFreeSpace(PAGE_SIZE_BYTES - offset);
        if (!storage_manager->writePage(data_file, 0, page)) {
            std::cerr << "Failed to write final page during delete" << std::endl;
            return false;
        }

        // Update indexes
        for (const auto& col : table->columns) {
            if (col.is_primary_key) {
                std::string index_file = getIndexPath(db_name, table_name, col.name);
                for (const auto& record : records_to_delete) {
                    try {
                        int key = std::stoi(record.values[0]);
                        if (!index_manager->remove(index_file, key)) {
                            std::cerr << "Failed to update index for key: " << key << std::endl;
                            return false;
                        }
                    } catch (const std::exception& e) {
                        std::cerr << "Error updating index: " << e.what() << std::endl;
                        return false;
                    }
                }
            }
        }

        return true;
    } catch (const std::exception& e) {
        std::cerr << "Error in remove: " << e.what() << std::endl;
        return false;
    }
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

std::string getIndexPath(const std::string& db_name, const std::string& table_name, const std::string& column_name) {
    return "./data/" + db_name + "/" + table_name + "_" + column_name + ".idx";
}

std::vector<Record> Database::join(
    const std::string& left_table,
    const std::string& right_table,
    const std::string& left_column,
    const std::string& right_column,
    JoinType join_type) {
    
    std::vector<Record> result;
    
    try {
        // Get table info
        TableInfo* left_info = catalog_manager->getTableInfo(left_table);
        TableInfo* right_info = catalog_manager->getTableInfo(right_table);
        
        if (!left_info || !right_info) {
            std::cerr << "One or both tables not found" << std::endl;
            return result;
        }

        // Find join column indices
        int left_col_idx = -1, right_col_idx = -1;
        if (!findJoinColumns(left_info, right_info, left_column, right_column, 
                           left_col_idx, right_col_idx)) {
            return result;
        }

        // Get all records from both tables
        std::vector<Record> left_records = storage_manager->getAllRecords(getTablePath(left_table));
        std::vector<Record> right_records = storage_manager->getAllRecords(getTablePath(right_table));

        // Create hash table for right table records
        std::unordered_multimap<std::string, Record> right_hash;
        for (const auto& right_record : right_records) {
            right_hash.insert({right_record.values[right_col_idx], right_record});
        }

        // Perform join based on join type
        for (const auto& left_record : left_records) {
            bool match_found = false;
            auto range = right_hash.equal_range(left_record.values[left_col_idx]);
            
            for (auto it = range.first; it != range.second; ++it) {
                // Create joined record
                Record joined_record;
                joined_record.values = left_record.values;
                joined_record.values.insert(
                    joined_record.values.end(),
                    it->second.values.begin(),
                    it->second.values.end()
                );
                result.push_back(joined_record);
                match_found = true;
            }

            // Handle outer joins
            if (!match_found && (join_type == JoinType::LEFT || 
                               join_type == JoinType::RIGHT)) {
                Record joined_record;
                joined_record.values = left_record.values;
                // Add NULL values for right table
                joined_record.values.insert(
                    joined_record.values.end(),
                    right_info->columns.size(),
                    "NULL"
                );
                result.push_back(joined_record);
            }
        }

        // Display results
        if (!result.empty()) {
            std::cout << "\nJoin Results (" << result.size() << " records):\n";
            std::cout << "----------------------------------------\n";
            
            // Print header
            for (const auto& col : left_info->columns) {
                std::cout << std::setw(15) << std::left << (left_table + "." + col.name);
            }
            for (const auto& col : right_info->columns) {
                std::cout << std::setw(15) << std::left << (right_table + "." + col.name);
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

    } catch (const std::exception& e) {
        std::cerr << "Error in join: " << e.what() << std::endl;
    }

    return result;
}

bool Database::findJoinColumns(
    const TableInfo* left_table,
    const TableInfo* right_table,
    const std::string& left_column,
    const std::string& right_column,
    int& left_col_idx,
    int& right_col_idx) {
    
    // Find left column index
    for (size_t i = 0; i < left_table->columns.size(); i++) {
        if (left_table->columns[i].name == left_column) {
            left_col_idx = i;
            break;
        }
    }

    // Find right column index
    for (size_t i = 0; i < right_table->columns.size(); i++) {
        if (right_table->columns[i].name == right_column) {
            right_col_idx = i;
            break;
        }
    }

    if (left_col_idx == -1 || right_col_idx == -1) {
        std::cerr << "Join columns not found" << std::endl;
        return false;
    }

    // Verify column types match
    if (left_table->columns[left_col_idx].type != 
        right_table->columns[right_col_idx].type) {
        std::cerr << "Join column types do not match" << std::endl;
        return false;
    }

    return true;
} 