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
                // Fix the path to include the proper prefix
                std::string index_file = "./data/" + db_name + "/" + table_name + "_" + col.name + ".idx";
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
                // Fix the path to include the proper prefix
                std::string index_file = "./data/" + db_name + "/" + table_name + "_" + col.name + ".idx";
                std::cout << "Updating index: " << index_file << std::endl;
                
                if (!index_manager->insert(index_file, key, record)) {
                    std::cerr << "Failed to update index: " << index_file << std::endl;
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
        // Parse WHERE clause
        std::istringstream where_iss(where_clause);
        std::string column, op, value;
        where_iss >> column >> op;
        std::getline(where_iss, value);
        value = value.substr(value.find_first_not_of(" \t"));
        if (!value.empty() && (value[0] == '\'' || value[0] == '"'))
            value = value.substr(1, value.length() - 2);

        // Find records to update
        std::vector<Record> records_to_update_metadata;
        if (!selectWithCondition(table_name, column, op, value, records_to_update_metadata)) {
            if (records_to_update_metadata.empty()) {
                std::cout << "No records matched the WHERE clause. No update performed." << std::endl;
                return true;
            }
            std::cerr << "Error during selectWithCondition for update." << std::endl;
            return false;
        }
        if (records_to_update_metadata.empty()) {
            std::cout << "No records matched the WHERE clause. No update performed." << std::endl;
            return true;
        }

        // Display query results
        std::cout << "\nQuery Results (" << records_to_update_metadata.size() << " records):" << std::endl;
        std::cout << "----------------------------------------" << std::endl;
        
        TableInfo* table = catalog_manager->getTableInfo(table_name);
        if (!table) return false;
        
        for (size_t i = 0; i < table->columns.size(); i++) {
            std::cout << std::left << std::setw(15) << table->columns[i].name;
        }
        std::cout << std::endl;
        std::cout << "----------------------------------------" << std::endl;
        
        for (const auto& record : records_to_update_metadata) {
            for (size_t i = 0; i < record.values.size(); i++) {
                std::string display_value = record.values[i];
                std::cout << std::left << std::setw(15) << display_value;
            }
            std::cout << std::endl;
        }
        std::cout << "----------------------------------------" << std::endl;

        // Parse SET clause
        size_t equals_pos = set_clause.find('=');
        if (equals_pos == std::string::npos) return false;
        std::string update_col_name = set_clause.substr(0, equals_pos);
        std::string new_value_str = set_clause.substr(equals_pos + 1);
        update_col_name.erase(0, update_col_name.find_first_not_of(" \t"));
        update_col_name.erase(update_col_name.find_last_not_of(" \t") + 1);
        new_value_str.erase(0, new_value_str.find_first_not_of(" \t"));
        new_value_str.erase(new_value_str.find_last_not_of(" \t") + 1);
        if (!new_value_str.empty() && (new_value_str[0] == '\'' || new_value_str[0] == '"'))
            new_value_str = new_value_str.substr(1, new_value_str.length() - 2);

        // Find column index
        int update_col_idx = -1;
        for (size_t i = 0; i < table->columns.size(); i++) {
            if (table->columns[i].name == update_col_name) {
                update_col_idx = i;
                break;
            }
        }
        if (update_col_idx == -1) return false;

        // Check if we're updating a primary key column
        bool updating_primary_key = table->columns[update_col_idx].is_primary_key;

        // Get the data file path
        std::string data_file = getTablePath(table_name);
        
        // Read all records first
        std::vector<Record> all_records;
        std::ifstream inFile(data_file, std::ios::binary);
        if (!inFile) {
            std::cerr << "Failed to open file: " << data_file << std::endl;
            return false;
        }
        
        // Get file size
        inFile.seekg(0, std::ios::end);
        size_t file_size = inFile.tellg();
        inFile.seekg(0, std::ios::beg);
        
        // Read all records from file
        if (file_size > 0) {
            std::vector<char> buffer(file_size);
            inFile.read(buffer.data(), file_size);
            
            size_t pos = 0;
            while (pos < file_size) {
                // Read record size
                size_t record_size;
                if (pos + sizeof(size_t) > file_size) break;
                memcpy(&record_size, buffer.data() + pos, sizeof(size_t));
                
                if (record_size == 0 || pos + record_size > file_size) break;
                
                // Create new record and deserialize
                Record record;
                
                // Manual deserialization
                size_t data_pos = pos + sizeof(size_t); // Skip record size
                
                // Read number of values
                size_t num_values;
                memcpy(&num_values, buffer.data() + data_pos, sizeof(size_t));
                data_pos += sizeof(size_t);
                
                // Read values
                for (size_t i = 0; i < num_values; i++) {
                    size_t str_len;
                    memcpy(&str_len, buffer.data() + data_pos, sizeof(size_t));
                    data_pos += sizeof(size_t);
                    
                    std::string value(buffer.data() + data_pos, str_len);
                    record.values.push_back(value);
                    data_pos += str_len;
                }
                
                all_records.push_back(record);
                pos += record_size;
            }
        }
        inFile.close();
        
        // Update matching records
        bool any_updated = false;
        for (auto& record : all_records) {
            for (const auto& meta_record : records_to_update_metadata) {
                if (record.values == meta_record.values) {
                    record.values[update_col_idx] = new_value_str;
                    any_updated = true;
                    break;
                }
            }
        }
        
        // Create temporary file and write all records
        std::string temp_file = data_file + ".tmp";
        std::ofstream outFile(temp_file, std::ios::binary);
        if (!outFile) {
            std::cerr << "Failed to create temporary file: " << temp_file << std::endl;
            return false;
        }
        
        // Write all records to the temporary file
        for (const auto& record : all_records) {
            // Calculate record size
            size_t total_size = sizeof(size_t); // Size field itself
            total_size += sizeof(size_t);       // Number of values
            
            for (const auto& value : record.values) {
                total_size += sizeof(size_t);   // String length
                total_size += value.length();   // String content
            }
            
            // Create buffer
            std::vector<char> buffer(total_size);
            size_t pos = 0;
            
            // Write total size first
            memcpy(buffer.data() + pos, &total_size, sizeof(size_t));
            pos += sizeof(size_t);
            
            // Write number of values
            size_t num_values = record.values.size();
            memcpy(buffer.data() + pos, &num_values, sizeof(size_t));
            pos += sizeof(size_t);
            
            // Write all values
            for (const auto& value : record.values) {
                // Write string length
                size_t len = value.length();
                memcpy(buffer.data() + pos, &len, sizeof(size_t));
                pos += sizeof(size_t);
                
                // Write string content
                memcpy(buffer.data() + pos, value.c_str(), len);
                pos += len;
            }
            
            // Write to file
            outFile.write(buffer.data(), total_size);
        }
        
        outFile.close();
        
        // Replace original file with temporary file
        if (std::remove(data_file.c_str()) != 0) {
            std::cerr << "Failed to remove original file: " << data_file << std::endl;
            std::remove(temp_file.c_str());
            return false;
        }
        
        if (std::rename(temp_file.c_str(), data_file.c_str()) != 0) {
            std::cerr << "Failed to rename temporary file: " << temp_file << std::endl;
            return false;
        }
        
        // Rebuild indexes if necessary
        if (updating_primary_key) {
            for (const auto& col : table->columns) {
                if (col.is_primary_key) {
                    // Find column index
                    int col_index = -1;
                    for (size_t i = 0; i < table->columns.size(); i++) {
                        if (table->columns[i].name == col.name) {
                            col_index = i;
                            break;
                        }
                    }
                    
                    if (col_index >= 0) {
                        // Recreate index
                        std::string index_file = db_name + "/" + table_name + "_" + col.name + ".idx";
                        
                        // Drop old index
                        if (!index_manager->dropIndex(db_name, table_name, col.name)) {
                            std::cerr << "Failed to drop index for column: " << col.name << std::endl;
                        }
                        
                        // Create new index
                        if (!index_manager->createIndex(db_name, table_name, col.name)) {
                            std::cerr << "Failed to rebuild index for column: " << col.name << std::endl;
                            return false;
                        }
                        
                        // Rebuild index with all records
                        for (const auto& record : all_records) {
                            if (col_index < record.values.size()) {
                                try {
                                    int key = std::stoi(record.values[col_index]);
                                    if (!index_manager->insert(index_file, key, record)) {
                                        std::cerr << "Failed to update index with key: " << key << std::endl;
                                    }
                                } catch (...) {
                                    std::cerr << "Error parsing key for index" << std::endl;
                                }
                            }
                        }
                    }
                }
            }
        }
        
        std::cout << "Records updated successfully" << std::endl;
        return true;
    } catch (const std::exception& e) {
        std::cerr << "Exception in Database::update: " << e.what() << std::endl;
        return false;
    }
}

bool Database::remove(const std::string& table_name,
                     const std::string& where_clause) {
    try {
        // Parse WHERE clause
        std::istringstream where_iss(where_clause);
        std::string column, op, value;
        where_iss >> column >> op;
        std::getline(where_iss, value);
        value = value.substr(value.find_first_not_of(" \t"));
        if (!value.empty() && (value[0] == '\'' || value[0] == '"'))
            value = value.substr(1, value.length() - 2);

        // Find records to delete
        std::vector<Record> records_to_delete;
        if (!selectWithCondition(table_name, column, op, value, records_to_delete)) {
            if (records_to_delete.empty()) {
                std::cout << "No records matched the WHERE clause. No delete performed." << std::endl;
                return true;
            }
            std::cerr << "Error during selectWithCondition for delete." << std::endl;
            return false;
        }
        if (records_to_delete.empty()) {
            std::cout << "No records matched the WHERE clause. No delete performed." << std::endl;
            return true;
        }

        // Display query results
        std::cout << "\nQuery Results (" << records_to_delete.size() << " records):" << std::endl;
        std::cout << "----------------------------------------" << std::endl;
        
        TableInfo* table = catalog_manager->getTableInfo(table_name);
        if (!table) return false;
        
        for (size_t i = 0; i < table->columns.size(); i++) {
            std::cout << std::left << std::setw(15) << table->columns[i].name;
        }
        std::cout << std::endl;
        std::cout << "----------------------------------------" << std::endl;
        
        for (const auto& record : records_to_delete) {
            for (size_t i = 0; i < record.values.size(); i++) {
                std::string display_value = record.values[i];
                std::cout << std::left << std::setw(15) << display_value;
            }
            std::cout << std::endl;
        }
        std::cout << "----------------------------------------" << std::endl;

        // Get the data file path
        std::string data_file = getTablePath(table_name);
        
        // Read all records first
        std::vector<Record> all_records;
        std::ifstream inFile(data_file, std::ios::binary);
        if (!inFile) {
            std::cerr << "Failed to open file: " << data_file << std::endl;
            return false;
        }
        
        // Get file size
        inFile.seekg(0, std::ios::end);
        size_t file_size = inFile.tellg();
        inFile.seekg(0, std::ios::beg);
        
        // Read all records from file
        if (file_size > 0) {
            std::vector<char> buffer(file_size);
            inFile.read(buffer.data(), file_size);
            
            size_t pos = 0;
            while (pos < file_size) {
                // Read record size
                size_t record_size;
                if (pos + sizeof(size_t) > file_size) break;
                memcpy(&record_size, buffer.data() + pos, sizeof(size_t));
                
                if (record_size == 0 || pos + record_size > file_size) break;
                
                // Create new record and deserialize
                Record record;
                
                // Manual deserialization
                size_t data_pos = pos + sizeof(size_t); // Skip record size
                
                // Read number of values
                size_t num_values;
                memcpy(&num_values, buffer.data() + data_pos, sizeof(size_t));
                data_pos += sizeof(size_t);
                
                // Read values
                for (size_t i = 0; i < num_values; i++) {
                    size_t str_len;
                    memcpy(&str_len, buffer.data() + data_pos, sizeof(size_t));
                    data_pos += sizeof(size_t);
                    
                    std::string value(buffer.data() + data_pos, str_len);
                    record.values.push_back(value);
                    data_pos += str_len;
                }
                
                all_records.push_back(record);
                pos += record_size;
            }
        }
        inFile.close();
        
        // Filter out records to delete
        std::vector<Record> remaining_records;
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
        
        // Create temporary file and write remaining records
        std::string temp_file = data_file + ".tmp";
        std::ofstream outFile(temp_file, std::ios::binary);
        if (!outFile) {
            std::cerr << "Failed to create temporary file: " << temp_file << std::endl;
            return false;
        }
        
        // Write remaining records to the temporary file
        for (const auto& record : remaining_records) {
            // Calculate record size
            size_t total_size = sizeof(size_t); // Size field itself
            total_size += sizeof(size_t);       // Number of values
            
            for (const auto& value : record.values) {
                total_size += sizeof(size_t);   // String length
                total_size += value.length();   // String content
            }
            
            // Create buffer
            std::vector<char> buffer(total_size);
            size_t pos = 0;
            
            // Write total size first
            memcpy(buffer.data() + pos, &total_size, sizeof(size_t));
            pos += sizeof(size_t);
            
            // Write number of values
            size_t num_values = record.values.size();
            memcpy(buffer.data() + pos, &num_values, sizeof(size_t));
            pos += sizeof(size_t);
            
            // Write all values
            for (const auto& value : record.values) {
                // Write string length
                size_t len = value.length();
                memcpy(buffer.data() + pos, &len, sizeof(size_t));
                pos += sizeof(size_t);
                
                // Write string content
                memcpy(buffer.data() + pos, value.c_str(), len);
                pos += len;
            }
            
            // Write to file
            outFile.write(buffer.data(), total_size);
        }
        
        outFile.close();
        
        // Replace original file with temporary file
        if (std::remove(data_file.c_str()) != 0) {
            std::cerr << "Failed to remove original file: " << data_file << std::endl;
            std::remove(temp_file.c_str());
            return false;
        }
        
        if (std::rename(temp_file.c_str(), data_file.c_str()) != 0) {
            std::cerr << "Failed to rename temporary file: " << temp_file << std::endl;
            return false;
        }
        
        // Rebuild all indexes
        for (const auto& col : table->columns) {
            if (col.is_primary_key) {
                // Find column index
                int col_index = -1;
                for (size_t i = 0; i < table->columns.size(); i++) {
                    if (table->columns[i].name == col.name) {
                        col_index = i;
                        break;
                    }
                }
                
                if (col_index >= 0) {
                    // Recreate index
                    std::string index_file = db_name + "/" + table_name + "_" + col.name + ".idx";
                    
                    // Drop old index
                    if (!index_manager->dropIndex(db_name, table_name, col.name)) {
                        std::cerr << "Failed to drop index for column: " << col.name << std::endl;
                    }
                    
                    // Create new index
                    if (!index_manager->createIndex(db_name, table_name, col.name)) {
                        std::cerr << "Failed to rebuild index for column: " << col.name << std::endl;
                        return false;
                    }
                    
                    // Rebuild index with remaining records
                    for (const auto& record : remaining_records) {
                        if (col_index < record.values.size()) {
                            try {
                                int key = std::stoi(record.values[col_index]);
                                if (!index_manager->insert(index_file, key, record)) {
                                    std::cerr << "Failed to update index with key: " << key << std::endl;
                                }
                            } catch (...) {
                                std::cerr << "Error parsing key for index" << std::endl;
                            }
                        }
                    }
                }
            }
        }
        
        std::cout << "Records deleted successfully" << std::endl;
        return true;
    } catch (const std::exception& e) {
        std::cerr << "Exception in Database::remove: " << e.what() << std::endl;
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
