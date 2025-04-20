#include "database.h"
#include <iostream>
#include <sstream>
#include <algorithm>
#include <cstring>  // for memset
#include "page.h"
#include "catalog_manager.h" // Add include for CatalogManager

// Forward declare helper function
std::vector<std::string> readRecord(const char* data, const std::vector<ColumnInfo>& columns);

Database::Database(const std::string& name) : db_name(name) {
    storage_manager = new StorageManager("./data");
    storage_manager->createDatabase(name);
    
    catalog_manager = new CatalogManager(name);
    index_manager = new IndexManager(storage_manager);
}

Database::~Database() {
    delete storage_manager;
    delete catalog_manager;
    delete index_manager;
}

bool Database::createTable(const std::string& table_name, 
                         const std::vector<ColumnInfo>& columns) {
    if (!storage_manager->createTable(db_name, table_name)) {
        return false;
    }

    return catalog_manager->createTable(table_name, columns);
}

bool Database::dropTable(const std::string& table_name) {
    TableInfo* table = catalog_manager->getTableInfo(table_name);
    if (!table) {
        return false;
    }

    // Drop all indexes
    for (const auto& index_file : table->index_files) {
        // Extract column name from index file name
        std::string column_name = index_file.substr(
            table_name.length() + 1,
            index_file.length() - table_name.length() - 5
        );
        index_manager->dropIndex(table_name, column_name);
    }

    // Remove the table's data file
    storage_manager->dropTable(db_name, table_name);
    return catalog_manager->dropTable(table_name);
}

bool Database::createIndex(const std::string& table_name, const std::string& column_name) {
    TableInfo* table = catalog_manager->getTableInfo(table_name);
    if (!table) {
        std::cerr << "Table not found: " << table_name << std::endl;
        return false;
    }

    // Verify column exists and is of indexable type
    bool found = false;
    for (const auto& col : table->columns) {
        if (col.name == column_name && col.type == "INT") {
            found = true;
            break;
        }
    }
    if (!found) {
        std::cerr << "Column not found or not indexable: " << column_name << std::endl;
        return false;
    }

    // Check if index already exists
    std::string index_name = table_name + "_" + column_name + ".idx";
    if (std::find(table->index_files.begin(), 
                  table->index_files.end(), 
                  index_name) != table->index_files.end()) {
        std::cerr << "Index already exists" << std::endl;
        return false;
    }

    std::cout << "Creating index file in database: " << db_name << std::endl;
    
    // First create the index file
    if (!storage_manager->createTable(db_name, index_name)) {
        std::cerr << "Failed to create index file" << std::endl;
        return false;
    }

    // Then add index to catalog
    if (!catalog_manager->addIndex(table_name, column_name)) {
        std::cerr << "Failed to add index to catalog" << std::endl;
        // Cleanup the created file
        storage_manager->dropTable(db_name, index_name);
        return false;
    }

    // Finally build the index
    if (!index_manager->createIndex(table_name, column_name, table)) {
        std::cerr << "Failed to build index" << std::endl;
        // Rollback changes
        catalog_manager->removeIndex(table_name, column_name);
        storage_manager->dropTable(db_name, index_name);
        return false;
    }

    std::cout << "Successfully created index on " << table_name << "(" << column_name << ")" << std::endl;
    return true;
}

bool Database::insert(const std::string& table_name, 
                     const std::vector<std::string>& values) {
    TableInfo* table = catalog_manager->getTableInfo(table_name);
    if (!table || values.size() != table->columns.size()) {
        return false;
    }

    // Check for duplicates using indexes
    for (size_t i = 0; i < table->columns.size(); i++) {
        const ColumnInfo& col = table->columns[i];
        if (col.type == "INT") {
            std::string index_name = table_name + "_" + col.name + ".idx";
            if (std::find(table->index_files.begin(), 
                         table->index_files.end(), 
                         index_name) != table->index_files.end()) {
                try {
                    int key = std::stoi(values[i]);
                    if (index_manager->exists(index_name, key)) {
                        return false; // Duplicate found
                    }
                } catch (...) {
                    continue;
                }
            }
        }
    }

    // Calculate record size
    size_t record_size = 0;
    for (const auto& col : table->columns) {
        if (col.type == "INT") {
            record_size += sizeof(int);
        } else if (col.type == "VARCHAR") {
            record_size += col.size;
        }
    }

    // Create record
    std::vector<char> record_data(record_size);
    size_t offset = 0;
    
    try {
        for (size_t i = 0; i < values.size(); i++) {
            const ColumnInfo& col = table->columns[i];
            std::string value = values[i];
            
            if (col.type == "INT") {
                int int_val = std::stoi(value);
                memcpy(record_data.data() + offset, &int_val, sizeof(int));
                offset += sizeof(int);
            }
            else if (col.type == "VARCHAR") {
                if (value.length() > static_cast<size_t>(col.size)) {
                    return false; // String too long
                }
                memcpy(record_data.data() + offset, value.c_str(), value.length());
                memset(record_data.data() + offset + value.length(), 0, col.size - value.length());
                offset += col.size;
            }
        }
    } catch (const std::exception& e) {
        return false; // Value conversion failed
    }

    // Find or create page with enough space
    Page page;
    int page_id = 0;
    bool found_space = false;

    while (storage_manager->readPage(table->data_file, page_id, page)) {
        if (page.free_space >= static_cast<int>(record_size)) {
            found_space = true;
            break;
        }
        page_id++;
    }

    if (!found_space) {
        page = Page();
        page.free_space = PAGE_SIZE;
    }

    // Write record
    size_t write_offset = PAGE_SIZE - page.free_space;
    memcpy(page.data + write_offset, record_data.data(), record_size);
    page.free_space -= record_size;

    if (!storage_manager->writePage(table->data_file, page_id, page)) {
        return false;
    }

    // Update indexes
    for (size_t i = 0; i < table->columns.size(); i++) {
        const ColumnInfo& col = table->columns[i];
        if (col.type == "INT") {
            std::string index_name = table_name + "_" + col.name + ".idx";
            if (std::find(table->index_files.begin(), 
                         table->index_files.end(), 
                         index_name) != table->index_files.end()) {
                try {
                    int key = std::stoi(values[i]);
                    if (!index_manager->insertEntry(index_name, key, page_id)) {
                        // Index update failed - should implement rollback
                        return false;
                    }
                } catch (...) {
                    continue;
                }
            }
        }
    }

    return true;
}

std::vector<std::vector<std::string>> Database::select(
    const std::string& table_name, 
    const std::string& condition) {
    
    std::vector<std::vector<std::string>> results;
    TableInfo* table = catalog_manager->getTableInfo(table_name);
    if (!table) {
        std::cerr << "Table not found: " << table_name << std::endl;
        return results;
    }

    // Calculate record size
    size_t record_size = 0;
    for (const auto& col : table->columns) {
        if (col.type == "INT") {
            record_size += sizeof(int);
        } else if (col.type == "VARCHAR") {
            record_size += col.size;
        }
    }

    if (record_size == 0) {
        std::cerr << "Invalid record size" << std::endl;
        return results;
    }

    // Parse condition if it exists
    std::string col_name, op, value;
    if (!condition.empty()) {
        std::istringstream iss(condition);
        iss >> col_name >> op >> value;
    }

    // Find column index for condition
    int col_idx = -1;
    std::string col_type;
    if (!condition.empty()) {
        for (size_t i = 0; i < table->columns.size(); i++) {
            if (table->columns[i].name == col_name) {
                col_idx = i;
                col_type = table->columns[i].type;
                break;
            }
        }
    }

    // Full table scan
    Page page;
    int page_id = 0;

    std::cout << "Scanning table: " << table_name << std::endl;
    
    // Read first page to check if file exists and has data
    if (!storage_manager->readPage(table->data_file, 0, page)) {
        std::cout << "No data found in table" << std::endl;
        return results;
    }

    do {
        // If page is completely empty, we've reached the end of data
        if (page.free_space == PAGE_SIZE) {
            break;
        }

        size_t used_space = PAGE_SIZE - page.free_space;
        size_t num_records = used_space / record_size;

        // Process records in this page
        for (size_t i = 0; i < num_records; i++) {
            const char* record_start = page.data + (i * record_size);
            std::vector<std::string> record = readRecord(record_start, table->columns);
            
            // If no condition, add all records
            bool matches = condition.empty();
            
            // If there is a condition, check if record matches
            if (!matches && col_idx != -1) {
                const std::string& field_value = record[col_idx];
                if (col_type == "INT") {
                    try {
                        int record_val = std::stoi(field_value);
                        int cond_val = std::stoi(value);
                        if (op == "=") matches = (record_val == cond_val);
                        else if (op == ">") matches = (record_val > cond_val);
                        else if (op == "<") matches = (record_val < cond_val);
                    } catch (...) {
                        continue;
                    }
                } else if (op == "=" && field_value == value) {
                    matches = true;
                }
            }
            
            if (matches) {
                results.push_back(record);
            }
        }

        page_id++;
    } while (storage_manager->readPage(table->data_file, page_id, page));

    // Print results
    if (results.empty()) {
        std::cout << "No records found" << std::endl;
    } else {
        for (const auto& record : results) {
            for (size_t i = 0; i < record.size(); i++) {
                std::cout << record[i];
                if (i < record.size() - 1) {
                    std::cout << ", ";
                }
            }
            std::cout << std::endl;
        }
        std::cout << results.size() << " record(s) found" << std::endl;
    }

    return results;
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

    // Find records to update using where clause
    auto records_to_update = select(table_name, where_clause);
    if (records_to_update.empty()) return false;

    // Update records
    Page page;
    int page_id = 0;
    bool success = true;

    while (storage_manager->readPage(table->data_file, page_id, page)) {
        bool page_modified = false;
        size_t offset = 0;

        while (offset < static_cast<size_t>(PAGE_SIZE - page.free_space)) {
            std::vector<std::string> current_record;
            const char* ptr = page.data + offset;
            size_t record_size = 0;

            // Extract current record
            for (const auto& col : table->columns) {
                if (col.type == "INT") {
                    int val = *reinterpret_cast<const int*>(ptr);
                    current_record.push_back(std::to_string(val));
                    record_size += sizeof(int);
                }
                else if (col.type == "VARCHAR") {
                    std::string str(ptr, col.size);
                    str.erase(std::find(str.begin(), str.end(), '\0'), str.end());
                    current_record.push_back(str);
                    record_size += col.size;
                }
            }

            // Check if this record matches any record to update
            for (const auto& record : records_to_update) {
                if (record == current_record) {
                    // Update the record
                    if (table->columns[update_col_idx].type == "INT") {
                        int new_val = std::stoi(new_value);
                        memcpy(page.data + offset + record_size, 
                               &new_val, sizeof(int));
                    }
                    else if (table->columns[update_col_idx].type == "VARCHAR") {
                        std::string padded_value = new_value;
                        padded_value.append(
                            table->columns[update_col_idx].size - new_value.length(), 
                            '\0');
                        memcpy(page.data + offset + record_size,
                               padded_value.c_str(),
                               table->columns[update_col_idx].size);
                    }
                    page_modified = true;
                }
            }

            offset += record_size;
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
                     const std::string& condition) {
    TableInfo* table = catalog_manager->getTableInfo(table_name);
    if (!table) {
        return false;
    }

    // Find records to delete
    auto records_to_delete = select(table_name, condition);
    if (records_to_delete.empty()) return true; // Nothing to delete

    Page page;
    int page_id = 0;
    bool success = true;

    while (storage_manager->readPage(table->data_file, page_id, page)) {
        bool page_modified = false;
        size_t read_offset = 0;
        size_t write_offset = 0;

        while (read_offset < static_cast<size_t>(PAGE_SIZE - page.free_space)) {
            std::vector<std::string> current_record;
            const char* ptr = page.data + read_offset;
            size_t record_size = 0;

            // Extract current record
            for (const auto& col : table->columns) {
                if (col.type == "INT") {
                    int val = *reinterpret_cast<const int*>(ptr);
                    current_record.push_back(std::to_string(val));
                    record_size += sizeof(int);
                }
                else if (col.type == "VARCHAR") {
                    std::string str(ptr, col.size);
                    str.erase(std::find(str.begin(), str.end(), '\0'), str.end());
                    current_record.push_back(str);
                    record_size += col.size;
                }
            }

            // Check if this record should be deleted
            bool should_delete = false;
            for (const auto& record : records_to_delete) {
                if (record == current_record) {
                    should_delete = true;
                    break;
                }
            }

            if (!should_delete) {
                // Keep this record by moving it to write_offset if necessary
                if (write_offset != read_offset) {
                    memmove(page.data + write_offset, 
                           page.data + read_offset, 
                           record_size);
                    page_modified = true;
                }
                write_offset += record_size;
            } else {
                page_modified = true;
            }

            read_offset += record_size;
        }

        if (page_modified) {
            // Update free space
            page.free_space = PAGE_SIZE - write_offset;
            
            // Clear the rest of the page
            if (write_offset < PAGE_SIZE) {
                memset(page.data + write_offset, 0, PAGE_SIZE - write_offset);
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