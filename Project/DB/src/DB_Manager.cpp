#include"include/DB_Manager.h"  
#include"include/Transaction.h"
#include <vector>

#include <filesystem>
namespace fs = std::filesystem;

DatabaseManager::DatabaseManager(const std::string& catalog_path) 
    : catalog_path(catalog_path), in_transaction(false), transaction_log_file("data/transactions.log") {
    catalog.load(catalog_path);
    loadIndexes();
}

DatabaseManager::~DatabaseManager() {
    catalog.save(catalog_path);
    for (auto& pair : indexes) delete pair.second;  // Clean up B+ Tree indexes
}

/**
 * Creates a table with constraints and indexes.
 */
bool DatabaseManager::createTable(
    const std::string& table_name, 
    const std::vector<std::string>& column_names, 
    const std::vector<FieldType>& column_types,
    const std::vector<int>& primary_key_indices,
    const std::vector<bool>& is_foreign_key,
    const std::vector<std::string>& references_table,
    const std::vector<std::string>& references_column,
    const std::string& primary_key,
    const std::map<std::string, std::pair<std::string, std::string>>& foreign_keys) 
{
    (void)primary_key_indices;
    (void)foreign_keys;
    
    if (catalog.tableExists(table_name)) return false;  

    std::vector<Column> columns;
    for (size_t i = 0; i < column_names.size(); ++i) {
        columns.emplace_back(column_names[i], static_cast<ColumnType>(column_types[i]), 0, 
                             (column_names[i] == primary_key), is_foreign_key[i], 
                             references_table[i], references_column[i]);
    }

    Schema schema(table_name, columns, "data/" + table_name + ".db", "index/" + table_name + ".idx");
    if (!catalog.addTable(schema)) return false;

    createIndex(schema);
    return true;
}


/**
 * Drops a table and deletes its data/index files.
 */
bool DatabaseManager::dropTable(const std::string& table_name) {
    if (!catalog.tableExists(table_name)) return false;

    Schema schema = catalog.getSchema(table_name);
    std::remove(schema.data_file_path.c_str());
    std::remove(schema.index_file_path.c_str());

    delete indexes[table_name];  // Free memory
    indexes.erase(table_name);

    return catalog.removeTable(table_name);
}

/**
 * Inserts a record after checking constraints.
 */
bool DatabaseManager::insertRecord(const std::string& table_name, const Record& record) {
    if (!catalog.tableExists(table_name)) return false;
    if (!checkPrimaryKey(table_name, record) || !checkForeignKey(table_name, record) ||
        !checkUnique(table_name, record) || !checkNotNull(table_name, record) ||
        !checkDataType(table_name, record)) {
        return false;
    }

    return insertRecordInternal(table_name, record);
}

/**
 * Deletes a record based on a key.
 */
bool DatabaseManager::deleteRecord(const std::string& table_name, const std::string& key_column, const FieldValue& key_value) {
    if (!catalog.tableExists(table_name)) return false;
    return deleteRecordInternal(table_name, key_column, key_value);
}

/**
 * Searches for records based on a key.
 */
std::vector<Record> DatabaseManager::searchRecords(const std::string& table_name, const std::string& key_column, const FieldValue& key_value) {
    if (!catalog.tableExists(table_name)) return {};
    return searchRecordsInternal(table_name, key_column, key_value);
}

/**
 * Begins a new transaction.
 */
void DatabaseManager::beginTransaction() {
    if (in_transaction) return;
    in_transaction = true;
    transaction_buffer.clear();
}

/**
 * Commits the transaction.
 */
void DatabaseManager::commitTransaction() {
    if (!in_transaction) return;
    
    flushTransactionLog();
    in_transaction = false;
}



/**
 * Rolls back the transaction.
 */
void DatabaseManager::rollbackTransaction() {
    if (!in_transaction) return;
    
    rollbackTransactionBuffer();
    in_transaction = false;
}

/**
 * Creates an index for a table.
 */
void DatabaseManager::createIndex(const Schema& schema) {
    if (indexes.find(schema.name) != indexes.end()) return;
    indexes[schema.name] = new BPlusTree(schema.index_file_path);
}

/**
 * Loads indexes from disk.
 */
void DatabaseManager::loadIndexes() {
    for (const auto& table : catalog.listTables()) {
        Schema schema = catalog.getSchema(table);
        indexes[table] = new BPlusTree(schema.index_file_path);
    }
}

/**
 * Inserts a record internally (handles file storage).
 */
bool DatabaseManager::insertRecordInternal(const std::string& table_name, const Record& record) {
    Schema schema = catalog.getSchema(table_name);
    std::ofstream file(schema.data_file_path, std::ios::app | std::ios::binary);
    if (!file) return false;

    int offset = file.tellp();
    saveRecord(file, record, schema);
    file.close();

    // Fix: Use col_name instead of name
    auto primary_key = schema.columns[0].col_name;
    indexes[table_name]->insert(record.at(primary_key).intValue, offset);

    return true;
}

void DatabaseManager::saveRecord(std::ofstream& file, const Record& record, const Schema& schema) {
    for (const auto& column : schema.columns) {
        // Fix: Use col_name instead of name
        auto value = record.at(column.col_name);
        if (value.type == FieldType::INT) {
            file.write(reinterpret_cast<const char*>(&value.intValue), sizeof(int));
        } else if (value.type == FieldType::FLOAT) {
            file.write(reinterpret_cast<const char*>(&value.floatValue), sizeof(float));
        } else if (value.type == FieldType::STRING) {
            size_t len = value.stringValue.size();
            file.write(reinterpret_cast<const char*>(&len), sizeof(size_t));
            file.write(value.stringValue.c_str(), len);
        } else if (value.type == FieldType::BOOL) {
            file.write(reinterpret_cast<const char*>(&value.boolValue), sizeof(bool));
        }
    }
}

void DatabaseManager::rollbackTransactionBuffer() {
    for (const auto& entry : transaction_buffer) {
        Schema schema = catalog.getSchema(entry.first);
        // Fix: Use col_name instead of name
        deleteRecordInternal(entry.first, schema.columns[0].col_name, entry.second.at(schema.columns[0].col_name));
    }
    transaction_buffer.clear();
}

void DatabaseManager::flushTransactionLog() {
    std::ofstream file(transaction_log_file, std::ios::app);
    for (const auto& entry : transaction_buffer) {
        Schema schema = catalog.getSchema(entry.first);
        // Fix: Use col_name instead of name
        file << "INSERT " << entry.first << " " << entry.second.at(schema.columns[0].col_name).intValue << std::endl;
    }
    file.close();
    transaction_buffer.clear();
}


/**
 * Checks if the foreign key constraint is satisfied.
 */
bool DatabaseManager::checkForeignKey(const std::string& table_name, const Record& record) {
    Schema schema = catalog.getSchema(table_name);
    
    for (const auto& column : schema.columns) {
        if (column.is_foreign) {  // FIXED: was 'is_foreign_key'
            std::vector<Record> parentRecords = searchRecords(column.ref_table, column.ref_column, record.at(column.col_name)); // FIXED: was 'references_table' & 'references_column'
            if (parentRecords.empty()) {
                std::cerr << "Foreign key constraint failed for column: " << column.col_name << std::endl;
                return false;
            }
        }
    }
    return true;
}

bool DatabaseManager::checkUnique(const std::string& table_name, const Record& record) {
    Schema schema = catalog.getSchema(table_name);

    for (const auto& column : schema.columns) {
        if (column.is_unique) {  // FIXED
            std::vector<Record> existingRecords = searchRecords(table_name, column.col_name, record.at(column.col_name));
            if (!existingRecords.empty()) {
                std::cerr << "Unique constraint violated for column: " << column.col_name << std::endl;
                return false;
            }
        }
    }
    return true;
}

// Record DatabaseManager::loadRecord(std::ifstream& file, const Schema& schema) {
//     Record record;
//     for (const auto& column : schema.columns) {
//         FieldValue value;
//         FieldType colType = static_cast<FieldType>(column.col_type); // Ensure both are same type

//         if (colType == FieldType::INT) { 
//             file.read(reinterpret_cast<char*>(&value.intValue), sizeof(int));
//             value.type = FieldType::INT;
//         } else if (colType == FieldType::FLOAT) { 
//             file.read(reinterpret_cast<char*>(&value.floatValue), sizeof(float));
//             value.type = FieldType::FLOAT;
//         } else if (colType == FieldType::STRING) { 
//             size_t len;
//             file.read(reinterpret_cast<char*>(&len), sizeof(size_t));

//             std::string buffer(len, '\0'); 
//             file.read(&buffer[0], len);
//             value.stringValue = buffer;

//             value.type = FieldType::STRING;
//         } else if (colType == FieldType::BOOL) { 
//             file.read(reinterpret_cast<char*>(&value.boolValue), sizeof(bool));
//             value.type = FieldType::BOOL;
//         }

//         record[column.col_name] = value;
//     }
//     return record;
// }



bool DatabaseManager::checkNotNull(const std::string& table_name, const Record& record) {
    Schema schema = catalog.getSchema(table_name);

    for (const auto& column : schema.columns) {
        if (column.not_null && record.find(column.col_name) == record.end()) { // FIXED: was 'is_not_null'
            std::cerr << "NOT NULL constraint failed for column: " << column.col_name << std::endl;
            return false;
        }
    }
    return true;
}

bool DatabaseManager::checkDataType(const std::string& table_name, const Record& record) {
    Schema schema = catalog.getSchema(table_name);

    for (const auto& column : schema.columns) {
        FieldType expectedType = static_cast<FieldType>(column.col_type); // FIXED: was 'type'
        if (record.find(column.col_name) != record.end()) {
            FieldType actualType = record.at(column.col_name).type;
            if (actualType != expectedType) {
                std::cerr << "Data type mismatch for column: " << column.col_name << std::endl;
                return false;
            }
        }
    }
    return true;
}
Record DatabaseManager::loadRecord(std::ifstream& file, const Schema& schema) {
    Record record;
    for (const auto& column : schema.columns) {
        FieldValue value;
        FieldType colType = static_cast<FieldType>(column.col_type); // Convert once

        if (colType == FieldType::INT) { 
            file.read(reinterpret_cast<char*>(&value.intValue), sizeof(int));
            value.type = FieldType::INT;
        } else if (colType == FieldType::FLOAT) { 
            file.read(reinterpret_cast<char*>(&value.floatValue), sizeof(float));
            value.type = FieldType::FLOAT;
        } else if (colType == FieldType::STRING) { 
            size_t len;
            file.read(reinterpret_cast<char*>(&len), sizeof(size_t));

            std::string buffer(len, '\0'); // Use std::string directly
            file.read(&buffer[0], len);
            value.stringValue = buffer; // No need for manual new/delete

            value.type = FieldType::STRING;
        } else if (colType == FieldType::BOOL) { 
            file.read(reinterpret_cast<char*>(&value.boolValue), sizeof(bool));
            value.type = FieldType::BOOL;
        }

        record[column.col_name] = value;
    }
    return record;
}

/**
 * Lists all tables in the database.
 */
std::vector<std::string> DatabaseManager::listTables() const {
    return catalog.listTables();
}

/**
 * Gets the schema of a specific table.
 */
Schema DatabaseManager::getTableSchema(const std::string& table_name) const {
    return catalog.getSchema(table_name);
}
bool DatabaseManager::checkPrimaryKey(const std::string& table_name, const Record& record) {
    // Implement primary key check logic here
    return true; // Placeholder
}

bool DatabaseManager::deleteRecordInternal(const std::string& table_name, const std::string& column_name, const FieldValue& value) {
    // Implement record deletion logic here
    return true; // Placeholder
}

std::vector<Record> DatabaseManager::searchRecordsInternal(const std::string& table_name, const std::string& column_name, const FieldValue& value) {
    std::vector<Record> results;
    // Implement record search logic here
    return results;
}

void DatabaseManager::createDatabase(const std::string &dbName) {
    std::string dbPath = "databases/" + dbName;
    if (!fs::exists(dbPath)) {
        fs::create_directory(dbPath);
        catalog.createMetadata(dbName); // Ensure metadata is initialized
        std::cout << "Database '" << dbName << "' created successfully.\n";
    } else {
        std::cout << "Database already exists!\n";
    }
}


void DatabaseManager::listDatabases() {
    std::vector<std::string> dbList;
    std::string dbPath = "databases"; // Adjust path if needed
    if (!std::filesystem::exists(dbPath)) {
        return; // Return if no databases found
    }

    for (const auto& entry : std::filesystem::directory_iterator(dbPath)) {
        if (entry.is_directory()) {
            dbList.push_back(entry.path().filename().string());
        }
    }
    for(auto i: dbList){
        std::cout<<"DATABASE Name: " << i << std::endl;
    }
}

void DatabaseManager::switchDatabase(const std::string& dbName) {
    // Your logic here
}

void DatabaseManager::deleteDatabase(const std::string& dbName) {
    std::string dbPath = "databases/" + dbName;
    
    if (!std::filesystem::exists(dbPath)) {
        std::cout << "Error: Database '" << dbName << "' does not exist.\n";
        return;
    }

    try {
        std::filesystem::remove_all(dbPath);
        std::cout << "Database '" << dbName << "' deleted successfully.\n";
    } catch (const std::filesystem::filesystem_error& e) {
        std::cout << "Error deleting database: " << e.what() << "\n";
    }
}
