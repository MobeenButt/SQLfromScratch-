#ifndef DATABASE_MANAGER_H
#define DATABASE_MANAGER_H

#include <string>
#include <vector>
#include <map>
#include <memory>
#include <fstream>
#include <iostream>
#include "Catalog.h"
#include "Index.h"

enum class FieldType { INT, FLOAT, STRING, BOOL };

class FieldValue {
public:
    FieldType type;
    int intValue;
    float floatValue;
    bool boolValue;
    std::string stringValue;

    FieldValue() : type(FieldType::STRING), stringValue("") {} 
    FieldValue(int value) : type(FieldType::INT), intValue(value) {}
    FieldValue(float value) : type(FieldType::FLOAT), floatValue(value) {}
    FieldValue(bool value) : type(FieldType::BOOL), boolValue(value) {}
    FieldValue(const std::string& value) : type(FieldType::STRING), stringValue(value) {}

    void print() const {
        switch (type) {
            case FieldType::INT: std::cout << intValue; break;
            case FieldType::FLOAT: std::cout << floatValue; break;
            case FieldType::STRING: std::cout << stringValue; break;
            case FieldType::BOOL: std::cout << (boolValue ? "true" : "false"); break;
        }
    }

    std::string toString() const {
        switch (type) {
            case FieldType::INT: return std::to_string(intValue);
            case FieldType::FLOAT: return std::to_string(floatValue);
            case FieldType::STRING: return stringValue;
            case FieldType::BOOL: return boolValue ? "true" : "false";
        }
        return "";
    }
};

using Record = std::map<std::string, FieldValue>;

class DatabaseManager {
public:
    DatabaseManager(const std::string& catalog_path = "data/catalog.bin");
    ~DatabaseManager();

    // Table Operations
    bool createTable(
        const std::string& table_name, 
        const std::vector<std::string>& column_names, 
        const std::vector<FieldType>& column_types,
        const std::vector<int>& primary_key_indices,
        const std::vector<bool>& is_foreign_key,
        const std::vector<std::string>& references_table,
        const std::vector<std::string>& references_column,
        const std::string& primary_key
    );

    bool dropTable(const std::string& table_name);
    
    // Record Operations
    bool insertRecord(const std::string& table_name, const Record& record);
    bool deleteRecord(const std::string& table_name, const std::string& key_column, const FieldValue& key_value);
    std::vector<Record> searchRecords(const std::string& table_name, const std::string& key_column, const FieldValue& key_value);
    
    // Table Information
    std::vector<std::string> listTables() const;
    Schema getTableSchema(const std::string& table_name) const;

    // Transaction Support
    void beginTransaction();
    void commitTransaction();
    void rollbackTransaction();

    // Database Operations
    void createDatabase(const std::string &dbName);
    void deleteDatabase(const std::string &dbName);
    void switchDatabase(const std::string &dbName);
    std::vector<std::string> listDatabases();  // Change return type
    std::vector<std::string> listTableColumns(const std::string& table_name) const;
    void displayCurrentDatabase(const std::string& dbName) const;
    void displayTable(const std::string& table_name);
    // std::vector<std::string> listDatabases();  // Change return type

    // Index Operations
    bool createIndex(const std::string& index_name, 
                   const std::string& table_name,
                   const std::string& column_name,
                   db::IndexType type);
    bool dropIndex(const std::string& index_name);

private:
    std::string catalog_path;
    Catalog catalog;
    db::IndexManager index_manager;
    bool in_transaction;
    std::vector<std::pair<std::string, Record>> transaction_buffer;
    std::string transaction_log_file;

    // Helper Functions
    void saveRecord(std::ofstream& file, const Record& record, const Schema& schema);
    Record loadRecord(std::ifstream& file, const Schema& schema);
    
    // Constraints Checking
    bool checkForeignKey(const std::string& table_name, const Record& record);
    bool checkPrimaryKey(const std::string& table_name, const Record& record);
    bool checkUnique(const std::string& table_name, const Record& record);
    bool checkNotNull(const std::string& table_name, const Record& record);
    bool checkDataType(const std::string& table_name, const Record& record);
    
    // Internal Record Operations
    bool insertRecordInternal(const std::string& table_name, const Record& record);
    bool deleteRecordInternal(const std::string& table_name, const std::string& key_column, const FieldValue& key_value);
    std::vector<Record> searchRecordsInternal(const std::string& table_name, const std::string& key_column, const FieldValue& key_value);

    // Transaction Support
    void flushTransactionLog();
    void rollbackTransactionBuffer();
};

#endif