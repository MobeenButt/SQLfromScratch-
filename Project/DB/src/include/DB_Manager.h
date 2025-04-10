#ifndef DATABASE_MANAGER_H
#define DATABASE_MANAGER_H

#include <string>
#include <vector>
#include <map>
#include <fstream>
#include <iostream>
#include "Catalog.h"
#include "BPlusTree.h"

enum class FieldType { INT, FLOAT, STRING, BOOL };

class FieldValue {
public:
    FieldType type;
    int intValue;
    float floatValue;
    bool boolValue;
    std::string stringValue;

    // **Constructors**
    FieldValue() : type(FieldType::STRING), stringValue("") {} 
    FieldValue(int value) : type(FieldType::INT), intValue(value) {}
    FieldValue(float value) : type(FieldType::FLOAT), floatValue(value) {}
    FieldValue(bool value) : type(FieldType::BOOL), boolValue(value) {}
    FieldValue(const std::string& value) : type(FieldType::STRING), stringValue(value) {}

    // **Copy Constructor**
    FieldValue(const FieldValue& other) {
        *this = other;
    }

    // **Assignment Operator**
    FieldValue& operator=(const FieldValue& other) {
        if (this == &other) return *this;
        type = other.type;
        stringValue = other.stringValue;
        intValue = other.intValue;
        floatValue = other.floatValue;
        boolValue = other.boolValue;
        return *this;
    }

    // **Destructor**
    ~FieldValue() {}

    // **Print Method**
    void print() const {
        switch (type) {
            case FieldType::INT: std::cout << intValue; break;
            case FieldType::FLOAT: std::cout << floatValue; break;
            case FieldType::STRING: std::cout << stringValue; break;
            case FieldType::BOOL: std::cout << (boolValue ? "true" : "false"); break;
        }
    }
};

using Record = std::map<std::string, FieldValue>;

class DatabaseManager {
public:
    DatabaseManager(const std::string& catalog_path = "data/catalog.bin");
    ~DatabaseManager();

    // **Table Operations**
    bool createTable(
        const std::string& table_name, 
        const std::vector<std::string>& column_names, 
        const std::vector<FieldType>& column_types,
        const std::vector<int>& primary_key_indices,
        const std::vector<bool>& is_foreign_key,
        const std::vector<std::string>& references_table,
        const std::vector<std::string>& references_column,
        const std::string& primary_key,
        const std::map<std::string, std::pair<std::string, std::string>>& foreign_keys = {}
    );

    bool dropTable(const std::string& table_name);
    
    // **Record Operations**
    bool insertRecord(const std::string& table_name, const Record& record);
    bool deleteRecord(const std::string& table_name, const std::string& key_column, const FieldValue& key_value);
    std::vector<Record> searchRecords(const std::string& table_name, const std::string& key_column, const FieldValue& key_value);
    
    // **Table Information**
    std::vector<std::string> listTables() const;
    Schema getTableSchema(const std::string& table_name) const;

    // **Transaction Support**
    void beginTransaction();
    void commitTransaction();
    void rollbackTransaction();

    // database
    void createDatabase(const std::string &dbName);
    void deleteDatabase(const std::string &dbName);
    void switchDatabase(const std::string &dbName);
    void listDatabases();

private:
    Catalog catalog;
    std::string catalog_path;
    std::map<std::string, BPlusTree*> indexes;

    // **Helper Functions**
    void createIndex(const Schema& schema);
    void loadIndexes();
    void saveRecord(std::ofstream& file, const Record& record, const Schema& schema);
    Record loadRecord(std::ifstream& file, const Schema& schema);
    
    // **Constraints Checking**
    bool checkForeignKey(const std::string& table_name, const Record& record);
    bool checkPrimaryKey(const std::string& table_name, const Record& record);
    bool checkUnique(const std::string& table_name, const Record& record);
    bool checkNotNull(const std::string& table_name, const Record& record);
    bool checkDataType(const std::string& table_name, const Record& record);
    
    // **Internal Record Operations**
    bool insertRecordInternal(const std::string& table_name, const Record& record);
    bool deleteRecordInternal(const std::string& table_name, const std::string& key_column, const FieldValue& key_value);
    std::vector<Record> searchRecordsInternal(const std::string& table_name, const std::string& key_column, const FieldValue& key_value);

    // **Transaction Support**
    bool in_transaction;
    std::vector<std::pair<std::string, Record>> transaction_buffer;
    std::string transaction_log_file;
    void flushTransactionLog();
    void loadTransactionLog();
    void logTransactionOperation(const std::string& operation, const std::string& table_name, const Record& record);
    void rollbackTransactionBuffer();
};

#endif
