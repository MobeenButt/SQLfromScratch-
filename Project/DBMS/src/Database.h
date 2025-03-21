#ifndef DATABASE_H
#define DATABASE_H

#include <string>
#include <unordered_map>
#include <memory>
#include "Table.h"

class Database {
private:
    std::string name;
    std::unordered_map<std::string, std::unique_ptr<Table>> tables;

public:
    Database(const std::string& dbName);
    ~Database();

    bool createTable(const std::string& tableName);
    Table* getTable(const std::string& tableName);
    void addColumnToTable(const std::string& tableName, const std::string& colName, const std::string& colType, bool isPrimaryKey = false);
    void showTableSchema(const std::string& tableName);
    void listTables();
    void saveMetadata();
    void loadMetadata();
};

#endif
