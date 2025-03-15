#ifndef DATABASE_H
#define DATABASE_H

#include "Table.h"
#include <unordered_map>
using namespace std;

class Database {
private:
    unordered_map<string, Table*> tables;

public:
    Database();
    ~Database();
    void createTable(const string& tableName);
    void addColumnToTable(const string& tableName, const string& columnName, const string& columnType, bool isPrimaryKey = false);
    void showTableSchema(const string& tableName);
};

#endif
