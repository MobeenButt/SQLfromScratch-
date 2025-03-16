#ifndef DATABASE_H
#define DATABASE_H

#include "Table.h"
#include <unordered_map>
using namespace std;

class Database {
private:
    string activeDatabase; // bad me implement kr lo ga, for ease it is active in which actions will be done
    unordered_map<string, Table*> tables;
    BPlusTree index;

public:
    
    Database();
    ~Database();
    Table* getTable(const string& tableName);
    void createTable(const string& tableName);
    void addColumnToTable(const string& tableName, const string& columnName, const string& columnType, bool isPrimaryKey = false);
    void showTableSchema(const string& tableName);

    void setActiveDatabase(const string& dbName) {
        activeDatabase = dbName;
    }

    string getActiveDatabase() const {
        return activeDatabase;
    }
};

#endif
