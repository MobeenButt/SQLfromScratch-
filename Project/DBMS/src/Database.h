// Database.h
#ifndef DATABASE_H
#define DATABASE_H

#include <map>
#include <string>
#include "Table.h"

class Database {
public:
    string name;
    map<string, Table> tables;

    Database(string name);
    void createTable(string tableName);
    Table* getTable(string tableName);
};

#endif
