// Database.cpp
#include "Database.h"
#include <iostream>

Database::Database(:string name) : name(name) {}

void Database::createTable(:string tableName) {
    tables[tableName] = Table(tableName);
}

Table* Database::getTable(:string tableName) {
    if (tables.find(tableName) != tables.end()) {
        return &tables[tableName];
    }
    return nullptr;
}
