#include "BPlusTree.h"
#include "Database.h"
#include <iostream>

using namespace std;

Database::Database() {}


Database::~Database() {
    for (auto& table : tables) {
        delete table.second; 
    }
}
void Database::createTable(const string& tableName) {
    if (tables.find(tableName) != tables.end()) {
        cout << "Table '" << tableName << "' already exists!\n";
        return;
    }
    tables[tableName] = new Table(tableName);
    cout << "Table '" << tableName << "' created.\n";
}

void Database::addColumnToTable(const string& tableName, const string& colName, const string& colType, bool isPrimaryKey) {
    if (tables.find(tableName) == tables.end()) {
        cout << "Table not found!\n";
        return;
    }
    tables[tableName]->addColumn(colName, colType, isPrimaryKey);
}

void Database::showTableSchema(const string& tableName) {
    if (tables.find(tableName) == tables.end()) {
        cout << "Table not found!\n";
        return;
    }
    tables[tableName]->displayTable();
}

Table* Database::getTable(const string& tableName) {
    if (tables.find(tableName) != tables.end()) {
        return tables[tableName];
    }
    return nullptr;
}
