// Table.cpp
#include "Table.h"
#include <iostream>

Table::Table(std::string name) : name(name) {}

void Table::addColumn(std::string columnName, std::string type, bool isPrimaryKey) {
    columns.emplace_back(columnName, type, isPrimaryKey);
    if (isPrimaryKey) {
        index = BPlusTree(); // Initialize B+ Tree index for primary key
    }
}

void Table::insertRecord(int primaryKey, std::string data) {
    index.insert(primaryKey, data);
}

void Table::searchRecord(int primaryKey) {
    std::string result = index.search(primaryKey);
    if (!result.empty()) {
        std::cout << "Record: " << result << "\n";
    } else {
        std::cout << "Record not found.\n";
    }
}

void Table::deleteRecord(int primaryKey) {
    index.remove(primaryKey);
}
