#include "Table.h"
#include <iostream>
#include <fstream>

using namespace std;

Table::Table(const std::string& tableName) : name(tableName) {
    loadFromDisk();
}

void Table::addColumn(const std::string& name, const std::string& type, bool isPrimaryKey) {
    if (isPrimaryKey && !primaryKeyColumn.empty()) {
        cerr << "Error: Table already has a primary key column.\n";
        return;
    }
    columns.emplace_back(name, type, isPrimaryKey);
    if (isPrimaryKey) {
        primaryKeyColumn = name;
    }
    cout << "Column '" << name << "' added to table '" << this->name << "'.\n";
    saveToDisk();
}

void Table::insertRecord(int key, const std::string& data) {
    // Temporarily remove the index-based check for primary key
    cout << "Inserting record: " << data << " with key: " << key << endl;
    saveToDisk();
}

std::string Table::searchRecord(int key) {
    // Temporarily remove the index-based search
    return ""; // just return an empty string for now
}

void Table::deleteRecord(int key) {
    // Temporarily remove the index-based deletion
    cout << "Record with key " << key << " deleted." << endl;
    saveToDisk();
}

void Table::displayTable() const {
    cout << "Table: " << name << "\n";
    for (const auto& column : columns) {
        column.displayColumns();
    }
}

void Table::saveToDisk() {
    ofstream file("tables/" + name + ".db", ios::binary | ios::trunc);
    if (!file) {
        cerr << "Error: Cannot open table file for writing.\n";
        return;
    }

    saveColumnsToDisk(file);
    file.close();
}

void Table::loadFromDisk() {
    ifstream file("tables/" + name + ".db", ios::binary);
    if (!file) {
        cerr << "Error: Cannot open table file for reading.\n";
        return;
    }

    loadColumnsFromDisk(file);
    file.close();
}

bool Table::validatePrimaryKey(int key) {
    return true; // Temporarily bypass primary key validation
}

void Table::saveColumnsToDisk(ofstream& file) {
    size_t columnCount = columns.size();
    file.write(reinterpret_cast<char*>(&columnCount), sizeof(columnCount));

    for (const auto& column : columns) {
        size_t nameLen = column.getName().size();
        file.write(reinterpret_cast<char*>(&nameLen), sizeof(nameLen));
        file.write(column.getName().c_str(), nameLen);

        size_t typeLen = column.getType().size();
        file.write(reinterpret_cast<char*>(&typeLen), sizeof(typeLen));
        file.write(column.getType().c_str(), typeLen);

        bool isPrimaryKey = column.isPrimaryKeyColumn();
        file.write(reinterpret_cast<char*>(&isPrimaryKey), sizeof(isPrimaryKey));
    }
}

void Table::loadColumnsFromDisk(ifstream& file) {
    size_t columnCount;
    file.read(reinterpret_cast<char*>(&columnCount), sizeof(columnCount));

    for (size_t i = 0; i < columnCount; ++i) {
        size_t nameLen, typeLen;
        file.read(reinterpret_cast<char*>(&nameLen), sizeof(nameLen));

        string name(nameLen, ' ');
        file.read(&name[0], nameLen);

        file.read(reinterpret_cast<char*>(&typeLen), sizeof(typeLen));
        string type(typeLen, ' ');
        file.read(&type[0], typeLen);

        bool isPrimaryKey;
        file.read(reinterpret_cast<char*>(&isPrimaryKey), sizeof(isPrimaryKey));

        columns.emplace_back(name, type, isPrimaryKey);
        if (isPrimaryKey) {
            primaryKeyColumn = name;
        }
    }
}
