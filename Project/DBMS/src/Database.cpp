<<<<<<< Updated upstream
<<<<<<< Updated upstream
#include "BPlusTree.h"
=======
>>>>>>> Stashed changes
=======
>>>>>>> Stashed changes
#include "Database.h"
#include "BPlusTree.h"
#include <iostream>

<<<<<<< Updated upstream
<<<<<<< Updated upstream
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
=======
// Database::Database(std::string name) : name(name) {}
Database::Database(std::string name) : name(name) {
    loadMetadata();
}

=======
// Database::Database(std::string name) : name(name) {}
Database::Database(std::string name) : name(name) {
    loadMetadata();
}

>>>>>>> Stashed changes
bool Database::createTable(string tableName) {
    if (tables.find(tableName) != tables.end()) return false; // Avoid duplicate tables

    tables[tableName] = Table(tableName);
    saveMetadata(); // Save table name
    return true;
}



Table* Database::getTable(std::string tableName) {
    auto it = tables.find(tableName);
    if (it != tables.end()) {
        return &it->second;
<<<<<<< Updated upstream
>>>>>>> Stashed changes
=======
>>>>>>> Stashed changes
    }
    return nullptr;
}
void Database::listTables() {
    std::cout << "Tables in database:\n";
    for (const auto& table : tables) {
        std::cout << "- " << table.first << std::endl;
    }
}

void Database::saveMetadata() {
    ofstream file("database_metadata.db", ios::trunc);
    if (!file) {
        cerr << "Error: Cannot open metadata file.\n";
        return;
    }

    for (const auto& pair : tables) {
        file << pair.first << "\n";  // Store table names
    }
    file.close();
}
void Database::loadMetadata() {
    ifstream file("database_metadata.db");
    if (!file) return;

    string tableName;
    while (getline(file, tableName)) {
        tables[tableName] = Table(tableName);
    }
    file.close();
}
