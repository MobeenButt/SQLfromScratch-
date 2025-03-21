#include "Database.h"
#include <iostream>
#include <fstream>
#include <stdexcept>
#include <sys/stat.h>
#include <direct.h>

using namespace std;

Database::Database(const std::string& dbName) : name(dbName) {
    // Ensure the "tables" directory exists
#ifdef _WIN32
    _mkdir("tables"); // Windows command
#else
    system("mkdir -p tables"); // Linux/macOS command
#endif
    loadMetadata();
}

Database::~Database() {
    // No need to manually delete tables, as unique_ptr automatically handles it
}

bool Database::createTable(const string& tableName) {
    if (tables.find(tableName) != tables.end()) {
        cout << "Table '" << tableName << "' already exists!\n";
        return false;
    }

    try {
        tables[tableName] = make_unique<Table>(tableName);
        saveMetadata();
        cout << "Table '" << tableName << "' created.\n";
        return true;
    } catch (const bad_alloc& e) {
        cerr << "Memory allocation failed while creating table: " << e.what() << endl;
        return false;
    }
}

void Database::addColumnToTable(const string& tableName, const string& colName, const string& colType, bool isPrimaryKey) {
    auto it = tables.find(tableName);
    if (it == tables.end()) {
        cout << "Table not found!\n";
        return;
    }
    it->second->addColumn(colName, colType, isPrimaryKey);
}

void Database::showTableSchema(const string& tableName) {
    auto it = tables.find(tableName);
    if (it == tables.end()) {
        cout << "Table not found!\n";
        return;
    }
    it->second->displayTable();
}

Table* Database::getTable(const string& tableName) {
    auto it = tables.find(tableName);
    return (it != tables.end()) ? it->second.get() : nullptr;
}

void Database::listTables() {
    cout << "Tables in database '" << name << "':\n";
    for (const auto& table : tables) {
        cout << "- " << table.first << endl;
    }
}

void Database::saveMetadata() {
    ofstream file("database_metadata.db", ios::binary | ios::trunc);
    if (!file) {
        cerr << "Error: Cannot open metadata file.\n";
        return;
    }

    try {
        size_t numTables = tables.size();
        file.write(reinterpret_cast<char*>(&numTables), sizeof(numTables));

        for (const auto& table : tables) {
            size_t nameLen = table.first.size();
            file.write(reinterpret_cast<char*>(&nameLen), sizeof(nameLen));
            file.write(table.first.c_str(), nameLen);
        }
    } catch (const exception& e) {
        cerr << "Error while saving metadata: " << e.what() << endl;
    }

    file.close();
}

void Database::loadMetadata() {
    ifstream file("database_metadata.db", ios::binary);
    if (!file) {
        cerr << "Metadata file not found. Creating a new database.\n";
        return;
    }

    try {
        size_t numTables;
        if (!file.read(reinterpret_cast<char*>(&numTables), sizeof(numTables))) {
            cerr << "Error: Failed to read number of tables from metadata.\n";
            return;
        }

        for (size_t i = 0; i < numTables; ++i) {
            size_t nameLen;
            if (!file.read(reinterpret_cast<char*>(&nameLen), sizeof(nameLen))) {
                cerr << "Error: Failed to read table name length from metadata.\n";
                return;
            }

            string tableName(nameLen, ' ');
            if (!file.read(&tableName[0], nameLen)) {
                cerr << "Error: Failed to read table name from metadata.\n";
                return;
            }

            tables[tableName] = make_unique<Table>(tableName);
        }
        file.close();
    } catch (const std::exception& e) {
        cerr << "Error occurred while loading metadata: " << e.what() << endl;
    }
}
