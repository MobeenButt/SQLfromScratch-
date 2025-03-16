#include "Database.h"
#include <iostream>
#include <string>
#include <fstream>
#ifdef _WIN32
    #include <direct.h>
    #define mkdir(path) _mkdir(path)
#else
    #include <sys/stat.h>
#endif
#include "json.hpp"

void recordMenu(Database& db,json& meta);

using json = nlohmann::json;
using namespace std;
// namespace fs = filesystem;

const string METADATA_FILE = "metadata.json";
const string DATABASES_FOLDER = "databases/";

void saveMetadata(const json& metadata) {
    ofstream file(METADATA_FILE);
    if (file.is_open()) {
        file << metadata.dump(4); // Pretty-print JSON
        file.close();
    }
}

json loadMetadata() {
    ifstream file(METADATA_FILE);
    json metadata;
    if (file.is_open()) {
        file >> metadata;
        file.close();
    }
    return metadata;
}

void saveTableData(const string& dbName, const string& tableName, const json& tableData) {
    string filePath = DATABASES_FOLDER + dbName + "/" + tableName + ".json";
    ofstream file(filePath);
    if (file.is_open()) {
        file << tableData.dump(4);
        file.close();
        cout << "Data saved to: " << filePath << endl;  // Debug message
    } else {
        cerr << "Error: Could not open file " << filePath << " for writing!" << endl;
    }
    
}

json loadTableData(const string& dbName, const string& tableName) {
    string filePath = DATABASES_FOLDER + dbName + "/" + tableName + ".json";
    ifstream file(filePath);
    json tableData;
    if (file.is_open()) {
        file >> tableData;
        file.close();
    }
    return tableData;
}

void showMenu() {
    cout << "\nDatabase Management System" << endl;
    cout << "1. Create a new database" << endl;
    cout << "2. Create a new table" << endl;
    cout << "3. Add a column to a table" << endl;
    cout << "4. Insert data into table" << endl;
    cout << "5. Show table schema" << endl;
    cout << "6. Manage Records" << endl;
    cout << "7. Exit" << endl;
    cout << "Enter your choice: ";
}

int main() {
    Database db;
    json metadata = loadMetadata();
    int choice;
    string dbName, tableName, columnName, dataType;
    bool isPrimaryKey;
    
    while (true) {
        showMenu();
        cin >> choice;
        cin.ignore(); // Clear newline character

        switch (choice) {
            case 1:
                cout << "Enter database name: ";
                getline(cin, dbName);
                if (!metadata["databases"].contains(dbName)) {
                    metadata["databases"][dbName] = json::object();
                    mkdir((DATABASES_FOLDER + dbName).c_str());
                    saveMetadata(metadata);

                    cout << "Database '" << dbName << "' created successfully.\n";
                } else {
                    cout << "Database already exists!\n";
                }
                break;
            
            case 2:
                cout << "Enter database name: ";
                getline(cin, dbName);
                if (!metadata["databases"].contains(dbName)) {
                    cout << "Database not found!\n";
                    break;
                }
                cout << "Enter table name: ";
                getline(cin, tableName);
                db.createTable(tableName);
                metadata["databases"][dbName]["tables"][tableName] = json::object();
                saveMetadata(metadata);
                cout << "Table '" << tableName << "' created successfully in database '" << dbName << "'.\n";
                break;
            
            case 3:
                cout << "Enter database name: ";
                getline(cin, dbName);
                cout << "Enter table name: ";
                getline(cin, tableName);
                cout << "Enter column name: ";
                getline(cin, columnName);
                cout << "Enter data type (int/string/etc.): ";
                getline(cin, dataType);
                cout << "Is this column a primary key? (1 for Yes, 0 for No): ";
                cin >> isPrimaryKey;
                cin.ignore();
                db.addColumnToTable(tableName, columnName, dataType, isPrimaryKey);
                metadata["databases"][dbName]["tables"][tableName][columnName] = {
                    {"type", dataType},
                    {"primary_key", isPrimaryKey}
                };
                saveMetadata(metadata);
                cout << "Column '" << columnName << "' added to table '" << tableName << "' in database '" << dbName << "'.\n";
                break;
            
                case 4: {
                    cout << "Enter database name: ";
                    getline(cin, dbName);
                    cout << "Enter table name: ";
                    getline(cin, tableName);
                    if (!metadata["databases"].contains(dbName) || !metadata["databases"][dbName]["tables"].contains(tableName)) {
                        cout << "Table not found!\n";
                        break;
                    }
                    json newData;
                    for (auto& col : metadata["databases"][dbName]["tables"][tableName].items()) {
                        string value;
                        cout << "Enter " << col.key() << " (" << col.value()["type"].get<string>() << "): ";
                        getline(cin, value);
                        newData[col.key()] = value;
                    }
                    json tableData = loadTableData(dbName, tableName);
                    tableData.push_back(newData);
                    saveTableData(dbName, tableName, tableData);
                    cout << "Data inserted successfully!\n";
                    break;
                }
                
            
            case 5:
                cout << "Enter database name: ";
                getline(cin, dbName);
                cout << "Enter table name: ";
                getline(cin, tableName);
                if (metadata["databases"].contains(dbName) && metadata["databases"][dbName]["tables"].contains(tableName)) {
                    cout << "Schema for table '" << tableName << "' in database '" << dbName << "':\n";
                    for (auto& col : metadata["databases"][dbName]["tables"][tableName].items()) {
                        cout << "Column: " << col.key() << " | Type: " << col.value()["type"].get<string>() << " | Primary Key: " << (col.value()["primary_key"].get<bool>() ? "Yes" : "No") << "\n";
                    }
                } else {
                    cout << "Table not found!\n";
                }
                break;
            
            case 6:
                recordMenu(db,metadata);
                break;    
            case 7:
                cout << "Exiting...\n";
                return 0;
            
            default:
                cout << "Invalid choice. Try again.\n";
        }
    }
}

void recordMenu(Database& db, json& metadata) {
    string dbName, tableName;
    int primaryKey;
    string value;

    cout << "Enter database name: ";
    getline(cin, dbName);

    // Validate database existence
    if (!metadata["databases"].contains(dbName)) {
        cout << "Database not found!\n";
        return;
    }

    while (true) {
        cout << "\nRecord Management for Database: " << dbName << "\n";
        cout << "1. Insert Record\n";
        cout << "2. Search Record\n";
        cout << "3. Delete Record\n";
        cout << "4. Back to Main Menu\n";
        cout << "Enter your choice: ";

        int choice;
        cin >> choice;
        cin.ignore();

        switch (choice) {
            case 1:
                cout << "Enter table name: ";
                getline(cin, tableName);
                if (!metadata["databases"][dbName]["tables"].contains(tableName)) {
                    cout << "Table not found!\n";
                    break;
                }
                cout << "Enter primary key: ";
                cin >> primaryKey;
                cout << "Enter data: ";
                cin.ignore();
                getline(cin, value);
                db.getTable(tableName)->insertRecord(primaryKey, value);
                cout << "Record inserted into '" << tableName << "'.\n";
                break;

            case 2:
                cout << "Enter table name: ";
                getline(cin, tableName);
                if (!metadata["databases"][dbName]["tables"].contains(tableName)) {
                    cout << "Table not found!\n";
                    break;
                }
                cout << "Enter primary key: ";
                cin >> primaryKey;
                db.getTable(tableName)->searchRecord(primaryKey);
                break;

            case 3:
                cout << "Enter table name: ";
                getline(cin, tableName);
                if (!metadata["databases"][dbName]["tables"].contains(tableName)) {
                    cout << "Table not found!\n";
                    break;
                }
                cout << "Enter primary key: ";
                cin >> primaryKey;
                db.getTable(tableName)->deleteRecord(primaryKey);
                cout << "Record deleted from '" << tableName << "'.\n";
                break;

            case 4:
                return;

            default:
                cout << "Invalid choice! Try again.\n";
        }
    }
}
