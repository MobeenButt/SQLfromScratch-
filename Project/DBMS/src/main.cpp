#include "Database.h"
#include <iostream>
#include <unordered_map>

using namespace std;

void showMainMenu() {
    cout << "\nDatabase System\n";
    cout << "1. Create Database\n";
    cout << "2. Switch Database\n";
    cout << "3. Create Table\n";
    cout << "4. Add Column\n";
    cout << "5. Insert Record\n";
    cout << "6. Search Record\n";
    cout << "7. Delete Record\n";
    cout << "8. Show Tables\n";
    cout << "9. Exit\n";
    cout << "Enter choice: ";
}

int main() {
    unordered_map<string, Database*> databases;
    Database* currentDatabase = nullptr;
    string dbName, tableName, columnName, dataType, data;
    bool isPrimaryKey;
    int key, choice;

    while (true) {
        showMainMenu();
        cin >> choice;
        cin.ignore();

        switch (choice) {
            case 1: // Create Database
                cout << "Enter database name: ";
                getline(cin, dbName);
                if (databases.find(dbName) == databases.end()) {
                    databases[dbName] = new Database(dbName);
                    cout << "Database '" << dbName << "' created successfully.\n";
                } else {
                    cout << "Database '" << dbName << "' already exists!\n";
                }
                break;

            case 2: // Switch Database
                cout << "Enter database name: ";
                getline(cin, dbName);
                if (databases.find(dbName) != databases.end()) {
                    currentDatabase = databases[dbName];
                    cout << "Switched to database '" << dbName << "'.\n";
                } else {
                    cout << "Database '" << dbName << "' not found!\n";
                }
                break;

            case 3: // Create Table
                if (!currentDatabase) {
                    cout << "No database selected. Please switch to a database first.\n";
                    break;
                }
                cout << "Enter table name: ";
                getline(cin, tableName);
                currentDatabase->createTable(tableName);
                break;

            case 4: // Add Column
                if (!currentDatabase) {
                    cout << "No database selected. Please switch to a database first.\n";
                    break;
                }
                cout << "Enter table name: ";
                getline(cin, tableName);
                cout << "Enter column name: ";
                getline(cin, columnName);
                cout << "Enter column type: ";
                getline(cin, dataType);
                cout << "Is primary key? (1 for Yes, 0 for No): ";
                cin >> isPrimaryKey;
                cin.ignore();
                currentDatabase->addColumnToTable(tableName, columnName, dataType, isPrimaryKey);
                break;

            case 5: // Insert Record
                if (!currentDatabase) {
                    cout << "No database selected. Please switch to a database first.\n";
                    break;
                }
                cout << "Enter table name: ";
                getline(cin, tableName);
                cout << "Enter primary key: ";
                cin >> key;
                cin.ignore();
                cout << "Enter data: ";
                getline(cin, data);
                currentDatabase->getTable(tableName)->insertRecord(key, data);
                break;

            case 6: // Search Record
                if (!currentDatabase) {
                    cout << "No database selected. Please switch to a database first.\n";
                    break;
                }
                cout << "Enter table name: ";
                getline(cin, tableName);
                cout << "Enter primary key: ";
                cin >> key;
                cin.ignore();
                cout << "Record: " << currentDatabase->getTable(tableName)->searchRecord(key) << endl;
                break;

            case 7: // Delete Record
                if (!currentDatabase) {
                    cout << "No database selected. Please switch to a database first.\n";
                    break;
                }
                cout << "Enter table name: ";
                getline(cin, tableName);
                cout << "Enter primary key: ";
                cin >> key;
                cin.ignore();
                currentDatabase->getTable(tableName)->deleteRecord(key);
                break;

            case 8: // Show Tables
                if (!currentDatabase) {
                    cout << "No database selected. Please switch to a database first.\n";
                    break;
                }
                currentDatabase->listTables();
                break;

            case 9: // Exit
                cout << "Exiting...\n";
                // Clean up dynamically allocated databases
                for (auto& db : databases) {
                    delete db.second;
                }
                return 0;

            default:
                cout << "Invalid choice. Try again.\n";
        }
    }
}