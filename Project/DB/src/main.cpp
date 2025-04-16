#include <iostream>
#include <conio.h>
#include <vector>
#include <limits>
#include <sstream>
#include "include/DB_Manager.h"

using namespace std;

// Utility functions
void clearInputBuffer() {
    cin.clear();
    cin.ignore(numeric_limits<streamsize>::max(), '\n');
}

void pressAnyKeyToContinue() {
    cout << "\nPress any key to continue...";
    _getch();
}

void displayHeader(const string& title) {
    system("cls");
    cout << "=======================================\n";
    cout << "           " << title << "\n";
    cout << "=======================================\n";
}

// Menu display functions
void displayMainMenu(int selected) {
    displayHeader("DATABASE MANAGEMENT");
    
    const string options[] = {
        "Database Operations",
        "Table Operations",
        "Data Operations",
        "Current database check",
        "Exit"

    };
    
    for (int i = 0; i < 5; i++) {
        cout << (i == selected ? " > " : "   ") << options[i] << "\n";
    }
}

void displayDatabaseMenu(int selected) {
    displayHeader("DATABASE OPERATIONS");
    
    const string options[] = {
        "Create Database",
        "List Databases",
        "Switch Database",
        "Delete Database",
        "Back to Main Menu"
    };
    
    for (int i = 0; i < 5; i++) {
        cout << (i == selected ? " > " : "   ") << options[i] << "\n";
    }
}

void displayTableMenu(int selected) {
    displayHeader("TABLE OPERATIONS");
    
    const string options[] = {
        "Create Table",
        "List Tables",
        "Describe Table",
        "Drop Table",
        "Back to Main Menu"
    };
    
    for (int i = 0; i < 5; i++) {
        cout << (i == selected ? " > " : "   ") << options[i] << "\n";
    }
}

void displayDataMenu(int selected) {
    displayHeader("DATA MENU");

    const string options[] = {
        "Insert Record",
        "Search Record",
        "Display Table",        // <-- newly added
        "Delete Record",        // <-- re-enabled
        "Back"
    };

    for (int i = 0; i < 5; i++) {
        cout << (i == selected ? " > " : "   ") << options[i] << "\n";
    }
}

// Operation handlers
void handleCreateDatabase(DatabaseManager& dbManager) {
    string dbName;
    cout << "Enter Database Name: ";
    getline(cin, dbName);
    
    if (!dbName.empty()) {
        dbManager.createDatabase(dbName);
        cout << "Database created successfully.\n";
    } else {
        cout << "Error: Database name cannot be empty.\n";
    }
}

void handleSwitchDatabase(DatabaseManager& dbManager, string& currentDb) {
    string dbName;
    cout << "Enter Database Name: ";
    getline(cin, dbName);
    
    if (!dbName.empty()) {
        dbManager.switchDatabase(dbName);
        currentDb = dbName;
        cout << "Switched to database '" << dbName << "'\n";
    } else {
        cout << "Error: Database name cannot be empty.\n";
    }
}

void handleCreateTable(DatabaseManager& dbManager, const string& currentDb) {
    if (currentDb.empty()) {
        cout << "Error: No database selected.\n";
        return;
    }

    string tableName;
    cout << "Enter Table Name: ";
    getline(cin, tableName);

    if (tableName.empty()) {
        cout << "Error: Table name cannot be empty.\n";
        return;
    }

    // Get columns
    vector<string> colNames;
    vector<FieldType> colTypes;
    vector<int> pkIndices;
    string primaryKey;
    
    cout << "Enter column definitions (name type) one per line (empty to finish):\n";
    cout << "Types: INT, FLOAT, STRING, BOOL\n";
    cout << "Mark primary key with * after type (e.g., 'id INT *')\n";
    
    string line;
    while (true) {
        cout << "Column " << colNames.size()+1 << ": ";
        getline(cin, line);
        if (line.empty()) break;
        
        istringstream iss(line);
        string name, type, pkMarker;
        iss >> name >> type >> pkMarker;
        
        FieldType ftype;
        if (type == "INT") ftype = FieldType::INT;
        else if (type == "FLOAT") ftype = FieldType::FLOAT;
        else if (type == "STRING") ftype = FieldType::STRING;
        else if (type == "BOOL") ftype = FieldType::BOOL;
        else {
            cout << "Invalid type. Try again.\n";
            continue;
        }
        
        colNames.push_back(name);
        colTypes.push_back(ftype);
        
        if (!pkMarker.empty() && pkMarker == "*") {
            pkIndices.push_back(colNames.size()-1);
            primaryKey = name;
        }
    }

    if (colNames.empty()) {
        cout << "Error: Table must have at least one column.\n";
        return;
    }

    // Create vectors for unused parameters
    vector<bool> isForeign(colNames.size(), false);
    vector<string> refTables(colNames.size(), "");
    vector<string> refCols(colNames.size(), "");

    if (dbManager.createTable(tableName, colNames, colTypes, pkIndices, 
                            isForeign, refTables, refCols, primaryKey)) {
        cout << "Table created successfully.\n";
    } else {
        cout << "Error: Failed to create table.\n";
    }
}

void handleInsertRecord(DatabaseManager& dbManager, const string& currentDb) {
    if (currentDb.empty()) {
        cout << "Error: No database selected.\n";
        return;
    }

    string tableName;
    cout << "Enter Table Name: ";
    getline(cin, tableName);

    auto schema = dbManager.getTableSchema(tableName);
    if (schema.columns.empty()) {
        cout << "Error: Table not found.\n";
        return;
    }

    Record record;
    for (const auto& col : schema.columns) {
        cout << "Enter value for " << col.col_name << " (" << 
            (static_cast<FieldType>(col.col_type) == FieldType::INT ? "INT" : 
             static_cast<FieldType>(col.col_type) == FieldType::FLOAT ? "FLOAT" :
             static_cast<FieldType>(col.col_type) == FieldType::STRING ? "STRING" : "BOOL") << "): ";
        
        string value;
        getline(cin, value);
        
        FieldValue fvalue;
        switch (static_cast<FieldType>(col.col_type)) {
            case FieldType::INT: fvalue = stoi(value); break;
            case FieldType::FLOAT: fvalue = stof(value); break;
            case FieldType::STRING: fvalue = value; break;
            case FieldType::BOOL: fvalue = (value == "true" || value == "1"); break;
        }
        
        record[col.col_name] = fvalue;
    }

    if (dbManager.insertRecord(tableName, record)) {
        cout << "Record inserted successfully.\n";
    } else {
        cout << "Error: Failed to insert record.\n";
    }
}

void handleSearchRecords(DatabaseManager& dbManager, const string& currentDb) {
    if (currentDb.empty()) {
        cout << "Error: No database selected.\n";
        return;
    }
    auto tables = dbManager.listTables();
                        cout << "Tables in current database:\n";
                        for (const auto& table : tables) {
                            cout << " - " << table << "\n";
                        }
    string tableName;
    
    cout << "Enter Table Name: ";
    getline(cin, tableName);

    auto schema = dbManager.getTableSchema(tableName);
    if (schema.columns.empty()) {
        cout << "Error: Table not found.\n";
        return;
    }

    string columnName;
    cout << "Enter Column Name to search: ";
    getline(cin, columnName);

    cout << "Enter Search Value: ";
    string value;
    getline(cin, value);

    // Determine column type
    FieldType ftype = FieldType::STRING;
    for (const auto& col : schema.columns) {
        if (col.col_name == columnName) {
            ftype = static_cast<FieldType>(col.col_type);
            break;
        }
    }

    FieldValue searchValue;
    try {
        switch (ftype) {
            case FieldType::INT: searchValue = stoi(value); break;
            case FieldType::FLOAT: searchValue = stof(value); break;
            case FieldType::STRING: searchValue = value; break;
            case FieldType::BOOL: searchValue = (value == "true" || value == "1"); break;
        }
    } catch (...) {
        cout << "Error: Invalid value for column type.\n";
        return;
    }

    auto results = dbManager.searchRecords(tableName, columnName, searchValue);
    if (results.empty()) {
        cout << "No records found.\n";
        return;
    }

    cout << "\nSearch Results (" << results.size() << " records):\n";
    for (const auto& record : results) {
        for (const auto& [key, val] : record) {
            cout << key << ": ";
            val.print();
            cout << " | ";
        }
        cout << "\n";
    }
}

// Main program flow
int main() {
    DatabaseManager dbManager;
    string currentDb;
    int currentMenu = 0; // 0=main, 1=db, 2=table, 3=data
    int selection = 0;

    while (true) {
        // Display appropriate menu
        switch (currentMenu) {
            case 0: displayMainMenu(selection); break;
            case 1: displayDatabaseMenu(selection); break;
            case 2: displayTableMenu(selection); break;
            case 3: displayDataMenu(selection); break;
        }

        // Handle input
        int key = _getch();
        if (key == 224) { // Arrow keys
            key = _getch();
            int maxOptions = (currentMenu == 0 ? 4 : 
                             (currentMenu == 1 ? 4 : 
                             (currentMenu == 2 ? 4 : 4))); // Updated for data menu

            if (key == 72) selection = (selection == 0) ? maxOptions : selection - 1; // Up
            if (key == 80) selection = (selection == maxOptions) ? 0 : selection + 1; // Down
        } 
        else if (key == 13) { // Enter
            system("cls");

            if (currentMenu == 0) { // Main menu
                switch (selection) {
                    case 0: currentMenu = 1; selection = 0; break;
                    case 1: currentMenu = 2; selection = 0; break;
                    case 2: currentMenu = 3; selection = 0; break;
                    case 3: {
                        dbManager.displayCurrentDatabase(currentDb);
                        pressAnyKeyToContinue(); 
                        break;
                    }
                    case 4: return 0; // Exit
                }
            } 
            else if (currentMenu == 1) { // Database menu
                switch (selection) {
                    case 0: handleCreateDatabase(dbManager); break;
                    case 1: {
                        auto dbs = dbManager.listDatabases();
                        if (dbs.empty()) {
                            std::cout << "No databases found.\n";
                        } else {
                            std::cout << "Databases:\n";
                            for (const auto& db : dbs) {
                                std::cout << " - " << db << "\n";
                            }
                        }
                        break;
                    }
                    case 2: handleSwitchDatabase(dbManager, currentDb); break;
                    case 3: {
                        auto dbs = dbManager.listDatabases();
                        if (dbs.empty()) {
                            std::cout << "No databases found.\n";
                        } else {
                            std::cout << "Databases:\n";
                            for (const auto& db : dbs) {
                                std::cout << " - " << db << "\n";
                            }
                        }
                        string dbName;
                        cout << "Enter Database Name to Delete: ";
                        getline(cin, dbName);
                        dbManager.deleteDatabase(dbName);
                        if (currentDb == dbName) currentDb.clear();
                        break;
                    }
                    case 4: currentMenu = 0; selection = 0; break;
                }
                pressAnyKeyToContinue();
            }
            else if (currentMenu == 2) { // Table menu
                switch (selection) {
                    case 0: handleCreateTable(dbManager, currentDb); break;
                    case 1: {
                        auto tables = dbManager.listTables();
                        cout << "Tables in current database:\n";
                        for (const auto& table : tables) {
                            cout << " - " << table << "\n";
                        }
                        break;
                    }
                    case 2: {
                        auto tables = dbManager.listTables();
                        cout << "Tables in current database:\n";
                        for (const auto& table : tables) {
                            cout << " - " << table << "\n";
                        }
                        string tableName;
                        cout << "Enter Table Name: ";
                        getline(cin, tableName);
                        auto schema = dbManager.getTableSchema(tableName);
                        cout << "Table Structure:\n";
                        for (const auto& col : schema.columns) {
                            cout << col.col_name << " (" << 
                                (static_cast<FieldType>(col.col_type) == FieldType::INT ? "INT" : 
                                static_cast<FieldType>(col.col_type) == FieldType::FLOAT ? "FLOAT" :
                                static_cast<FieldType>(col.col_type) == FieldType::STRING ? "STRING" : "BOOL");
                            if (col.is_primary) cout << " [PRIMARY KEY]";
                            cout << "\n";
                        }
                        break;
                    }
                    case 3: {
                        auto tables = dbManager.listTables();
                        cout << "Tables in current database:\n";
                        for (const auto& table : tables) {
                            cout << " - " << table << "\n";
                        }
                        string tableName;
                        cout << "Enter Table Name to Drop: ";
                        getline(cin, tableName);
                        if (dbManager.dropTable(tableName)) {
                            cout << "Table dropped successfully.\n";
                        } else {
                            cout << "Error: Failed to drop table.\n";
                        }
                        break;
                    }
                    case 4: currentMenu = 0; selection = 0; break;
                }
                pressAnyKeyToContinue();
            }
            else if (currentMenu == 3) { // Data menu
                switch (selection) {
                    case 0: handleInsertRecord(dbManager, currentDb); break;

                    case 1: handleSearchRecords(dbManager, currentDb); break;

                    case 2: {
                        auto tables = dbManager.listTables();
                        cout << "Tables in current database:\n";
                        for (const auto& table : tables) {
                            cout << " - " << table << "\n";
                        }
                        std::string tableName;
                        std::cout << "Enter Table Name to Display: ";
                        std::getline(std::cin, tableName);
                        dbManager.displayTable(tableName);
                        break;
                    }

                    case 3: {
                        auto tables = dbManager.listTables();
                        cout << "Tables in current database:\n";
                        for (const auto& table : tables) {
                            cout << " - " << table << "\n";
                        }
                        string tableName, columnName, value;
                        cout << "Enter Table Name: ";
                        getline(cin, tableName);
                        cout << "Enter Column Name: ";
                        getline(cin, columnName);
                        cout << "Enter Value to Delete: ";
                        getline(cin, value);

                        auto schema = dbManager.getTableSchema(tableName);
                        FieldType ftype = FieldType::STRING;
                        for (const auto& col : schema.columns) {
                            if (col.col_name == columnName) {
                                ftype = static_cast<FieldType>(col.col_type);
                                break;
                            }
                        }

                        FieldValue fvalue;
                        try {
                            switch (ftype) {
                                case FieldType::INT: fvalue = stoi(value); break;
                                case FieldType::FLOAT: fvalue = stof(value); break;
                                case FieldType::STRING: fvalue = value; break;
                                case FieldType::BOOL: fvalue = (value == "true" || value == "1"); break;
                            }
                        } catch (...) {
                            cout << "Error: Invalid value for column type.\n";
                            break;
                        }

                        if (dbManager.deleteRecord(tableName, columnName, fvalue)) {
                            cout << "Record deleted successfully.\n";
                        } else {
                            cout << "Error: Failed to delete record.\n";
                        }
                        break;
                    }

                    case 4: currentMenu = 0; selection = 0; break;
                }
                pressAnyKeyToContinue();
            }
        }
        else if (key == 27) { // ESC - go back
            if (currentMenu != 0) {
                currentMenu = 0;
                selection = 0;
            }
        }
    }

    return 0;
}
