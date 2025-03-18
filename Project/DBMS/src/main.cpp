// main.cpp
#include <iostream>
using namespace std;
#include "Database.h"

void showMenu() {
    cout << "\nDatabase System\n";
    cout << "1. Create Database\n";
    cout << "2. Create Table\n";
    cout << "3. Add Column\n";
    cout << "4. Insert Record\n";
    cout << "5. Search Record\n";
    cout << "6. Delete Record\n";
    cout << "7. Exit\n";
    cout << "Enter choice: ";
}

int main() {
    Database db("MyDatabase");
    int choice;

    while (true) {
        showMenu();
        cin >> choice;
        cin.ignore(); // Clear buffer

        string tableName, columnName, type, data;
        int key;

        if (choice == 1) {
            cout << "Database already exists.\n";
        } 
        else if (choice == 2) {
            cout << "Enter table name: ";
            getline(cin, tableName);
            db.createTable(tableName);
            cout << "Table created.\n";
        } 
        else if (choice == 3) {
            cout << "Enter table name: ";
            getline(cin, tableName);
            cout << "Enter column name: ";
            getline(cin, columnName);
            cout << "Enter type (int/string): ";
            getline(cin, type);
            db.getTable(tableName)->addColumn(columnName, type, true);
            cout << "Column added.\n";
        } 
        else if (choice == 4) {
            cout << "Enter table name: ";
            getline(cin, tableName);
            cout << "Enter primary key: ";
            cin >> key;
            cin.ignore();
            cout << "Enter data: ";
            getline(cin, data);
            db.getTable(tableName)->insertRecord(key, data);
            cout << "Record inserted.\n";
        } 
        else if (choice == 5) {
            cout << "Enter table name: ";
            getline(cin, tableName);
            cout << "Enter primary key: ";
            cin >> key;
            db.getTable(tableName)->searchRecord(key);
        } 
        else if (choice == 6) {
            cout << "Enter table name: ";
            getline(cin, tableName);
            cout << "Enter primary key: ";
            cin >> key;
            db.getTable(tableName)->deleteRecord(key);
            cout << "Record deleted.\n";
        } 
        else if (choice == 7) {
            cout << "Exiting...\n";
            return 0;
        } 
        else {
            cout << "Invalid choice. Try again.\n";
        }
    }
}
