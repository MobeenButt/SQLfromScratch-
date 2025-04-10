#include <iostream>
#include <conio.h>  // For _getch()
#include "include/DB_Manager.h"  // Ensure this exists and contains database functions

using namespace std;

// Function to display the menu
void displayMenu(int selected) {
    system("cls");  // Clears console (Windows only)
    cout << "=======================================\n";
    cout << "           DATABASE MANAGEMENT         \n";
    cout << "=======================================\n";
    
    string options[] = {"Create Database", "Show Databases", "Switch Database", "Exit"};
    for (int i = 0; i < 4; i++) {
        if (i == selected)
            cout << " o>  " << options[i] << "\n";  // Highlight selection
        else
            cout << "   " << options[i] << "\n";
    }
}

int main() {
    DatabaseManager dbManager;  // Assuming this handles DB operations

    int selected = 0;
    while (true) {
        displayMenu(selected);
        char key = _getch();  // Read input without Enter key
        
        if (key == 72) selected = (selected == 0) ? 3 : selected - 1;  // Up Arrow (ASCII 72)
        if (key == 80) selected = (selected == 3) ? 0 : selected + 1;  // Down Arrow (ASCII 80)
        if (key == 13) {  // Enter Key (ASCII 13)
            system("cls");

            if (selected == 0) {  // Create Database
                string dbName;
                cout << "Enter Database Name: ";
                cin >> dbName;
                dbManager.createDatabase(dbName);
            } 
            else if (selected == 1) {  // Show Databases
                dbManager.listDatabases();
            } 
            else if (selected == 2) {  // Switch Database
                string dbName;
                cout << "Enter Database Name to Switch: ";
                cin >> dbName;
                dbManager.switchDatabase(dbName);
            } 
            else if (selected == 3) {  // Exit
                cout << "Exiting...\n";
                break;
            }

            cout << "\nPress any key to return to the menu...";
            _getch();
        }
    }
    return 0;
}
