<<<<<<< Updated upstream
<<<<<<< Updated upstream
#ifndef TABLE_H
#define TABLE_H
#include "Column.h"
#include "BPLusTree.h"
#include <string>
#include <iostream>
#include <unordered_map>
using namespace std;
class Table
{
private:
    string name;
    unordered_map<string, Column> columns;
    BPlusTree index; // Index on primary key


public:
    Table(const string &name);

    void addColumn(const string &columnName, const string &columnType, bool isPrimaryKey = false);
    void displayTable() const;
    bool hasColumn(const string &columnName) const;

    void insertRecord(int primaryKey, std::string data);
    void deleteRecord(int primaryKey);
    std::string searchRecord(int primaryKey);


=======
=======
>>>>>>> Stashed changes
// #ifndef TABLE_H
// #define TABLE_H

// #include <string>
// #include "Column.h"
// #include "BPlusTree.h"

// class Table {
// private:
//     std::string name;
//     std::vector<Column> columns;
//     BPlusTree index;

// public:
//     Table()=default;
//     Table(std::string tableName);
//     void insertRecord(int key, const std::string& data);
//     std::string searchRecord(int key);
//     void deleteRecord(int key);
//     void addColumn(const std::string& name, const std::string& type, bool isPrimaryKey);
//     void saveToDisk();
//     void loadFromDisk();

// };

// #endif
#ifndef TABLE_H
#define TABLE_H

#include <string>
#include <vector>
#include <fstream>
#include <iostream>
#include "Column.h"
#include "BPlusTree.h"

class Table {
private:
    std::string name;               // Name of the table
    std::vector<Column> columns;    // List of columns
    BPlusTree index;                // B+Tree index for records
    std::string primaryKeyColumn;   // Name of the primary key column

public:
    Table() = default;
    Table(const std::string& tableName);

    // Record management
    void insertRecord(int key, const std::string& data);
    std::string searchRecord(int key);
    void deleteRecord(int key);

    // Column management
    void addColumn(const std::string& name, const std::string& type, bool isPrimaryKey);

    // Disk I/O
    void saveToDisk();
    void loadFromDisk();

private:
    // Helper functions
    bool validatePrimaryKey(int key);
    void saveColumnsToDisk(std::ofstream& file);
    void loadColumnsFromDisk(std::ifstream& file);
<<<<<<< Updated upstream
>>>>>>> Stashed changes
=======
>>>>>>> Stashed changes
};

#endif