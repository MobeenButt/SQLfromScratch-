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


};

#endif
