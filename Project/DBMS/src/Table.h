// Table.h
#ifndef TABLE_H
#define TABLE_H

#include <vector>
#include <map>
#include "Column.h"
#include "BPlusTree.h"

class Table {
public:
    string name;
    vector<Column> columns;
    BPlusTree index;

    Table(string name);
    void addColumn(string columnName, string type, bool isPrimaryKey);
    void insertRecord(int primaryKey, string data);
    void searchRecord(int primaryKey);
    void deleteRecord(int primaryKey);
};

#endif
