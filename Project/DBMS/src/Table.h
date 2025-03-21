#ifndef TABLE_H
#define TABLE_H

#include <string>
#include <vector>
#include "Column.h"

class Table {
private:
    std::string name;
    std::vector<Column> columns;
    std::string primaryKeyColumn;

public:
    Table(const std::string& tableName);
    void addColumn(const std::string& name, const std::string& type, bool isPrimaryKey = false);
    void insertRecord(int key, const std::string& data); // Temporary remove index functionality
    std::string searchRecord(int key); // Temporary remove index functionality
    void deleteRecord(int key); // Temporary remove index functionality
    void displayTable() const;
    void saveToDisk();
    void loadFromDisk();
    bool validatePrimaryKey(int key); // Temporary remove index validation
    void saveColumnsToDisk(std::ofstream& file);
    void loadColumnsFromDisk(std::ifstream& file);
};

#endif
