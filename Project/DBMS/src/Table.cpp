#include "Table.h"
#include"Column.h"
Table::Table(const string& name) : name(name){}
void Table::addColumn(const string& columnName, const string& columnType, bool isPrimaryKey) {
    columns[columnName] = Column(columnName, columnType, isPrimaryKey);
}

void Table::displayTable() const
{
    cout << "Table Name: " << name << endl;
    for (const auto& column : columns)
    {
        column.second.displayColumns();
    }
}

bool Table::hasColumn(const string& columnName) const
{
    return columns.find(columnName) != columns.end();
}

void Table::insertRecord(int primaryKey, std::string data) {
    index.insert(primaryKey, data);
}

void Table::deleteRecord(int primaryKey) {
    index.remove(primaryKey);
}

std::string Table::searchRecord(int primaryKey) {
    return index.search(primaryKey);
}
