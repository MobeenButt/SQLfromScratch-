#ifndef COLUMN_H
#define COLUMN_H

#include <string>

class Column {
private:
    std::string name;
    std::string type;
    bool isPrimaryKey;

public:
    Column(const std::string& name = "", const std::string& type = "", bool isPrimaryKey = false);
    std::string getName() const;
    std::string getType() const;
    bool isPrimaryKeyColumn() const;
    void displayColumns() const;
};

#endif