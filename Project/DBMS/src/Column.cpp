#include "Column.h"
#include <iostream>

using namespace std;

Column::Column(const std::string& name, const std::string& type, bool isPrimaryKey)
    : name(name), type(type), isPrimaryKey(isPrimaryKey) {}

std::string Column::getName() const {
    return name;
}

std::string Column::getType() const {
    return type;
}

bool Column::isPrimaryKeyColumn() const {
    return isPrimaryKey;
}

void Column::displayColumns() const {
    cout << "Column Name: " << name << " | Type: " << type;
    if (isPrimaryKey) {
        cout << " | Primary Key";
    }
    cout << endl;
}