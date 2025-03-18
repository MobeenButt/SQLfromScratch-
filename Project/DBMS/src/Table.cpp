// #include "Table.h"

// // Constructor
// Table::Table(const std::string& tableName) : name(tableName), index(4) {
//     loadFromDisk(); // Load existing records and columns
// }

// // Insert a record
// void Table::insertRecord(int key, const std::string& data) {
//     if (!validatePrimaryKey(key)) {
//         std::cerr << "Error: Duplicate primary key '" << key << "'.\n";
//         return;
//     }
//     index.insert(key, data);
// }

// // Search for a record
// std::string Table::searchRecord(int key) {
//     return index.search(key);
// }

// // Delete a record
// void Table::deleteRecord(int key) {
//     index.remove(key);
// }

// // Add a column to the table
// void Table::addColumn(const std::string& name, const std::string& type, bool isPrimaryKey) {
//     if (isPrimaryKey && !primaryKeyColumn.empty()) {
//         std::cerr << "Error: Table already has a primary key column.\n";
//         return;
//     }
//     columns.emplace_back(name, type, isPrimaryKey);
//     if (isPrimaryKey) {
//         primaryKeyColumn = name;
//     }
//     std::cout << "Column '" << name << "' added to table '" << this->name << "'.\n";
// }

// // Save table data and metadata to disk
// void Table::saveToDisk() {
//     std::ofstream file("tables/" + name + ".db", std::ios::binary | std::ios::trunc);
//     if (!file) {
//         std::cerr << "Error: Cannot open table file for writing.\n";
//         return;
//     }

//     // Save columns
//     saveColumnsToDisk(file);

//     // Save records
//     index.saveToDisk(file);

//     file.close();
// }

// // Load table data and metadata from disk
// void Table::loadFromDisk() {
//     std::ifstream file("tables/" + name + ".db", std::ios::binary);
//     if (!file) {
//         std::cerr << "Error: Cannot open table file for reading.\n";
//         return;
//     }

//     // Load columns
//     loadColumnsFromDisk(file);

//     // Load records
//     index.loadFromDisk(file);

//     file.close();
// }

// // Validate primary key uniqueness
// bool Table::validatePrimaryKey(int key) {
//     return index.search(key).empty();
// }

// // Save columns to disk
// void Table::saveColumnsToDisk(std::ofstream& file) {
//     for (const auto& column : columns) {
//         file << column.getName() << " " << column.getType() << " " << column.isPrimaryKey() << "\n";
//     }
// }

// // Load columns from disk
// void Table::loadColumnsFromDisk(std::ifstream& file) {
//     std::string name, type;
//     bool isPrimaryKey;
//     while (file >> name >> type >> isPrimaryKey) {
//         columns.emplace_back(name, type, isPrimaryKey);
//         if (isPrimaryKey) {
//             primaryKeyColumn = name;
//         }
//     }
// }

#include "Table.h"

// Constructor
Table::Table(const std::string& tableName) : name(tableName), index(4) {
    loadFromDisk(); // Load existing records and columns
}

// Insert a record
void Table::insertRecord(int key, const std::string& data) {
    if (!validatePrimaryKey(key)) {
        std::cerr << "Error: Duplicate primary key '" << key << "'.\n";
        return;
    }
    index.insert(key, data);
    saveToDisk();  // Save after insertion
}

// Search for a record
std::string Table::searchRecord(int key) {
    return index.search(key);
}

// Delete a record
void Table::deleteRecord(int key) {
    index.remove(key);
    saveToDisk();  // Save after deletion
}

// Add a column to the table
void Table::addColumn(const std::string& name, const std::string& type, bool isPrimaryKey) {
    if (isPrimaryKey && !primaryKeyColumn.empty()) {
        std::cerr << "Error: Table already has a primary key column.\n";
        return;
    }
    columns.emplace_back(name, type, isPrimaryKey);
    if (isPrimaryKey) {
        primaryKeyColumn = name;
    }
    std::cout << "Column '" << name << "' added to table '" << this->name << "'.\n";
    saveToDisk();  // Save after adding column
}

// Save table data and metadata to disk
void Table::saveToDisk() {
    std::ofstream file("tables/" + name + ".db", std::ios::binary | std::ios::trunc);
    if (!file) {
        std::cerr << "Error: Cannot open table file for writing.\n";
        return;
    }

    // Save columns
    saveColumnsToDisk(file);

    // Save records
    index.saveToDisk("tables/" + name + "_index.db");  // Save index separately

    file.close();
}

// Load table data and metadata from disk
void Table::loadFromDisk() {
    std::ifstream file("tables/" + name + ".db", std::ios::binary);
    if (!file) {
        std::cerr << "Error: Cannot open table file for reading.\n";
        return;
    }

    // Load columns
    loadColumnsFromDisk(file);

    // Load records
    index.loadFromDisk("tables/" + name + "_index.db");  // Load index separately

    file.close();
}

// Validate primary key uniqueness
bool Table::validatePrimaryKey(int key) {
    return index.search(key).empty();  // Returns true if key does NOT exist
}

// Save columns to disk (Binary Mode)
void Table::saveColumnsToDisk(std::ofstream& file) {
    size_t columnCount = columns.size();
    file.write(reinterpret_cast<char*>(&columnCount), sizeof(columnCount));

    for (const auto& column : columns) {
        size_t nameLen = column.getName().size();
        file.write(reinterpret_cast<char*>(&nameLen), sizeof(nameLen));
        file.write(column.getName().c_str(), nameLen);

        size_t typeLen = column.getType().size();
        file.write(reinterpret_cast<char*>(&typeLen), sizeof(typeLen));
        file.write(column.getType().c_str(), typeLen);

        bool isPrimaryKey = column.isPrimaryKey();
        file.write(reinterpret_cast<char*>(&isPrimaryKey), sizeof(isPrimaryKey));
    }
}

// Load columns from disk (Binary Mode)
void Table::loadColumnsFromDisk(std::ifstream& file) {
    size_t columnCount;
    file.read(reinterpret_cast<char*>(&columnCount), sizeof(columnCount));

    for (size_t i = 0; i < columnCount; ++i) {
        size_t nameLen, typeLen;
        file.read(reinterpret_cast<char*>(&nameLen), sizeof(nameLen));

        std::string name(nameLen, ' ');
        file.read(&name[0], nameLen);

        file.read(reinterpret_cast<char*>(&typeLen), sizeof(typeLen));
        std::string type(typeLen, ' ');
        file.read(&type[0], typeLen);

        bool isPrimaryKey;
        file.read(reinterpret_cast<char*>(&isPrimaryKey), sizeof(isPrimaryKey));

        columns.emplace_back(name, type, isPrimaryKey);
        if (isPrimaryKey) {
            primaryKeyColumn = name;
        }
    }
}
