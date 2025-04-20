#include <iostream>
#include <string>
#include <sstream>
#include <vector>
#include <algorithm>
#include <fstream>
#include "database.h"

void printHelp() {
    std::cout << "Available commands:\n"
              << "CREATE DATABASE <name>\n"
              << "USE DATABASE <name>\n"
              << "CREATE TABLE <name> (<column_name> <type>(size),...)\n"
              << "DROP TABLE <name>\n"
              << "INSERT INTO <table> VALUES (value1, value2, ...)\n"
              << "SELECT * FROM <table> [WHERE column_name = value]\n"
              << "UPDATE <table> SET column = value WHERE condition\n"
              << "DELETE FROM <table> WHERE condition\n"
              << "CREATE INDEX ON <table>(<column>)\n"
              << "HELP\n"
              << "EXIT\n";
}

// Helper function to split string by delimiter
std::vector<std::string> split(const std::string& str, char delimiter) {
    std::vector<std::string> tokens;
    std::string token;
    std::istringstream tokenStream(str);
    while (std::getline(tokenStream, token, delimiter)) {
        // Trim whitespace
        token.erase(0, token.find_first_not_of(" \t"));
        token.erase(token.find_last_not_of(" \t") + 1);
        if (!token.empty()) {
            tokens.push_back(token);
        }
    }
    return tokens;
}

// Helper function to parse column definitions
std::vector<ColumnInfo> parseColumnDefs(const std::string& columnDefs) {
    std::vector<ColumnInfo> columns;
    std::vector<std::string> columnTokens = split(columnDefs, ',');
    
    for (const auto& colDef : columnTokens) {
        std::istringstream iss(colDef);
        std::string name, type, sizeStr;
        iss >> name >> type;
        
        // Extract size from type if present (e.g., VARCHAR(20))
        size_t openParen = type.find('(');
        size_t closeParen = type.find(')');
        int size = 0;
        
        if (openParen != std::string::npos && closeParen != std::string::npos) {
            sizeStr = type.substr(openParen + 1, closeParen - openParen - 1);
            type = type.substr(0, openParen);
            size = std::stoi(sizeStr);
        } else if (type == "INT") {
            size = sizeof(int);
        }
        
        columns.push_back({name, type, size});
    }
    return columns;
}

int main() {
    std::string input;
    Database* current_db = nullptr;
    std::string current_db_name;

    std::cout << "Simple DBMS v1.0\n";
    std::cout << "Type 'HELP' for commands\n";

    while (true) {
        std::cout << (current_db_name.empty() ? "dbms" : current_db_name) << "> ";
        std::getline(std::cin, input);

        // Convert input to uppercase for case-insensitive commands
        std::string upperInput = input;
        std::transform(upperInput.begin(), upperInput.end(), upperInput.begin(), ::toupper);

        if (upperInput == "EXIT") {
            break;
        }

        if (upperInput == "HELP") {
            printHelp();
            continue;
        }

        try {
        std::istringstream iss(input);
        std::string command;
        iss >> command;
            std::transform(command.begin(), command.end(), command.begin(), ::toupper);

        if (command == "CREATE") {
            std::string type;
            iss >> type;
                std::transform(type.begin(), type.end(), type.begin(), ::toupper);
            
            if (type == "DATABASE") {
                std::string db_name;
                iss >> db_name;
                    if (db_name.empty()) {
                        std::cout << "Error: Database name required\n";
                        continue;
                    }
                    
                if (current_db) {
                    delete current_db;
                }
                current_db = new Database(db_name);
                    current_db_name = db_name;
                std::cout << "Database created: " << db_name << std::endl;
            }
                else if (type == "TABLE") {
                    if (!current_db) {
                        std::cout << "Error: No database selected\n";
                        continue;
                    }
                    
                    std::string table_name;
                    iss >> table_name;
                    
                    // Read column definitions
                    std::string columnDefs;
                    std::getline(iss, columnDefs);
                    
                    // Remove parentheses
                    size_t first = columnDefs.find('(');
                    size_t last = columnDefs.find_last_of(')');
                    if (first == std::string::npos || last == std::string::npos) {
                        std::cout << "Error: Invalid column definitions format\n";
                        continue;
                    }
                    
                    columnDefs = columnDefs.substr(first + 1, last - first - 1);
                    std::vector<ColumnInfo> columns = parseColumnDefs(columnDefs);
                    
                    if (columns.empty()) {
                        std::cout << "Error: No columns defined\n";
                        continue;
                    }
                    
                    if (current_db->createTable(table_name, columns)) {
                        std::cout << "Table created: " << table_name << std::endl;
                    } else {
                        std::cout << "Error creating table\n";
                    }
                }
                else if (type == "INDEX") {
                    if (!current_db) {
                        std::cout << "Error: No database selected\n";
                        continue;
                    }
                    
                    std::string on, table_name, column_name;
                    iss >> on;
                    if (on != "ON") {
                        std::cout << "Error: Invalid syntax. Use 'CREATE INDEX ON table(column)'\n";
                        continue;
                    }
                    
                    std::string tableAndColumn;
                    iss >> tableAndColumn;
                    
                    size_t openParen = tableAndColumn.find('(');
                    size_t closeParen = tableAndColumn.find(')');
                    
                    if (openParen == std::string::npos || closeParen == std::string::npos) {
                        std::cout << "Error: Invalid syntax. Use 'CREATE INDEX ON table(column)'\n";
                        continue;
                    }
                    
                    table_name = tableAndColumn.substr(0, openParen);
                    column_name = tableAndColumn.substr(openParen + 1, closeParen - openParen - 1);
                    
                    if (current_db->createIndex(table_name, column_name)) {
                        std::cout << "Index created successfully on " << table_name << "(" << column_name << ")\n";
                        
                        // Verify index file exists
                        std::string index_file = "./data/" + current_db_name + "/" + table_name + "_" + column_name + ".idx";
                        std::ifstream f(index_file.c_str());
                        if (f.good()) {
                            std::cout << "Index file verified at: " << index_file << std::endl;
                        } else {
                            std::cout << "Warning: Index file not found at: " << index_file << std::endl;
                        }
                    } else {
                        std::cout << "Error creating index\n";
                    }
                }
            }
            else if (command == "USE") {
                std::string type, db_name;
                iss >> type >> db_name;
                std::transform(type.begin(), type.end(), type.begin(), ::toupper);
                
                if (type != "DATABASE") {
                    std::cout << "Error: Invalid syntax. Use 'USE DATABASE <name>'\n";
                    continue;
                }
                
                if (current_db) {
                    delete current_db;
                }
                current_db = new Database(db_name);
                current_db_name = db_name;
                std::cout << "Using database: " << db_name << std::endl;
            }
            else if (command == "INSERT") {
                if (!current_db) {
                    std::cout << "Error: No database selected\n";
                    continue;
                }
                
                std::string into, table_name, values;
                iss >> into;
                if (into != "INTO") {
                    std::cout << "Error: Invalid syntax. Use 'INSERT INTO table VALUES (...)'\n";
                    continue;
                }
                
                iss >> table_name >> values;
                
                // Parse values
                size_t first = input.find('(');
                size_t last = input.find_last_of(')');
                if (first == std::string::npos || last == std::string::npos) {
                    std::cout << "Error: Invalid values format\n";
                    continue;
                }
                
                std::string valueStr = input.substr(first + 1, last - first - 1);
                std::vector<std::string> valueTokens = split(valueStr, ',');
                
                if (current_db->insert(table_name, valueTokens)) {
                    std::cout << "Record inserted successfully\n";
                } else {
                    std::cout << "Error inserting record\n";
                }
            }
            else if (command == "SELECT") {
                if (!current_db) {
                    std::cout << "Error: No database selected\n";
                    continue;
                }
                
                std::string asterisk, from, table_name, where, condition;
                iss >> asterisk >> from >> table_name;
                
                if (asterisk != "*" || from != "FROM") {
                    std::cout << "Error: Invalid syntax. Use 'SELECT * FROM table [WHERE condition]'\n";
                    continue;
                }
                
                // Check for WHERE clause
                iss >> where;
                if (where == "WHERE") {
                    std::getline(iss, condition);
                    condition = condition.substr(condition.find_first_not_of(" \t"));
                }
                
                auto results = current_db->select(table_name, condition);
                
                if (results.empty()) {
                    std::cout << "No records found\n";
                } else {
                    // Print results
                    for (const auto& record : results) {
                        for (size_t i = 0; i < record.size(); i++) {
                            std::cout << record[i];
                            if (i < record.size() - 1) std::cout << ", ";
                        }
                        std::cout << std::endl;
                    }
                }
            }
            else if (command == "UPDATE") {
                if (!current_db) {
                    std::cout << "Error: No database selected\n";
                    continue;
                }
                
                std::string table_name, set, set_clause, where, where_clause;
                iss >> table_name >> set;
                
                if (set != "SET") {
                    std::cout << "Error: Invalid syntax. Use 'UPDATE table SET column = value WHERE condition'\n";
                    continue;
                }
                
                // Get SET clause
                size_t wherePos = input.find(" WHERE ");
                if (wherePos == std::string::npos) {
                    std::cout << "Error: WHERE clause required\n";
                    continue;
                }
                
                set_clause = input.substr(input.find("SET ") + 4, wherePos - (input.find("SET ") + 4));
                where_clause = input.substr(wherePos + 7);
                
                if (current_db->update(table_name, set_clause, where_clause)) {
                    std::cout << "Records updated successfully\n";
                } else {
                    std::cout << "Error updating records\n";
                }
            }
            else if (command == "DELETE") {
                if (!current_db) {
                    std::cout << "Error: No database selected\n";
                    continue;
                }
                
                std::string from, table_name, where, condition;
                iss >> from;
                
                if (from != "FROM") {
                    std::cout << "Error: Invalid syntax. Use 'DELETE FROM table WHERE condition'\n";
                    continue;
                }
                
                iss >> table_name >> where;
                if (where != "WHERE") {
                    std::cout << "Error: WHERE clause required\n";
                    continue;
                }
                
                std::getline(iss, condition);
                condition = condition.substr(condition.find_first_not_of(" \t"));
                
                if (current_db->remove(table_name, condition)) {
                    std::cout << "Records deleted successfully\n";
                } else {
                    std::cout << "Error deleting records\n";
                }
            }
            else if (command == "DROP") {
                if (!current_db) {
                    std::cout << "Error: No database selected\n";
                    continue;
                }
                
                std::string type, table_name;
                iss >> type;
                std::transform(type.begin(), type.end(), type.begin(), ::toupper);
                
                if (type != "TABLE") {
                    std::cout << "Error: Invalid syntax. Use 'DROP TABLE <name>'\n";
                    continue;
                }
                
                iss >> table_name;
                if (current_db->dropTable(table_name)) {
                    std::cout << "Table dropped: " << table_name << std::endl;
                } else {
                    std::cout << "Error dropping table\n";
                }
            }
            else {
                std::cout << "Unknown command. Type 'HELP' for available commands\n";
            }
        }
        catch (const std::exception& e) {
            std::cout << "Error: " << e.what() << std::endl;
        }
    }

    if (current_db) {
        delete current_db;
    }

    return 0;
} 