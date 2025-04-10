#include "Index.h"
#include <algorithm>
#include <iostream>
#include <filesystem>
#include<cstring>
#include <fstream>
#include <memory>

namespace fs = std::filesystem;
namespace db {

//=============================================
// BTreeIndex Implementation
//=============================================

BTreeIndex::BTreeIndex(const std::string& name, 
                      const std::string& tableName, 
                      const std::string& columnName)
    : Index(name, tableName, columnName, IndexType::BTREE),
      root(std::make_shared<BTreeNode>(true)),
      indexFilePath("index/" + name + ".idx") {

        // Create index directory if it doesn't exist
    std::string dir = "index";
    if (!fs::exists(dir)) {
        fs::create_directories(dir);
    }
      }

BTreeIndex::~BTreeIndex() = default;

void BTreeIndex::insert(const std::string& key, int rowId) {
    auto node = root;
    if (node->keys.size() == MAX_KEYS) {
        auto new_root = std::make_shared<BTreeNode>(false);
        new_root->children.push_back(node);
        splitChild(new_root, 0);
        root = new_root;
    }
    insertNonFull(root, key, rowId);
}

void BTreeIndex::insertNonFull(std::shared_ptr<BTreeNode> node, const std::string& key, int rowId) {
    if (node->isLeaf) {
        auto it = std::lower_bound(node->keys.begin(), node->keys.end(), key);
        int pos = it - node->keys.begin();
        node->keys.insert(it, key);
        node->values.insert(node->values.begin() + pos, {rowId});
    } else {
        int i = node->keys.size() - 1;
        while (i >= 0 && key < node->keys[i]) i--;
        i++;
        
        if (node->children[i]->keys.size() == MAX_KEYS) {
            splitChild(node, i);
            if (key > node->keys[i]) i++;
        }
        insertNonFull(node->children[i], key, rowId);
    }
}

void BTreeIndex::splitChild(std::shared_ptr<BTreeNode> parent, int idx) {
    auto child = parent->children[idx];
    auto new_child = std::make_shared<BTreeNode>(child->isLeaf);
    
    size_t mid = MAX_KEYS / 2;
    new_child->keys.assign(child->keys.begin() + mid + 1, child->keys.end());
    child->keys.resize(mid);
    
    if (child->isLeaf) {
        new_child->values.assign(child->values.begin() + mid + 1, child->values.end());
        child->values.resize(mid);
    } else {
        new_child->children.assign(child->children.begin() + mid + 1, child->children.end());
        child->children.resize(mid + 1);
    }
    
    parent->keys.insert(parent->keys.begin() + idx, child->keys[mid]);
    parent->children.insert(parent->children.begin() + idx + 1, new_child);
}

std::vector<int> BTreeIndex::search(const std::string& key) {
    auto leaf = findLeaf(root, key);
    auto it = std::lower_bound(leaf->keys.begin(), leaf->keys.end(), key);
    if (it != leaf->keys.end() && *it == key) {
        return leaf->values[it - leaf->keys.begin()];
    }
    return {};
}

std::vector<int> BTreeIndex::rangeSearch(const std::string& startKey, const std::string& endKey) {
    std::vector<int> result;
    auto leaf = findLeaf(root, startKey);
    
    bool found = false;
    auto currentNode = leaf;
    
    while (currentNode != nullptr) {
        for (size_t i = 0; i < currentNode->keys.size(); i++) {
            if (currentNode->keys[i] >= startKey && currentNode->keys[i] <= endKey) {
                const auto& rowIds = currentNode->values[i];
                result.insert(result.end(), rowIds.begin(), rowIds.end());
            }
            
            if (currentNode->keys[i] > endKey) {
                found = true;
                break;
            }
        }
        
        if (found) break;
        currentNode = currentNode->next;
    }
    
    return result;
}

void BTreeIndex::remove(const std::string& key, int rowId) {
    auto leaf = findLeaf(root, key);
    
    for (size_t i = 0; i < leaf->keys.size(); i++) {
        if (leaf->keys[i] == key) {
            auto& values = leaf->values[i];
            values.erase(std::remove(values.begin(), values.end(), rowId), values.end());
            
            if (values.empty()) {
                leaf->keys.erase(leaf->keys.begin() + i);
                leaf->values.erase(leaf->values.begin() + i);
            }
            break;
        }
    }
}

std::shared_ptr<BTreeNode> BTreeIndex::findLeaf(std::shared_ptr<BTreeNode> node, const std::string& key) {
    while (!node->isLeaf) {
        size_t i = 0;
        while (i < node->keys.size() && key >= node->keys[i]) i++;
        node = node->children[i];
    }
    return node;
}
void BTreeIndex::save() {
    // Ensure directory exists
    std::string dir = "index";
    if (!fs::exists(dir)) {
        fs::create_directories(dir);
    }

    std::ofstream out(indexFilePath, std::ios::binary | std::ios::trunc);
    if (!out) {
        std::cerr << "Failed to open index file for writing: " << indexFilePath << std::endl;
        std::cerr << "Error: " << strerror(errno) << std::endl; // Print the actual error
        return;
    }
    saveNode(root, out);
    out.close();
}
void BTreeIndex::saveNode(std::shared_ptr<BTreeNode> node, std::ofstream& out) {
    out.write(reinterpret_cast<const char*>(&node->isLeaf), sizeof(bool));
    
    size_t keysCount = node->keys.size();
    out.write(reinterpret_cast<const char*>(&keysCount), sizeof(size_t));
    
    for (size_t i = 0; i < keysCount; i++) {
        size_t keyLength = node->keys[i].length();
        out.write(reinterpret_cast<const char*>(&keyLength), sizeof(size_t));
        out.write(node->keys[i].c_str(), keyLength);
        
        if (node->isLeaf) {
            size_t valuesCount = node->values[i].size();
            out.write(reinterpret_cast<const char*>(&valuesCount), sizeof(size_t));
            for (int rowId : node->values[i]) {
                out.write(reinterpret_cast<const char*>(&rowId), sizeof(int));
            }
        }
    }
    
    if (!node->isLeaf) {
        size_t childrenCount = node->children.size();
        out.write(reinterpret_cast<const char*>(&childrenCount), sizeof(size_t));
        for (const auto& child : node->children) {
            saveNode(child, out);
        }
    }
}

void BTreeIndex::load() {
    std::ifstream in(indexFilePath, std::ios::binary);
    if (!in) {
        std::cerr << "Index file does not exist: " << indexFilePath << std::endl;
        return;
    }
    root = loadNode(in);
    in.close();
    
    if (!root) {
        root = std::make_shared<BTreeNode>(true);
    }
}

std::shared_ptr<BTreeNode> BTreeIndex::loadNode(std::ifstream& in) {
    if (!in.good()) return nullptr;
    
    auto node = std::make_shared<BTreeNode>();
    in.read(reinterpret_cast<char*>(&node->isLeaf), sizeof(bool));
    
    size_t keysCount;
    in.read(reinterpret_cast<char*>(&keysCount), sizeof(size_t));
    
    for (size_t i = 0; i < keysCount; i++) {
        size_t keyLength;
        in.read(reinterpret_cast<char*>(&keyLength), sizeof(size_t));
        
        std::string key(keyLength, '\0');
        in.read(&key[0], keyLength);
        node->keys.push_back(key);
        
        if (node->isLeaf) {
            size_t valuesCount;
            in.read(reinterpret_cast<char*>(&valuesCount), sizeof(size_t));
            
            std::vector<int> values;
            for (size_t j = 0; j < valuesCount; j++) {
                int rowId;
                in.read(reinterpret_cast<char*>(&rowId), sizeof(int));
                values.push_back(rowId);
            }
            node->values.push_back(values);
        }
    }
    
    if (!node->isLeaf) {
        size_t childrenCount;
        in.read(reinterpret_cast<char*>(&childrenCount), sizeof(size_t));
        for (size_t i = 0; i < childrenCount; i++) {
            auto child = loadNode(in);
            node->children.push_back(child);
        }
    }
    
    return node;
}

//=============================================
// HashIndex Implementation  
//=============================================

HashIndex::HashIndex(const std::string& name,
    const std::string& tableName,
    const std::string& columnName)
: Index(name, tableName, columnName, IndexType::HASH),
indexFilePath("index/" + name + ".idx") {

// Create index directory if it doesn't exist
std::string dir = "index";
if (!fs::exists(dir)) {
try {
fs::create_directories(dir);
} catch (const std::filesystem::filesystem_error& e) {
std::cerr << "Error creating directory: " << e.what() << std::endl;
}
}
}

HashIndex::~HashIndex() = default;

void HashIndex::insert(const std::string& key, int rowId) {
    hashTable[key].push_back(rowId);
}

void HashIndex::remove(const std::string& key, int rowId) {
    auto it = hashTable.find(key);
    if (it != hashTable.end()) {
        auto& vec = it->second;
        vec.erase(std::remove(vec.begin(), vec.end(), rowId), vec.end());
        if (vec.empty()) {
            hashTable.erase(it);
        }
    }
}

std::vector<int> HashIndex::search(const std::string& key) {
    auto it = hashTable.find(key);
    return it != hashTable.end() ? it->second : std::vector<int>();
}

std::vector<int> HashIndex::rangeSearch(const std::string& startKey, const std::string& endKey) {
    std::vector<int> results;
    for (auto it = hashTable.lower_bound(startKey); 
         it != hashTable.end() && it->first <= endKey; ++it) {
        results.insert(results.end(), it->second.begin(), it->second.end());
    }
    return results;
}

void HashIndex::save() 
{
    // Ensure directory exists
    std::string dir = "index";
    if (!fs::exists(dir)) {
        fs::create_directories(dir);
    }

    std::ofstream out(indexFilePath, std::ios::binary | std::ios::trunc);
    if (!out) {
        std::cerr << "Failed to open index file for writing: " << indexFilePath << std::endl;
        return;
    }
    
    size_t tableSize = hashTable.size();
    out.write(reinterpret_cast<const char*>(&tableSize), sizeof(size_t));
    
    for (const auto& pair : hashTable) {
        size_t keyLength = pair.first.length();
        out.write(reinterpret_cast<const char*>(&keyLength), sizeof(size_t));
        out.write(pair.first.c_str(), keyLength);
        
        const auto& values = pair.second;
        size_t valuesCount = values.size();
        out.write(reinterpret_cast<const char*>(&valuesCount), sizeof(size_t));
        
        for (int rowId : values) {
            out.write(reinterpret_cast<const char*>(&rowId), sizeof(int));
        }
    }
    
    out.close();
}

void HashIndex::load() {
    std::ifstream in(indexFilePath, std::ios::binary);
    if (!in) {
        std::cerr << "Failed to open index file for reading: " << indexFilePath << std::endl;
        return;
    }
    
    hashTable.clear();
    size_t tableSize;
    in.read(reinterpret_cast<char*>(&tableSize), sizeof(size_t));
    
    for (size_t i = 0; i < tableSize; i++) {
        size_t keyLength;
        in.read(reinterpret_cast<char*>(&keyLength), sizeof(size_t));
        std::string key(keyLength, '\0');
        in.read(&key[0], keyLength);
        
        size_t valuesCount;
        in.read(reinterpret_cast<char*>(&valuesCount), sizeof(size_t));
        
        std::vector<int> values(valuesCount);
        for (size_t j = 0; j < valuesCount; j++) {
            in.read(reinterpret_cast<char*>(&values[j]), sizeof(int));
        }
        
        hashTable[key] = values;
    }
    
    in.close();
}

//=============================================
// IndexManager Implementation
//=============================================

IndexManager::IndexManager(const std::string& dbPath) : dbPath(dbPath) {
    if (!fs::exists(dbPath)) {
        fs::create_directories(dbPath);
    }
}

IndexManager::~IndexManager() {
    saveAllIndexes();
}

std::shared_ptr<Index> IndexManager::createIndex(const std::string& name, 
                                               const std::string& tableName, 
                                               const std::string& columnName, 
                                               IndexType type) {
    for (const auto& index : indexes) {
        if (index->getName() == name) {
            return index;
        }
    }
    
    std::shared_ptr<Index> newIndex;
    
    if (type == IndexType::BTREE) {
        newIndex = std::make_shared<BTreeIndex>(name, tableName, columnName);
    } else if (type == IndexType::HASH) {
        newIndex = std::make_shared<HashIndex>(name, tableName, columnName);
    }
    
    if (newIndex) {
        indexes.push_back(newIndex);
    }
    
    return newIndex;
}

void IndexManager::dropIndex(const std::string& name) {
    for (auto it = indexes.begin(); it != indexes.end(); ++it) {
        if ((*it)->getName() == name) {
            std::string indexFilePath = dbPath + "/" + (*it)->getTableName() + "_" + name + ".idx";
            fs::remove(indexFilePath);
            indexes.erase(it);
            return;
        }
    }
}

std::shared_ptr<Index> IndexManager::getIndex(const std::string& name) {
    for (const auto& index : indexes) {
        if (index->getName() == name) {
            return index;
        }
    }
    return nullptr;
}

std::vector<std::shared_ptr<Index>> IndexManager::getTableIndexes(const std::string& tableName) {
    std::vector<std::shared_ptr<Index>> result;
    for (const auto& index : indexes) {
        if (index->getTableName() == tableName) {
            result.push_back(index);
        }
    }
    return result;
}

std::vector<std::shared_ptr<Index>> IndexManager::getColumnIndexes(const std::string& tableName, 
                                                                  const std::string& columnName) {
    std::vector<std::shared_ptr<Index>> result;
    for (const auto& index : indexes) {
        if (index->getTableName() == tableName && index->getColumnName() == columnName) {
            result.push_back(index);
        }
    }
    return result;
}

void IndexManager::saveAllIndexes() {
    for (const auto& index : indexes) {
        index->save();
    }
}

void IndexManager::loadAllIndexes() {
    std::string indexDir = "index";
    if (!fs::exists(indexDir) || !fs::is_directory(indexDir)) {
        std::cerr << "Index directory does not exist.\n";
        return;
    }
    
    for (const auto& entry : fs::directory_iterator(indexDir)) {
        if (entry.path().extension() == ".idx") {
            std::ifstream in(entry.path(), std::ios::binary);
            if (!in) {
                std::cerr << "Failed to open index file: " << entry.path() << "\n";
                continue;
            }
            
            char type;
            in.read(&type, sizeof(char));
            if (!in) continue;
            
            size_t nameLen = 0;
            in.read(reinterpret_cast<char*>(&nameLen), sizeof(size_t));
            if (!in) continue;
            std::string indexName(nameLen, '\0');
            in.read(&indexName[0], nameLen);
            if (!in) continue;
            
            size_t tableNameLen = 0;
            in.read(reinterpret_cast<char*>(&tableNameLen), sizeof(size_t));
            if (!in) continue;
            std::string tableName(tableNameLen, '\0');
            in.read(&tableName[0], tableNameLen);
            if (!in) continue;
            
            size_t columnNameLen = 0;
            in.read(reinterpret_cast<char*>(&columnNameLen), sizeof(size_t));
            if (!in) continue;
            std::string columnName(columnNameLen, '\0');
            in.read(&columnName[0], columnNameLen);
            if (!in) continue;
            
            std::shared_ptr<Index> idx;
            if (type == 'B') {
                idx = std::make_shared<BTreeIndex>(indexName, tableName, columnName);
            } else if (type == 'H') {
                idx = std::make_shared<HashIndex>(indexName, tableName, columnName);
            } else {
                std::cerr << "Unknown index type in file: " << entry.path() << "\n";
                continue;
            }
            
            idx->load();
            indexes.push_back(idx);
        }
    }
}

} // namespace db