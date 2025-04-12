#ifndef INDEX_H
#define INDEX_H
#include <string>
#include <vector>
#include <map>
#include <memory>
#include <fstream>
#include "Catalog.h"

namespace db {

// Forward declarations
class Table;

enum class IndexType {
    BTREE,
    HASH
};

// Abstract base class for indexes
 class Index {
protected:
    std::string name;
    std::string tableName;
    std::string columnName;
    IndexType type;

public:
    Index(const std::string& name, const std::string& tableName, 
          const std::string& columnName, IndexType type)
        : name(name), tableName(tableName), columnName(columnName), type(type) {}
    
    virtual ~Index() {}
    
    std::string getName() const { return name; }
    std::string getTableName() const { return tableName; }
    std::string getColumnName() const { return columnName; }
    IndexType getType() const { return type; }
    
    // Abstract methods to be implemented by derived classes
    virtual void insert(const std::string& key, int rowId) = 0;
    virtual void remove(const std::string& key, int rowId) = 0;
    virtual std::vector<int> search(const std::string& key) = 0;
    virtual std::vector<int> rangeSearch(const std::string& startKey, const std::string& endKey) = 0;
    virtual void save() = 0;
    virtual void load() = 0;
};

// B-Tree Node structure
struct BTreeNode {
    bool isLeaf;
    std::vector<std::string> keys;
    std::vector<std::vector<int>> values; // For leaf nodes
    std::vector<std::shared_ptr<BTreeNode>> children; // For non-leaf nodes
    std::shared_ptr<BTreeNode> next; // For leaf nodes (linked list)
    
    BTreeNode(bool leaf = true) : isLeaf(leaf), next(nullptr) {}
};

// B-Tree Index implementation
class BTreeIndex : public Index {
private:
    static const int MAX_KEYS = 3; // Maximum keys per node
    std::shared_ptr<BTreeNode> root;
    std::string indexFilePath;

    // Helper methods
    void splitChild(std::shared_ptr<BTreeNode> parent, int idx);
    void insertNonFull(std::shared_ptr<BTreeNode> node, const std::string& key, int rowId);
    std::shared_ptr<BTreeNode> findLeaf(std::shared_ptr<BTreeNode> node, const std::string& key);
    void saveNode(std::shared_ptr<BTreeNode> node, std::ofstream& out);
    std::shared_ptr<BTreeNode> loadNode(std::ifstream& in);

public:
    BTreeIndex(const std::string& name, const std::string& tableName, const std::string& columnName);
    ~BTreeIndex();

    void insert(const std::string& key, int rowId) override;
    void remove(const std::string& key, int rowId) override;
    std::vector<int> search(const std::string& key) override;
    std::vector<int> rangeSearch(const std::string& startKey, const std::string& endKey) override;
    void save() override;
    void load() override;
};

// Hash Index implementation (simplified)
class HashIndex : public Index {
private:
    std::map<std::string, std::vector<int>> hashTable;
    std::string indexFilePath;

public:
    HashIndex(const std::string& name, const std::string& tableName, const std::string& columnName);
    ~HashIndex();

    void insert(const std::string& key, int rowId) override;
    void remove(const std::string& key, int rowId) override;
    std::vector<int> search(const std::string& key) override;
    std::vector<int> rangeSearch(const std::string& startKey, const std::string& endKey) override;
    void save() override;
    void load() override;
};

// Index Manager class to handle all indexes
class IndexManager {
private:
    std::vector<std::shared_ptr<Index>> indexes;
    std::string dbPath;

public:
    IndexManager(const std::string& dbPath);
    ~IndexManager();

    std::shared_ptr<Index> createIndex(const std::string& name, const std::string& tableName, 
                                      const std::string& columnName, IndexType type);
    void dropIndex(const std::string& name);
    std::shared_ptr<Index> getIndex(const std::string& name);
    std::vector<std::shared_ptr<Index>> getTableIndexes(const std::string& tableName);
    std::vector<std::shared_ptr<Index>> getColumnIndexes(const std::string& tableName, const std::string& columnName);
    void saveAllIndexes();
    void loadAllIndexes();
};

} // namespace db

#endif // INDEX_H