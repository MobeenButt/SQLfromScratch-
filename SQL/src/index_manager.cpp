#include "index_manager.h"
#include <algorithm>
#include <cstring>
#include <map>
#include <fstream>
#include <iostream>
#include "storage_manager.h"
#include <vector>
#include "database.h"
#include <filesystem>

// Forward declarations
class BPlusTree;
struct IndexNode;

// IndexNode definition
struct IndexNode {
    bool is_leaf;
    int next_leaf;
    std::vector<int> keys;
    std::vector<int> children;
    
    IndexNode() : is_leaf(true), next_leaf(-1) {}
};

// BPlusTree class definition
class BPlusTree {
private:
    StorageManager* storage_manager;
    std::string index_file;
    int order;
    int root_page_id;

    bool insertNonFull(int page_id, int key, int value);
    void splitNode(IndexNode& node, int page_id);
    int searchInNode(int page_id, int key);
    void serializeNode(const IndexNode& node, Page& page);
    void deserializeNode(const Page& page, IndexNode& node);

public:
    BPlusTree(StorageManager* sm, const std::string& filename, int tree_order);
    bool insert(int key, int value);
    int search(int key);
    bool exists(int key);
};

// IndexManager implementation
IndexManager::IndexManager(StorageManager* storage_manager) 
    : storage_manager(storage_manager) {
}

IndexManager::~IndexManager() {
}

bool IndexManager::createIndex(const std::string& db_name,
                             const std::string& table_name,
                             const std::string& column_name) {
    std::string index_file = "./data/" + db_name + "/" + table_name + "_" + column_name + ".idx";
    
    std::cout << "Creating index file: " << index_file << std::endl;
    
    // Create empty index file
    std::ofstream file(index_file, std::ios::binary);
    if (!file) {
        std::cerr << "Failed to create index file: " << index_file << std::endl;
        return false;
    }
    
    file.close();
    
    std::cout << "Successfully created file: " << index_file << std::endl;
    
    // For backward compatibility, still call writePage
    Page page;
    page.clear();
    page.setNumKeys(0);
    
    if (!storage_manager->writePage(index_file, 0, page)) {
        std::cerr << "Failed to write index page" << std::endl;
        return false;
    }
    
    std::cout << "Successfully created index file" << std::endl;
    return true;
}

bool IndexManager::dropIndex(const std::string& db_name, 
                           const std::string& table_name, 
                           const std::string& column_name) {
    std::string index_file = "./data/" + db_name + "/" + table_name + "_" + column_name + ".idx";
    
    if (std::remove(index_file.c_str()) != 0) {
        std::cerr << "Failed to remove index file: " << index_file << std::endl;
        return false;
    }
    
    return true;
}

bool IndexManager::insert(const std::string& index_file, int key, const Record& record) {
    // Fix path to include ./data/ prefix if not already included
    std::string full_path = index_file;
    if (full_path.substr(0, 7) != "./data/") {
        full_path = "./data/" + index_file;
    }
    
    std::cout << "Inserting into index file: " << full_path << std::endl;
    
    // Load all existing keys from index
    std::vector<int> keys;
    std::vector<Record> records;
    
    if (std::filesystem::exists(full_path)) {
        std::ifstream file(full_path, std::ios::binary);
        if (file) {
            // Read all keys from index
            int k;
            while (file >> k) {
                keys.push_back(k);
                file.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
            }
            file.close();
        }
    }
    
    // Add the new key if it doesn't exist
    if (std::find(keys.begin(), keys.end(), key) == keys.end()) {
        keys.push_back(key);
        records.push_back(record);
    }
    
    // Write keys back to index file
    std::ofstream out_file(full_path, std::ios::binary);
    if (!out_file) {
        std::cerr << "Failed to open index file for writing: " << full_path << std::endl;
        return false;
    }
    
    // Sort keys for faster searching
    std::sort(keys.begin(), keys.end());
    
    for (int k : keys) {
        out_file << k << std::endl;
    }
    
    out_file.close();
    if (!out_file) {
        std::cerr << "Error writing to index file" << std::endl;
        return false;
    }
    
    std::cout << "Successfully updated index file with key: " << key << std::endl;
    return true;
}

bool IndexManager::exists(const std::string& index_file, int key) {
    // Fix path to include ./data/ prefix if not already included
    std::string full_path = index_file;
    if (full_path.substr(0, 7) != "./data/") {
        full_path = "./data/" + index_file;
    }
    
    if (!std::filesystem::exists(full_path)) {
        std::cout << "Index file does not exist: " << full_path << std::endl;
        return false;
    }
    
    std::ifstream file(full_path, std::ios::binary);
    if (!file) {
        std::cerr << "Failed to open index file: " << full_path << std::endl;
        return false;
    }
    
    int k;
    while (file >> k) {
        if (k == key) {
            file.close();
            return true;
        }
        file.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
    }
    
    file.close();
    return false;
}

bool IndexManager::search(const std::string& index_file, 
                        const std::string& op, 
                        int value, 
                        std::vector<int>& result) {
    if (!std::filesystem::exists(index_file)) {
        std::cout << "Index file does not exist: " << index_file << std::endl;
        return false;
    }
    
    std::ifstream file(index_file, std::ios::binary);
    if (!file) {
        std::cerr << "Failed to open index file: " << index_file << std::endl;
        return false;
    }
    
    // Read all keys from index
    std::vector<int> all_keys;
    int k;
    while (file >> k) {
        all_keys.push_back(k);
        file.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
    }
    file.close();
    
    std::cout << "Found " << all_keys.size() << " records in index" << std::endl;
    
    // Filter keys based on operator
    for (int key : all_keys) {
        bool match = false;
        
        if (op == "=") match = (key == value);
        else if (op == "<") match = (key < value);
        else if (op == ">") match = (key > value);
        else if (op == "<=") match = (key <= value);
        else if (op == ">=") match = (key >= value);
        else if (op == "!=") match = (key != value);
        
        if (match) {
            result.push_back(key);
        }
    }
    
    std::cout << "Found " << result.size() << " matching records" << std::endl;
    return true;
}

// BPlusTree implementation
BPlusTree::BPlusTree(StorageManager* sm, const std::string& filename, int tree_order) 
    : storage_manager(sm), index_file(filename), order(tree_order) {
    try {
        std::cout << "Initializing B+ tree for file: " << filename << std::endl;
        
        Page root_page;
        bool root_exists = storage_manager->readPage(index_file, 0, root_page);
        
        if (!root_exists) {
            std::cout << "Creating new root node" << std::endl;
    // Initialize empty root node
    IndexNode root;
    root.is_leaf = true;
    root.next_leaf = -1;
    
            // Initialize the page
            root_page.clear();
            root_page.setFreeSpace(PAGE_SIZE);
            
            serializeNode(root, root_page);
            if (!storage_manager->writePage(index_file, 0, root_page)) {
                throw std::runtime_error("Failed to write initial root page");
            }
        }
    root_page_id = 0;
        std::cout << "B+ tree initialized successfully" << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "Error initializing B+ tree: " << e.what() << std::endl;
        throw;
    }
}

bool BPlusTree::exists(int key) {
    return search(key) != -1;
}

bool BPlusTree::insert(int key, int value) {
    try {
        std::cout << "Inserting key: " << key << " with value: " << value << std::endl;
        
        // Read root page
        Page root_page;
        if (!storage_manager->readPage(index_file, root_page_id, root_page)) {
            throw std::runtime_error("Failed to read root page during insert");
        }

    IndexNode root;
    deserializeNode(root_page, root);

        // If root is full, create new root
        if (static_cast<int>(root.keys.size()) == (2 * order - 1)) {
            std::cout << "Splitting root node" << std::endl;
            
            // Create new root
        IndexNode new_root;
        new_root.is_leaf = false;
        new_root.children.push_back(root_page_id);

            // Save old root
            Page old_root_page;
            serializeNode(root, old_root_page);
            if (!storage_manager->writePage(index_file, root_page_id + 1, old_root_page)) {
                throw std::runtime_error("Failed to write old root during split");
            }

            // Split old root
            splitNode(root, root_page_id + 1);

            // Save new root
            Page new_root_page;
            serializeNode(new_root, new_root_page);
            if (!storage_manager->writePage(index_file, root_page_id, new_root_page)) {
                throw std::runtime_error("Failed to write new root");
            }
    }

    return insertNonFull(root_page_id, key, value);
    } catch (const std::exception& e) {
        std::cerr << "Error in insert: " << e.what() << std::endl;
        return false;
    }
}

bool BPlusTree::insertNonFull(int page_id, int key, int value) {
    try {
    Page current_page;
        if (!storage_manager->readPage(index_file, page_id, current_page)) {
            throw std::runtime_error("Failed to read page during insert");
        }

    IndexNode node;
    deserializeNode(current_page, node);

        // If this is a leaf node
    if (node.is_leaf) {
            // Insert key and value
            auto it = std::lower_bound(node.keys.begin(), node.keys.end(), key);
            size_t pos = it - node.keys.begin();
            
            node.keys.insert(it, key);
            node.children.insert(node.children.begin() + pos, value);

        serializeNode(node, current_page);
            return storage_manager->writePage(index_file, page_id, current_page);
        }

        // If this is not a leaf node
        int i = node.keys.size() - 1;
        while (i >= 0 && key < node.keys[i]) {
            i--;
        }
        i++;

        Page child_page;
        if (!storage_manager->readPage(index_file, node.children[i], child_page)) {
            throw std::runtime_error("Failed to read child page");
        }

        IndexNode child;
        deserializeNode(child_page, child);

        if (static_cast<int>(child.keys.size()) == (2 * order - 1)) {
            splitNode(child, node.children[i]);
            if (key > node.keys[i]) {
                i++;
            }
        }

        return insertNonFull(node.children[i], key, value);
    } catch (const std::exception& e) {
        std::cerr << "Error in insertNonFull: " << e.what() << std::endl;
        return false;
    }
}

void BPlusTree::splitNode(IndexNode& node, int page_id) {
    try {
    IndexNode new_node;
    new_node.is_leaf = node.is_leaf;

        int mid = order - 1;
    
    // Move half of the keys to new node
    new_node.keys.assign(node.keys.begin() + mid + 1, node.keys.end());
    node.keys.resize(mid);

    // Move half of the children to new node
    if (!node.is_leaf) {
        new_node.children.assign(node.children.begin() + mid + 1, 
                               node.children.end());
        node.children.resize(mid + 1);
    }

    // Handle leaf node connections
    if (node.is_leaf) {
        new_node.next_leaf = node.next_leaf;
        node.next_leaf = page_id + 1;
    }

    // Save both nodes
    Page page, new_page;
    serializeNode(node, page);
    serializeNode(new_node, new_page);
        
        if (!storage_manager->writePage(index_file, page_id, page)) {
            throw std::runtime_error("Failed to write split node");
        }
        if (!storage_manager->writePage(index_file, page_id + 1, new_page)) {
            throw std::runtime_error("Failed to write new split node");
        }
    } catch (const std::exception& e) {
        std::cerr << "Error in splitNode: " << e.what() << std::endl;
        throw;
    }
}

int BPlusTree::search(int key) {
    try {
    return searchInNode(root_page_id, key);
    } catch (const std::exception& e) {
        std::cerr << "Error in search: " << e.what() << std::endl;
        return -1;
    }
}

int BPlusTree::searchInNode(int page_id, int key) {
    Page current_page;
    if (!storage_manager->readPage(index_file, page_id, current_page)) {
        throw std::runtime_error("Failed to read page during search");
    }

    IndexNode node;
    deserializeNode(current_page, node);

    if (node.is_leaf) {
        auto it = std::lower_bound(node.keys.begin(), node.keys.end(), key);
        if (it != node.keys.end() && *it == key) {
            return node.children[it - node.keys.begin()];
        }
        return -1;
    }

    int i = 0;
    while (i < static_cast<int>(node.keys.size()) && key > node.keys[i]) {
        i++;
    }

    return searchInNode(node.children[i], key);
}

void BPlusTree::serializeNode(const IndexNode& node, Page& page) {
    try {
        // Clear the page
        page.clear();
        
        // Write is_leaf flag
        page.writeData(0, &node.is_leaf, sizeof(bool));
        
        // Write next_leaf
        page.writeData(sizeof(bool), &node.next_leaf, sizeof(int));
        
        // Write number of keys
    int num_keys = node.keys.size();
        page.writeData(sizeof(bool) + sizeof(int), &num_keys, sizeof(int));
    
        // Write keys
        size_t offset = sizeof(bool) + 2 * sizeof(int);
    for (int key : node.keys) {
            page.writeData(offset, &key, sizeof(int));
            offset += sizeof(int);
    }
    
        // Write number of children
    int num_children = node.children.size();
        page.writeData(offset, &num_children, sizeof(int));
        offset += sizeof(int);
    
        // Write children
    for (int child : node.children) {
            page.writeData(offset, &child, sizeof(int));
            offset += sizeof(int);
        }
        
        page.setFreeSpace(PAGE_SIZE - offset);
    } catch (const std::exception& e) {
        std::cerr << "Error in serializeNode: " << e.what() << std::endl;
        throw;
    }
}

void BPlusTree::deserializeNode(const Page& page, IndexNode& node) {
    try {
        // Read is_leaf flag
        page.readData(0, &node.is_leaf, sizeof(bool));
    
        // Read next_leaf
        page.readData(sizeof(bool), &node.next_leaf, sizeof(int));
    
        // Read number of keys
        int num_keys;
        page.readData(sizeof(bool) + sizeof(int), &num_keys, sizeof(int));
    
        // Read keys
        size_t offset = sizeof(bool) + 2 * sizeof(int);
    node.keys.clear();
    for (int i = 0; i < num_keys; i++) {
            int key;
            page.readData(offset, &key, sizeof(int));
            node.keys.push_back(key);
            offset += sizeof(int);
        }
        
        // Read number of children
        int num_children;
        page.readData(offset, &num_children, sizeof(int));
        offset += sizeof(int);
        
        // Read children
    node.children.clear();
    for (int i = 0; i < num_children; i++) {
            int child;
            page.readData(offset, &child, sizeof(int));
            node.children.push_back(child);
            offset += sizeof(int);
        }
    } catch (const std::exception& e) {
        std::cerr << "Error in deserializeNode: " << e.what() << std::endl;
        throw;
    }
}

bool IndexManager::remove(const std::string& index_file, int key) {
    try {
        // Read the index page
        Page index_page;
        if (!storage_manager->readPage(index_file, 0, index_page)) {
            std::cerr << "Failed to read index page" << std::endl;
            return false;
        }

        // Find and remove the key
        std::vector<int> keys;
        size_t offset = 0;
        while (offset < PAGE_SIZE_BYTES - index_page.getFreeSpace()) {
            IndexRecord record;
            index_page.readData(offset, &record, sizeof(IndexRecord));
            if (record.key != key) {
                keys.push_back(record.key);
            }
            offset += sizeof(IndexRecord);
        }

        // Write back the remaining keys
        index_page.clear();
        offset = 0;
        for (int k : keys) {
            IndexRecord rec{k, 0};  // page_id is 0 since we're using a simple index
            index_page.writeData(offset, &rec, sizeof(IndexRecord));
            offset += sizeof(IndexRecord);
        }
        index_page.setFreeSpace(PAGE_SIZE_BYTES - offset);

        // Write the updated page back
        if (!storage_manager->writePage(index_file, 0, index_page)) {
            std::cerr << "Failed to write index page" << std::endl;
            return false;
        }

        return true;
    } catch (const std::exception& e) {
        std::cerr << "Error removing key from index: " << e.what() << std::endl;
        return false;
    }
}

// Add this helper function to your utilities
std::string getFullPath(const std::string& path) {
    if (path.substr(0, 7) == "./data/") {
        return path;
    }
    return "./data/" + path;
}