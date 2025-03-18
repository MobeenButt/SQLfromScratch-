// #include "BPlusTree.h"
// #include <iostream>
// #include <algorithm>
// #include <fstream>

// using namespace std;

// // Constructor
// BPlusTreeNode::BPlusTreeNode(bool leaf) {
//     isLeaf = leaf;
//     nextLeaf = nullptr;
// }

// // BPlusTree Constructor
// BPlusTree::BPlusTree(int order) {
//     root = new BPlusTreeNode(true);
//     maxKeys = order - 1;
//     loadFromDisk();
// }

// // Insert function
// void BPlusTree::insert(int key, const std::string& value) {
//     BPlusTreeNode* node = root;

//     // Find correct leaf node
//     while (!node->isLeaf) {
//         int i = 0;
//         while (i < static_cast<int>(node->keys.size()) && key > node->keys[i]) i++;
//         node = node->children[i];
//     }

//     // Insert in leaf
//     auto it = std::lower_bound(node->keys.begin(), node->keys.end(), key);
//     node->keys.insert(it, key);
//     node->values.insert(node->values.begin() + static_cast<int>(it - node->keys.begin()), value);

//     // Split if necessary
//     if (static_cast<int>(node->keys.size()) > maxKeys) {
//         splitLeafNode(node, key, value);
//     }

//     saveToDisk();
// }

// // Search function
// std::string BPlusTree::search(int key) {
//     BPlusTreeNode* node = root;

//     while (!node->isLeaf) {
//         int i = 0;
//         while (i < static_cast<int>(node->keys.size()) && key > node->keys[i]) i++;
//         node = node->children[i];
//     }

//     for (int i = 0; i < static_cast<int>(node->keys.size()); i++) {
//         if (node->keys[i] == key) {
//             return node->values[i];
//         }
//     }
//     return "Not found";
// }

// // Remove function (Basic Implementation)
// void BPlusTree::remove(int key) {
//     BPlusTreeNode* node = root;
//     while (!node->isLeaf) {
//         int i = 0;
//         while (i < static_cast<int>(node->keys.size()) && key > node->keys[i]) i++;
//         node = node->children[i];
//     }

//     auto it = std::find(node->keys.begin(), node->keys.end(), key);
//     if (it != node->keys.end()) {
//         int index = static_cast<int>(it - node->keys.begin());
//         node->keys.erase(it);
//         node->values.erase(node->values.begin() + index);
//         saveToDisk();
//     }
// }

// // Split leaf node
// void BPlusTree::splitLeafNode(BPlusTreeNode* node, int key, const std::string& value) {
//     int mid = (node->keys.size() + 1) / 2;
//     BPlusTreeNode* newLeaf = new BPlusTreeNode(true);

//     newLeaf->keys.assign(node->keys.begin() + mid, node->keys.end());
//     newLeaf->values.assign(node->values.begin() + mid, node->values.end());

//     node->keys.resize(mid);
//     node->values.resize(mid);

//     newLeaf->nextLeaf = node->nextLeaf;
//     node->nextLeaf = newLeaf;

//     if (node == root) {
//         BPlusTreeNode* newRoot = new BPlusTreeNode(false);
//         newRoot->keys.push_back(newLeaf->keys[0]);
//         newRoot->children.push_back(node);
//         newRoot->children.push_back(newLeaf);
//         root = newRoot;
//     } else {
//         insertInternal(newLeaf->keys[0], findParent(root, node), newLeaf);
//     }
// }

// // Split internal node
// void BPlusTree::splitInternalNode(BPlusTreeNode* node) {
//     int mid = node->keys.size() / 2;
//     BPlusTreeNode* newInternal = new BPlusTreeNode(false);

//     int pushUpKey = node->keys[mid];
    
//     newInternal->keys.assign(node->keys.begin() + mid + 1, node->keys.end());
//     newInternal->children.assign(node->children.begin() + mid + 1, node->children.end());

//     node->keys.resize(mid);
//     node->children.resize(mid + 1);

//     if (node == root) {
//         BPlusTreeNode* newRoot = new BPlusTreeNode(false);
//         newRoot->keys.push_back(pushUpKey);
//         newRoot->children.push_back(node);
//         newRoot->children.push_back(newInternal);
//         root = newRoot;
//     } else {
//         insertInternal(pushUpKey, findParent(root, node), newInternal);
//     }
// }

// // Insert into internal node
// void BPlusTree::insertInternal(int key, BPlusTreeNode* parent, BPlusTreeNode* child) {
//     auto it = std::lower_bound(parent->keys.begin(), parent->keys.end(), key);
//     int index = it - parent->keys.begin();

//     parent->keys.insert(it, key);
//     parent->children.insert(parent->children.begin() + index + 1, child);

//     if (static_cast<int>(parent->keys.size()) > maxKeys) {
//         splitInternalNode(parent);
//     }
// }

// // Find parent of a node
// BPlusTreeNode* BPlusTree::findParent(BPlusTreeNode* current, BPlusTreeNode* child) {
//     if (current->isLeaf || current->children[0]->isLeaf) return nullptr;

//     for (size_t i = 0; i < current->children.size(); i++) {
//         if (current->children[i] == child) return current;
//     }

//     for (size_t i = 0; i < current->children.size(); i++) {
//         BPlusTreeNode* parent = findParent(current->children[i], child);
//         if (parent) return parent;
//     }

//     return nullptr;
// }

// Save to Disk
// void BPlusTree::saveToDisk() {
//     ofstream file("BPlusTree.db", ios::trunc);
//     if (!file) {
//         cerr << "Error: Cannot open file for saving.\n";
//         return;
//     }

//     BPlusTreeNode* node = root;
//     while (!node->isLeaf) node = node->children[0];

//     while (node) {
//         for (size_t i = 0; i < node->keys.size(); i++) {
//             file << node->keys[i] << " " << node->values[i] << "\n";
//         }
//         node = node->nextLeaf;
//     }
//     file.close();
// }

// void BPlusTree::saveToDisk() {
//     ofstream file("BPlusTree.db", ios::trunc);
    
//     if (!file) {
//         cerr << "Error: Cannot open file for saving.\n";
//         perror("Error"); // Print system error message
//         return;
//     }

//     cout << "Saving data to BPlusTree.db...\n"; // Debugging message

//     BPlusTreeNode* node = root;
//     while (!node->isLeaf) node = node->children[0];

//     while (node) {
//         for (size_t i = 0; i < node->keys.size(); i++) {
//             file << node->keys[i] << " " << node->values[i] << "\n";
//         }
//         node = node->nextLeaf;
//     }
    
//     file.close();
//     cout << "Data successfully saved!\n";
// }

// // Load from Disk
// void BPlusTree::loadFromDisk() {
//     ifstream file("BPlusTree.db");
//     if (!file) return;

//     int key;
//     string value;
//     while (file >> key) {
//         file.ignore();
//         getline(file, value);
//         insert(key, value);
//     }
//     file.close();
// }

// // Display Tree
// void BPlusTree::display() {
//     BPlusTreeNode* node = root;
//     while (!node->isLeaf) node = node->children[0];

//     while (node) {
//         for (int key : node->keys) {
//             cout << key << " ";
//         }
//         cout << " | ";
//         node = node->nextLeaf;
//     }
//     cout << endl;
// }
#include "BPlusTree.h"
#include <iostream>
#include <algorithm>
#include <fstream>

using namespace std;

// Constructor
BPlusTreeNode::BPlusTreeNode(bool leaf) {
    isLeaf = leaf;
    nextLeaf = nullptr;
}

// BPlusTree Constructor
BPlusTree::BPlusTree(int order) {
    root = new BPlusTreeNode(true);
    maxKeys = order - 1;
}

// Destructor
BPlusTree::~BPlusTree() {
    deleteTree(root);
}
void BPlusTree::deleteTree(BPlusTreeNode* node) {
    if (!node) return;
    if (!node->isLeaf) {
        for (BPlusTreeNode* child : node->children) {
            deleteTree(child);
        }
    }
    delete node;
}

// Insert function
void BPlusTree::insert(int key, const std::string& value) {
    BPlusTreeNode* node = root;

    // Find correct leaf node
    while (!node->isLeaf) {
        int i = 0;
        while (i < static_cast<int>(node->keys.size()) && key > node->keys[i]) i++;
        node = node->children[i];
    }

    // Insert in leaf
    auto it = std::lower_bound(node->keys.begin(), node->keys.end(), key);
    node->keys.insert(it, key);
    node->values.insert(node->values.begin() + static_cast<int>(it - node->keys.begin()), value);

    // Split if necessary
    if (static_cast<int>(node->keys.size()) > maxKeys) {
        splitLeafNode(node, key, value);
    }
}

// Search function
std::string BPlusTree::search(int key) {
    BPlusTreeNode* node = root;

    while (!node->isLeaf) {
        int i = 0;
        while (i < static_cast<int>(node->keys.size()) && key > node->keys[i]) i++;
        node = node->children[i];
    }

    for (int i = 0; i < static_cast<int>(node->keys.size()); i++) {
        if (node->keys[i] == key) {
            return node->values[i];
        }
    }
    return "Not found";
}

// Remove function (Basic Implementation)
void BPlusTree::remove(int key) {
    BPlusTreeNode* node = root;
    while (!node->isLeaf) {
        int i = 0;
        while (i < static_cast<int>(node->keys.size()) && key > node->keys[i]) i++;
        node = node->children[i];
    }

    auto it = std::find(node->keys.begin(), node->keys.end(), key);
    if (it != node->keys.end()) {
        int index = static_cast<int>(it - node->keys.begin());
        node->keys.erase(it);
        node->values.erase(node->values.begin() + index);
    }
}

// Split leaf node
void BPlusTree::splitLeafNode(BPlusTreeNode* node, int key, const std::string& value) {
    int mid = (node->keys.size() + 1) / 2;
    BPlusTreeNode* newLeaf = new BPlusTreeNode(true);

    newLeaf->keys.assign(node->keys.begin() + mid, node->keys.end());
    newLeaf->values.assign(node->values.begin() + mid, node->values.end());

    node->keys.resize(mid);
    node->values.resize(mid);

    newLeaf->nextLeaf = node->nextLeaf;
    node->nextLeaf = newLeaf;

    if (node == root) {
        BPlusTreeNode* newRoot = new BPlusTreeNode(false);
        newRoot->keys.push_back(newLeaf->keys[0]);
        newRoot->children.push_back(node);
        newRoot->children.push_back(newLeaf);
        root = newRoot;
    } else {
        insertInternal(newLeaf->keys[0], findParent(root, node), newLeaf);
    }
}

// Split internal node
void BPlusTree::splitInternalNode(BPlusTreeNode* node) {
    int mid = node->keys.size() / 2;
    BPlusTreeNode* newInternal = new BPlusTreeNode(false);

    int pushUpKey = node->keys[mid];
    
    newInternal->keys.assign(node->keys.begin() + mid + 1, node->keys.end());
    newInternal->children.assign(node->children.begin() + mid + 1, node->children.end());

    node->keys.resize(mid);
    node->children.resize(mid + 1);

    if (node == root) {
        BPlusTreeNode* newRoot = new BPlusTreeNode(false);
        newRoot->keys.push_back(pushUpKey);
        newRoot->children.push_back(node);
        newRoot->children.push_back(newInternal);
        root = newRoot;
    } else {
        insertInternal(pushUpKey, findParent(root, node), newInternal);
    }
}

// Insert into internal node
void BPlusTree::insertInternal(int key, BPlusTreeNode* parent, BPlusTreeNode* child) {
    auto it = std::lower_bound(parent->keys.begin(), parent->keys.end(), key);
    int index = it - parent->keys.begin();

    parent->keys.insert(it, key);
    parent->children.insert(parent->children.begin() + index + 1, child);

    if (static_cast<int>(parent->keys.size()) > maxKeys) {
        splitInternalNode(parent);
    }
}

// Find parent of a node
BPlusTreeNode* BPlusTree::findParent(BPlusTreeNode* current, BPlusTreeNode* child) {
    if (current->isLeaf || current->children[0]->isLeaf) return nullptr;

    for (size_t i = 0; i < current->children.size(); i++) {
        if (current->children[i] == child) return current;
    }

    for (size_t i = 0; i < current->children.size(); i++) {
        BPlusTreeNode* parent = findParent(current->children[i], child);
        if (parent) return parent;
    }

    return nullptr;
}

// Save a node to disk
void BPlusTree::saveNodeToDisk(std::ofstream& file, BPlusTreeNode* node) {
    if (!node) return;

    // Save node metadata
    file << node->isLeaf << " " << node->keys.size() << " ";

    // Save keys
    for (int key : node->keys) {
        file << key << " ";
    }

    // Save values (if leaf)
    if (node->isLeaf) {
        for (const std::string& value : node->values) {
            file << value << " ";
        }
    }

    // Save children (if not leaf)
    if (!node->isLeaf) {
        for (BPlusTreeNode* child : node->children) {
            saveNodeToDisk(file, child);
        }
    }
}

// Load a node from disk
BPlusTreeNode* BPlusTree::loadNodeFromDisk(std::ifstream& file) {
    if (!file) return nullptr;

    bool isLeaf;
    int numKeys;
    file >> isLeaf >> numKeys;

    BPlusTreeNode* node = new BPlusTreeNode(isLeaf);

    // Load keys
    for (int i = 0; i < numKeys; i++) {
        int key;
        file >> key;
        node->keys.push_back(key);
    }

    // Load values (if leaf)
    if (isLeaf) {
        for (int i = 0; i < numKeys; i++) {
            std::string value;
            file >> value;
            node->values.push_back(value);
        }
    }

    // Load children (if not leaf)
    if (!isLeaf) {
        for (int i = 0; i <= numKeys; i++) {
            BPlusTreeNode* child = loadNodeFromDisk(file);
            node->children.push_back(child);
        }
    }

    return node;
}

// Save B+ Tree to disk
void BPlusTree::saveToDisk(const std::string& filename) {
    std::ofstream file(filename, std::ios::binary);
    if (!file) {
        std::cerr << "Error: Cannot open file for saving.\n";
        return;
    }

    saveNodeToDisk(file, root);
    file.close();
}

void BPlusTree::loadFromDisk(const std::string& filename) {
    std::ifstream file(filename, std::ios::binary);
    if (!file) {
        std::cerr << "Error: Cannot open file for loading.\n";
        return;
    }

    root = loadNodeFromDisk(file);
    file.close();
}

// void BPlusTree::saveToDisk(const std::string& filename) {
//     std::ofstream file(filename, std::ios::binary);
//     if (!file) {
//         std::cerr << "Error: Cannot open file for saving.\n";
//         return;
//     }

//     saveNodeToDisk(file, root);
//     file.close();
// }

// // Load B+ Tree from disk
// void BPlusTree::loadFromDisk(const std::string& filename) {
//     std::ifstream file(filename, std::ios::binary);
//     if (!file) {
//         std::cerr << "Error: Cannot open file for loading.\n";
//         return;
//     }

//     root = loadNodeFromDisk(file);
//     file.close();
// }

// Display Tree
void BPlusTree::display() {
    BPlusTreeNode* node = root;
    while (!node->isLeaf) node = node->children[0];

    while (node) {
        for (int key : node->keys) {
            cout << key << " ";
        }
        cout << " | ";
        node = node->nextLeaf;
    }
    cout << endl;
}