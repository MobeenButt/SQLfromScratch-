#include "BPlusTree.h"
#include <iostream>
#include <algorithm>
#include <fstream>

using namespace std;

// Constructor
BPlusTreeNode::BPlusTreeNode(bool leaf) : isLeaf(leaf), nextLeaf(nullptr) {}

// Destructor
BPlusTreeNode::~BPlusTreeNode() {
    for (auto child : children) {
        delete child;
    }
}

// Constructor for BPlusTree
BPlusTree::BPlusTree(int order) : root(nullptr), maxKeys(order - 1) {
    root = new BPlusTreeNode(true);
}

// Destructor for BPlusTree
BPlusTree::~BPlusTree() {
    deleteTree(root);
}

// Recursively delete the tree
void BPlusTree::deleteTree(BPlusTreeNode* node) {
    if (!node) return;
    if (!node->isLeaf) {
        for (auto child : node->children) {
            deleteTree(child);
        }
    }
    delete node;
}

// Insert a key-value pair into the B+ Tree
void BPlusTree::insert(int key, const std::string& value) {
    BPlusTreeNode* node = root;

    // Find the correct leaf node
    while (!node->isLeaf) {
        size_t i = 0;
        while (i < node->keys.size() && key > node->keys[i]) i++;
        node = node->children[i];
    }

    // Insert the key-value pair into the leaf node
    auto it = lower_bound(node->keys.begin(), node->keys.end(), key);
    size_t index = it - node->keys.begin();
    node->keys.insert(it, key);
    node->values.insert(node->values.begin() + index, value);

    // Split the leaf node if it exceeds the maximum number of keys
    if (node->keys.size() > static_cast<size_t>(maxKeys)) {
        splitLeafNode(node);
    }
}

// Search for a key in the B+ Tree
std::string BPlusTree::search(int key) {
    BPlusTreeNode* node = root;

    // Traverse to the correct leaf node
    while (!node->isLeaf) {
        size_t i = 0;
        while (i < node->keys.size() && key > node->keys[i]) i++;
        node = node->children[i];
    }

    // Search for the key in the leaf node
    auto it = lower_bound(node->keys.begin(), node->keys.end(), key);
    if (it != node->keys.end() && *it == key) {
        return node->values[it - node->keys.begin()];
    }
    return "Not found";
}

// Remove a key from the B+ Tree
void BPlusTree::remove(int key) {
    BPlusTreeNode* node = root;

    // Traverse to the correct leaf node
    while (!node->isLeaf) {
        size_t i = 0;
        while (i < node->keys.size() && key > node->keys[i]) i++;
        node = node->children[i];
    }

    // Remove the key-value pair from the leaf node
    auto it = lower_bound(node->keys.begin(), node->keys.end(), key);
    if (it != node->keys.end() && *it == key) {
        size_t index = it - node->keys.begin();
        node->keys.erase(it);
        node->values.erase(node->values.begin() + index);
    }
}

// Split a leaf node
void BPlusTree::splitLeafNode(BPlusTreeNode* node) {
    size_t mid = (node->keys.size() + 1) / 2;
    BPlusTreeNode* newLeaf = new BPlusTreeNode(true);

    // Move half of the keys and values to the new leaf node
    newLeaf->keys.assign(node->keys.begin() + mid, node->keys.end());
    newLeaf->values.assign(node->values.begin() + mid, node->values.end());
    node->keys.resize(mid);
    node->values.resize(mid);

    // Update the linked list of leaf nodes
    newLeaf->nextLeaf = node->nextLeaf;
    node->nextLeaf = newLeaf;

    // Insert the new leaf node into the parent
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

// Split an internal node
void BPlusTree::splitInternalNode(BPlusTreeNode* node) {
    size_t mid = node->keys.size() / 2;
    BPlusTreeNode* newInternal = new BPlusTreeNode(false);

    // Move half of the keys and children to the new internal node
    newInternal->keys.assign(node->keys.begin() + mid + 1, node->keys.end());
    newInternal->children.assign(node->children.begin() + mid + 1, node->children.end());
    node->keys.resize(mid);
    node->children.resize(mid + 1);

    // Insert the new internal node into the parent
    if (node == root) {
        BPlusTreeNode* newRoot = new BPlusTreeNode(false);
        newRoot->keys.push_back(node->keys[mid]);
        newRoot->children.push_back(node);
        newRoot->children.push_back(newInternal);
        root = newRoot;
    } else {
        insertInternal(node->keys[mid], findParent(root, node), newInternal);
    }
}

// Insert into an internal node
void BPlusTree::insertInternal(int key, BPlusTreeNode* parent, BPlusTreeNode* child) {
    auto it = lower_bound(parent->keys.begin(), parent->keys.end(), key);
    size_t index = it - parent->keys.begin();
    parent->keys.insert(it, key);
    parent->children.insert(parent->children.begin() + index + 1, child);

    // Split the internal node if it exceeds the maximum number of keys
    if (parent->keys.size() > static_cast<size_t>(maxKeys)) {
        splitInternalNode(parent);
    }
}

// Find the parent of a node
BPlusTreeNode* BPlusTree::findParent(BPlusTreeNode* current, BPlusTreeNode* child) {
    if (current->isLeaf || current->children[0] == child) return nullptr;

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
    size_t numKeys;
    file >> isLeaf >> numKeys;

    BPlusTreeNode* node = new BPlusTreeNode(isLeaf);

    // Load keys
    for (size_t i = 0; i < numKeys; i++) {
        int key;
        file >> key;
        node->keys.push_back(key);
    }

    // Load values (if leaf)
    if (isLeaf) {
        for (size_t i = 0; i < numKeys; i++) {
            std::string value;
            file >> value;
            node->values.push_back(value);
        }
    }

    // Load children (if not leaf)
    if (!isLeaf) {
        for (size_t i = 0; i <= numKeys; i++) {
            BPlusTreeNode* child = loadNodeFromDisk(file);
            node->children.push_back(child);
        }
    }

    return node;
}

// Save the B+ Tree to disk
void BPlusTree::saveToDisk(const std::string& filename) {
    std::ofstream file(filename, std::ios::binary);
    if (!file) {
        std::cerr << "Error: Cannot open file for saving.\n";
        return;
    }

    saveNodeToDisk(file, root);
    file.close();
}

// Load the B+ Tree from disk
void BPlusTree::loadFromDisk(const std::string& filename) {
    std::ifstream file(filename, std::ios::binary);
    if (!file) {
        std::cerr << "Error: Cannot open file for loading.\n";
        return;
    }

    root = loadNodeFromDisk(file);
    file.close();
}

// Display the B+ Tree (for debugging)
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