#include "BPlusTree.h"
#include <iostream>

BPlusTree::Node::Node(bool leaf) : isLeaf(leaf), nextLeaf(nullptr) {}

json BPlusTree::Node::serialize() {
    json j;
    j["isLeaf"] = isLeaf;
    j["keys"] = keys;
    j["values"] = values;
    j["nextLeaf"] = (nextLeaf != nullptr);
    return j;
}

BPlusTree::Node* BPlusTree::Node::deserialize(const json& j) {
    Node* node = new Node(j["isLeaf"]);
    node->keys = j["keys"].get<std::vector<int>>();
    node->values = j["values"].get<std::vector<std::string>>();
    return node;
}

BPlusTree::BPlusTree() {
    root = new Node(true);
    loadFromFile(); // Load existing data
}

BPlusTree::~BPlusTree() {
    saveToFile(); // Save before exiting
}

void BPlusTree::insert(int key, std::string value) {
    Node* leaf = root;
    while (!leaf->isLeaf) {
        auto it = std::upper_bound(leaf->keys.begin(), leaf->keys.end(), key);
        int index = it - leaf->keys.begin();
        leaf = leaf->children[index];
    }

    auto it = std::upper_bound(leaf->keys.begin(), leaf->keys.end(), key);
    int index = it - leaf->keys.begin();
    leaf->keys.insert(it, key);
    leaf->values.insert(leaf->values.begin() + index, value);

    if (leaf->keys.size() >= ORDER) {
        splitLeaf(leaf);
    }
    
    saveToFile();
}




std::string BPlusTree::search(int key) {
    Node* current = root;
    while (!current->isLeaf) {
        auto it = std::upper_bound(current->keys.begin(), current->keys.end(), key);
        int index = it - current->keys.begin();
        current = current->children[index];
    }

    for (size_t i = 0; i < current->keys.size(); i++) {
        if (current->keys[i] == key) {
            return current->values[i]; // Return the found value
        }
    }

    return "Key not found";
}



void BPlusTree::saveToFile() {
    std::ofstream file("bplustree.json");
    json j;
    j["root"] = root->serialize();
    file << j.dump(4);
}

void BPlusTree::loadFromFile() {
    std::ifstream file("bplustree.json");
    if (!file) return;
    json j;
    file >> j;
    root = Node::deserialize(j["root"]);
}

void BPlusTree::printTree(Node* node, int level) {
    if (node == nullptr) return;
    std::cout << "Level " << level << ": ";
    for (int key : node->keys) {
        std::cout << key << " ";
    }
    std::cout << "\n";
    
    if (!node->isLeaf) {
        for (Node* child : node->children) {
            printTree(child, level + 1);
        }
    }
}
void BPlusTree::display() {
    BPlusTree::printTree(root, 0);
}

void BPlusTree::insertInternal(int key, Node* leftChild, Node* rightChild) {
    if (leftChild->parent == nullptr) {
        root = new Node(false);
        root->keys.push_back(key);
        root->children.push_back(leftChild);
        root->children.push_back(rightChild);
        leftChild->parent = root;
        rightChild->parent = root;
        return;
    }
    
    Node* parent = leftChild->parent;
    auto it = std::upper_bound(parent->keys.begin(), parent->keys.end(), key);
    int index = it - parent->keys.begin();
    parent->keys.insert(it, key);
    parent->children.insert(parent->children.begin() + index + 1, rightChild);
    rightChild->parent = parent;
    
    if (parent->keys.size() >= ORDER)
        splitInternal(parent);
}

void BPlusTree::splitLeaf(Node* leaf) {
    int mid = ORDER / 2;
    Node* newLeaf = new Node(true);
    newLeaf->keys.assign(leaf->keys.begin() + mid, leaf->keys.end());
    newLeaf->values.assign(leaf->values.begin() + mid, leaf->values.end());
    leaf->keys.resize(mid);
    leaf->values.resize(mid);
    newLeaf->nextLeaf = leaf->nextLeaf;
    leaf->nextLeaf = newLeaf;
    newLeaf->parent = leaf->parent;
    
    insertInternal(newLeaf->keys[0], leaf, newLeaf);
}

void BPlusTree::splitInternal(Node* node) {
    int mid = ORDER / 2;
    Node* newNode = new Node(false);
    newNode->keys.assign(node->keys.begin() + mid + 1, node->keys.end());
    newNode->children.assign(node->children.begin() + mid + 1, node->children.end());
    node->keys.resize(mid);
    node->children.resize(mid + 1);
    newNode->parent = node->parent;
    
    for (auto* child : newNode->children)
        child->parent = newNode;
    
    insertInternal(node->keys[mid], node, newNode);
}
void BPlusTree::mergeLeaf(Node* leaf) {
    if (!leaf->parent) return; // Root case

    Node* parent = leaf->parent;
    int index = -1;

    for (size_t i = 0; i < parent->children.size(); i++) {
        if (parent->children[i] == leaf) {
            index = i;
            break;
        }
    }

    if (index > 0) { // Merge with left sibling
        Node* leftSibling = parent->children[index - 1];
        leftSibling->keys.insert(leftSibling->keys.end(), leaf->keys.begin(), leaf->keys.end());
        leftSibling->values.insert(leftSibling->values.end(), leaf->values.begin(), leaf->values.end());
        leftSibling->nextLeaf = leaf->nextLeaf;
        
        parent->keys.erase(parent->keys.begin() + (index - 1));
        parent->children.erase(parent->children.begin() + index);
        delete leaf;
    } else if (index < parent->children.size() - 1) { // Merge with right sibling
        Node* rightSibling = parent->children[index + 1];
        leaf->keys.insert(leaf->keys.end(), rightSibling->keys.begin(), rightSibling->keys.end());
        leaf->values.insert(leaf->values.end(), rightSibling->values.begin(), rightSibling->values.end());
        leaf->nextLeaf = rightSibling->nextLeaf;
        
        parent->keys.erase(parent->keys.begin() + index);
        parent->children.erase(parent->children.begin() + (index + 1));
        delete rightSibling;
    }

    if (parent->keys.size() < (ORDER / 2)) {
        mergeInternal(parent);
    }
}
void BPlusTree::mergeInternal(Node* node) {
    if (!node->parent) { // If root becomes empty, update root
        if (node->children.size() == 1) {
            root = node->children[0];
            root->parent = nullptr;
            delete node;
        }
        return;
    }

    Node* parent = node->parent;
    int index = -1;

    for (size_t i = 0; i < parent->children.size(); i++) {
        if (parent->children[i] == node) {
            index = i;
            break;
        }
    }

    if (index > 0) { // Merge with left sibling
        Node* leftSibling = parent->children[index - 1];
        leftSibling->keys.push_back(parent->keys[index - 1]);
        leftSibling->keys.insert(leftSibling->keys.end(), node->keys.begin(), node->keys.end());
        leftSibling->children.insert(leftSibling->children.end(), node->children.begin(), node->children.end());

        for (Node* child : node->children) {
            child->parent = leftSibling;
        }

        parent->keys.erase(parent->keys.begin() + (index - 1));
        parent->children.erase(parent->children.begin() + index);
        delete node;
    } else if (index < parent->children.size() - 1) { // Merge with right sibling
        Node* rightSibling = parent->children[index + 1];
        node->keys.push_back(parent->keys[index]);
        node->keys.insert(node->keys.end(), rightSibling->keys.begin(), rightSibling->keys.end());
        node->children.insert(node->children.end(), rightSibling->children.begin(), rightSibling->children.end());

        for (Node* child : rightSibling->children) {
            child->parent = node;
        }

        parent->keys.erase(parent->keys.begin() + index);
        parent->children.erase(parent->children.begin() + (index + 1));
        delete rightSibling;
    }

    if (parent->keys.size() < (ORDER / 2)) {
        mergeInternal(parent);
    }
}

void BPlusTree::remove(int key) {
    Node* leaf = root;
    
    while (!leaf->isLeaf) {
        auto it = std::upper_bound(leaf->keys.begin(), leaf->keys.end(), key);
        int index = it - leaf->keys.begin();
        leaf = leaf->children[index];
    }

    auto it = std::find(leaf->keys.begin(), leaf->keys.end(), key);
    if (it == leaf->keys.end()) {
        std::cout << "Key not found\n";
        return;
    }

    int index = it - leaf->keys.begin();
    leaf->keys.erase(it);
    leaf->values.erase(leaf->values.begin() + index);

    // Merge if needed
    if (leaf->keys.size() < (ORDER / 2)) {
        mergeLeaf(leaf);
    }

    saveToFile();
}
