#ifndef BPLUSTREE_H
#define BPLUSTREE_H

#include <vector>
#include <string>
#include <fstream>

class BPlusTreeNode {
public:
    bool isLeaf;
    std::vector<int> keys;
    std::vector<BPlusTreeNode*> children;
    std::vector<std::string> values;
    BPlusTreeNode* nextLeaf;

    BPlusTreeNode(bool leaf);
    ~BPlusTreeNode();
};

class BPlusTree {
private:
    BPlusTreeNode* root;
    int maxKeys;

    void insertInternal(int key, BPlusTreeNode* parent, BPlusTreeNode* child);
    void splitLeafNode(BPlusTreeNode* node);
    void splitInternalNode(BPlusTreeNode* node);
    BPlusTreeNode* findParent(BPlusTreeNode* current, BPlusTreeNode* child);
    void deleteTree(BPlusTreeNode* node);
    void saveNodeToDisk(std::ofstream& file, BPlusTreeNode* node);
    BPlusTreeNode* loadNodeFromDisk(std::ifstream& file);

public:
    BPlusTree(int order);
    ~BPlusTree();
    void insert(int key, const std::string& value);
    std::string search(int key);
    void remove(int key);
    void saveToDisk(const std::string& filename);
    void loadFromDisk(const std::string& filename);
    void display();
};

#endif