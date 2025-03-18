// BPlusTree.cpp
#include "BPlusTree.h"

void BPlusTree::insert(int key, string value) {
    tree[key] = value;
}

string BPlusTree::search(int key) {
    if (tree.find(key) != tree.end()) {
        return tree[key];
    }
    return "";
}

void BPlusTree::remove(int key) {
    tree.erase(key);
}
