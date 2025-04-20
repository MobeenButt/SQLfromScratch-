#pragma once
#include <cstring>

#define PAGE_SIZE 4096  // 4KB page size

struct Page {
    char data[PAGE_SIZE];
    int free_space;
    
    Page() : free_space(PAGE_SIZE) {
        memset(data, 0, PAGE_SIZE);
    }
}; 