#pragma once
#include "avl.h"
#include "hashtable.h"

class Zset {
public:
    AVLNode* tree = nullptr;
    Hmap map;
};

class Znode {
public:
    AVLNode tree;
    Hnode map;
    double score = 0;
    size_t len = 0;
    char name[0];
};

bool zset_add(Zset* set, const char* name, size_t len, double score);
Znode* zset_find(Zset* set, const char* name, size_t len);
Znode* zset_pop(Zset* set, const char* name, size_t len);
Znode* zset_query(Zset* set, double score, const char* name, size_t len);
void zset_dispose(Zset* set);
Znode* znode_offset(Znode* node, int64_t offset);
void znode_del(Znode* node);