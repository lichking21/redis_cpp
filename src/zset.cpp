#include <iostream>
#include <cassert>
#include <cstring>
#include "zset.h"
#include "common.h"

static Znode* znode_new(const char* name, size_t len, double score){

    Znode* node = new Znode();
    assert(node);
    avlinit(&node->tree);

    node->map.next = nullptr;
    node->map.hcode = str_hash((uint8_t*)name, len);
    node->score = score;
    node->len = len;
    memcpy(&node->name[0], name, len);

    return node;
}

static uint32_t min(size_t lhs, size_t rhs){
    return lhs < rhs ? lhs : rhs;
}

static bool zless(AVLNode* lhs, double score, const char* name, size_t len){

    Znode* zl = container_of(lhs, Znode, tree);
    if (zl->score != score)
        return zl->score < score;

    int rv = memcmp(zl->name, name, min(zl->len, len));
    if (rv != 0)
        return rv < 0;

    return zl->len < len;
}
static bool zless(AVLNode* lhs, AVLNode* rhs){

    Znode* zr = container_of(rhs, Znode, tree);
    return zless(lhs, zr->score, zr->name, zr->len);
}

static void tree_add(Zset* set, Znode* node){

    AVLNode* cur = nullptr;
    AVLNode** from = &set->tree;

    while(*from){

        cur = *from;
        from = zless(&node->tree, cur) ? &cur->left : &cur->right;
    }    

    *from = &node->tree;
    node->tree.parent = cur;
    set->tree = avlfix(&node->tree);
}

static void zset_update(Zset* set, Znode* node, double score){

    if (node->score == score)
        return;

    set->tree = avldel(&node->tree);
    node->score = score;
    avlinit(&node->tree);
    tree_add(set, node);
}

bool zset_add(Zset* set, const char* name, size_t len, double score){

    Znode* node = zset_find(set, name, len);
    if (node){

        zset_update(set, node, score);
        return false;
    }
    else {

        node = znode_new(name, len, score);
        map_insert(&set->map, &node->map);
        tree_add(set, node);
        return true;
    }
}

struct Hkey {

    Hnode node;
    const char* name = nullptr;
    size_t len = 0;
};
static bool hcmp(Hnode* node, Hnode* key){

    Znode* znode = container_of(node, Znode, map);
    Hkey* hkey = container_of(key, Hkey, node);
    
    if (znode->len != hkey->len)
        return false;

    return 0 == memcmp(znode->name, hkey->name, znode->len);
}


Znode* zset_find(Zset* set, const char* name, size_t len){

    if (!set->tree)
        return nullptr;

    Hkey key;
    key.node.hcode = str_hash((uint8_t*)name, len);
    key.name = name;
    key.len = len;
    
    Hnode* found = map_find(&set->map, &key.node, &hcmp);
    
    return found ? container_of(found, Znode, map) : nullptr;
}

Znode* zset_pop(Zset* set, const char* name, size_t len){

    if (!set->tree)
        return nullptr;

    Hkey key;
    key.node.hcode = str_hash((uint8_t*)name, len);
    key.name = name;
    key.len = len;

    Hnode* found = map_find(&set->map, &key.node, &hcmp);

    if (!found) 
        return nullptr;

    Znode* node = container_of(found, Znode, map);
    set->tree = avldel(&node->tree);
    return node;
}

Znode* zset_query(Zset* set, double score, const char* name, size_t len){

    AVLNode* found = nullptr;
    AVLNode* cur = set->tree;

    while(cur){

        if (zless(cur, score, name, len))
            cur = cur->right;
        else {

            found = cur;
            cur = cur->left;
        }
    }

    return found ? container_of(found, Znode, tree) : nullptr;
}


Znode* znode_offset(Znode* node, int64_t offset){

    AVLNode* tnode = node ? avloffset(&node->tree, offset) : nullptr;
    return tnode ? container_of(tnode, Znode, tree) : nullptr;
}

void znode_del(Znode* node){
    free(node);
}

static void tree_dispose(AVLNode* node){

    if (!node)
        return;

    tree_dispose(node->left);
    tree_dispose(node->right);
    znode_del(container_of(node, Znode, tree));
}

void zset_dispose(Zset* set){

    tree_dispose(set->tree);
    map_destroy(&set->map);
}