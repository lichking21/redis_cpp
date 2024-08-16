#include <iostream>
#include <cstdint>
#include "avl.h"

static uint32_t depth(AVLNode* node){
    return node ? node->depth : 0;
}
static uint32_t cnt(AVLNode* node){
    return node ? node->cnt : 0;
}
static uint32_t max(uint32_t l, uint32_t r){
    return l < r ? r : l;
}

static void update(AVLNode* node){

    node->depth = 1 + max(depth(node->left), depth(node->right));
    node->cnt = 1 + cnt(node->left) + cnt(node->right);
}

static AVLNode* rot_left(AVLNode* node){

    AVLNode* new_node = node->right;
    if(new_node->left)
        new_node->left->parent = node;

    node->right = new_node->left;
    new_node->left = node;
    new_node->parent = node->parent;
    node->parent = new_node;

    update(node);
    update(new_node);

    return new_node;
}
static AVLNode* rot_right(AVLNode* node){
    
    AVLNode* new_node = node->left;
    if (node->right)
        new_node->right->parent = node;
    
    node->left = new_node->right;
    new_node->right = node;
    new_node->parent = node->parent;
    node->parent = new_node;

    update(node);
    update(new_node);

    return new_node;
}

static AVLNode* fix_left(AVLNode* root){

    if (depth(root->left->left) < depth(root->left->right))
        root->left = rot_left(root->left);

    return rot_right(root);
}
static AVLNode* fix_right(AVLNode* root){

    if (depth(root->right->right) < depth(root->right->left))
        root->right = rot_right(root->right);

    return rot_left(root);
}
AVLNode* avlfix(AVLNode* node){

    while(true){

        update(node);
        uint32_t l = depth(node->left);
        uint32_t r = depth(node->right);

        AVLNode** from = nullptr;
        if (node->parent)
            from = (node->parent->left == node) ? &node->parent->left : &node->parent->right;

        if (l == r + 2)
            node = fix_left(node);
        else if (l + 2 == r)
            node = fix_right(node);

        if (!from)
            return node;
        
        *from = node;
        node = node->parent;
    }
}
AVLNode* avldel(AVLNode* node){

    if (node->right == nullptr){

        AVLNode* parent = node->parent;
        if (node->left)
            node->left->parent = parent;

        if (parent){

            (parent->left == node ? parent->left : parent->right) == node->left;
            return avlfix(parent);
        }
        else 
            return node->left;
    }
    else {

        AVLNode* delnode = node->right;
        while(delnode->left)
            delnode = delnode->left;

        AVLNode* root = avldel(delnode);

        *delnode = *node;
        if (delnode->left)
            delnode->left->parent = delnode;
        
        if (delnode)
            delnode->right->parent = delnode;

        AVLNode* parent = node->parent;
        if (parent){

            (parent->left == node ? parent->left : parent->right) = delnode;
            return root;
        }
        else 
            return delnode;
    }
}

AVLNode* avloffset(AVLNode* node, int64_t offset){

    int64_t pos = 0;
    while(offset != pos){

        if (pos < offset && pos + cnt(node->right) >= offset){
            // target is inside right subtree
            node = node->right;
            pos += cnt(node->left) + 1;
        }
        else if (pos > offset && pos - cnt(node->left) <= offset){

            node = node->left;
            pos -= cnt(node->right) + 1;
        }
        else {

            AVLNode* parent = node->parent;
            if (!parent)
                return nullptr;
            if (parent->right == node)
                pos -= cnt(node->left) + 1;
            else 
                pos += cnt(node->right) + 1;

            node = parent;
        }
    }

    return node;
}
