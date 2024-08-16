#include <iostream>
#include <cstdint>

class AVLNode {
public:
    uint32_t depth = 0;
    uint32_t cnt = 0;
    AVLNode*left = nullptr;
    AVLNode* right = nullptr;
    AVLNode* parent = nullptr;
};

inline void avlinit(AVLNode* node){

    node->depth = 1;
    node->cnt = 1;
    node->left = node->right = node->parent = nullptr;
}

AVLNode* avlfix(AVLNode* node);
AVLNode* avldel(AVLNode* node);
AVLNode* avloffset(AVLNode* node, int64_t offset);
