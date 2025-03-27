#ifndef REDBLACKTREE_H
#define REDBLACKTREE_H

#include <iostream>
#include <string>

enum Colour {
    RED,
    BLACK
};

struct Node {
    int key, value;
    Colour colour;
    Node *left, *right, *parent;

    Node(int key, int value): key(key), value(value), colour(RED), left(nullptr), right(nullptr), parent(nullptr){}
};

class RedBlackTree {
    private:
        int numNodes;
        Node* root;
        Node* NIL;

        void bstInsert(Node* newNode);
        void leftRotate(Node* x);
        void rightRotate(Node* x);
        void fixTree(Node* k);
        void inorder(Node* node);

    public:
        RedBlackTree();
        void insert(int key, int value);
        void printInorder();
};

#endif