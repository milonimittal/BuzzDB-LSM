#include "redBlackTree.h"
#include <iostream>

using namespace std;

void RedBlackTree::bstInsert(Node* newNode){
    Node* parent = nullptr;
    Node* current = root;
    while (current != NIL) {
        parent = current;
        if (newNode->key < current->key) {
            current = current->left;
        }
        else {
            current = current->right;
        }
    }

    newNode->parent = parent;
    if (parent == nullptr) {
        root = newNode;
    }
    else if (newNode->key < parent->key) {
        parent->left = newNode;
    }
    else {
        parent->right = newNode;
    }

    if (newNode->parent == nullptr) {
        newNode->colour = BLACK;
        return;
    }

    if (newNode->parent->parent == nullptr) {
        return;
    }
}

void RedBlackTree::leftRotate(Node* x) {
    Node* y = x->right;
    x->right = y->left;
    if (y->left != NIL) {
        y->left->parent = x;
    }
    y->parent = x->parent;
    if (x->parent == nullptr) {
        root = y;
    }
    else if (x == x->parent->left) {
        x->parent->left = y;
    }
    else {
        x->parent->right = y;
    }
    y->left = x;
    x->parent = y;
}

void RedBlackTree::rightRotate(Node* x) {
    Node* y = x->left;
    x->left = y->right;
    if (y->right != NIL) {
        y->right->parent = x;
    }
    y->parent = x->parent;
    if (x->parent == nullptr) {
        root = y;
    }
    else if (x == x->parent->right) {
        x->parent->right = y;
    }
    else {
        x->parent->left = y;
    }
    y->right = x;
    x->parent = y;
}

void RedBlackTree::fixTree(Node* k) {
    while (k != root && k->parent->colour == RED) {
        if (k->parent == k->parent->parent->left) {
            Node* u = k->parent->parent->right;
            if (u->colour == RED) {
                k->parent->colour = BLACK;
                u->colour = BLACK;
                k->parent->parent->colour = RED;
                k = k->parent->parent;
            }
            else {
                if (k == k->parent->right) {
                    k = k->parent;
                    leftRotate(k);
                }
                k->parent->colour = BLACK;
                k->parent->parent->colour = RED;
                rightRotate(k->parent->parent);
            }
        }
        else {
            Node* u = k->parent->parent->left;
            if (u->colour == RED) {
                k->parent->colour = BLACK;
                u->colour = BLACK;
                k->parent->parent->colour = RED;
                k = k->parent->parent;
            }
            else {
                if (k == k->parent->left) {
                    k = k->parent;
                    rightRotate(k);
                }
                k->parent->colour = BLACK;
                k->parent->parent->colour = RED;
                leftRotate(k->parent->parent);
            }
        }
    }
    root->colour = BLACK;
}

void RedBlackTree::inorder(Node* node) {
    if (node != NIL) {
        inorder(node->left);
        cout << node->key << " "<< node->value<<endl;
        inorder(node->right);
    }
}

RedBlackTree::RedBlackTree() {
    NIL = new Node(0, 0);
    NIL->colour = BLACK;
    NIL->left = NIL->right = NIL;
    root = NIL;
    numNodes = 0;
}

void RedBlackTree::insert(int key, int value) {
    Node* newNode = new Node(key, value);
    newNode->left = NIL;
    newNode->right = NIL;

    bstInsert(newNode);
    fixTree(newNode);
    numNodes++;
}

void RedBlackTree::printInorder() { 
    inorder(root); 
}
