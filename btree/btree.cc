#include <assert.h>
#include <string.h>
#include "btree.h"

KeyValuePair::KeyValuePair() { }


KeyValuePair::KeyValuePair(const KEY_T &k, const VALUE_T &v) : key(k), value(v) { }


KeyValuePair::KeyValuePair(const KeyValuePair &rhs) : key(rhs.key), value(rhs.value) { }


KeyValuePair::~KeyValuePair() { }


KeyValuePair &KeyValuePair::operator=(const KeyValuePair &rhs) {
    return *(new(this) KeyValuePair(rhs));
}

BTreeIndex::BTreeIndex(SIZE_T keysize, SIZE_T valuesize, BufferCache *cache, bool unique) {
    superblock.info.keysize = keysize;
    superblock.info.valuesize = valuesize;
    buffercache = cache;
    // note: ignoring unique now
}

BTreeIndex::BTreeIndex() {
    // shouldn't have to do anything
}


//
// Note, will not attach!
//
BTreeIndex::BTreeIndex(const BTreeIndex &rhs) {
    buffercache = rhs.buffercache;
    superblock_index = rhs.superblock_index;
    superblock = rhs.superblock;
}

BTreeIndex::~BTreeIndex() {
    // shouldn't have to do anything
}


BTreeIndex &BTreeIndex::operator=(const BTreeIndex &rhs) {
    return *(new(this)BTreeIndex(rhs));
}


ERROR_T BTreeIndex::AllocateNode(SIZE_T &n) {
    n = superblock.info.freelist;

    if (n == 0) {
        return ERROR_NOSPACE;
    }

    BTreeNode node;

    node.Unserialize(buffercache, n);
    assert(node.info.nodetype == BTREE_UNALLOCATED_BLOCK);
    superblock.info.freelist = node.info.freelist;
    superblock.Serialize(buffercache, superblock_index);
    buffercache->NotifyAllocateBlock(n);

    return ERROR_NOERROR;
}


ERROR_T BTreeIndex::DeallocateNode(const SIZE_T &n) {
    BTreeNode node;

    node.Unserialize(buffercache, n);
    assert(node.info.nodetype != BTREE_UNALLOCATED_BLOCK);
    node.info.nodetype = BTREE_UNALLOCATED_BLOCK;
    node.info.freelist = superblock.info.freelist;
    node.Serialize(buffercache, n);
    superblock.info.freelist = n;
    superblock.Serialize(buffercache, superblock_index);
    buffercache->NotifyDeallocateBlock(n);

    return ERROR_NOERROR;

}

ERROR_T BTreeIndex::Attach(const SIZE_T initblock, const bool create) {
    ERROR_T rc;

    superblock_index = initblock;
    assert(superblock_index == 0);

    if (create) {
        // build a super block, root node, and a free space list
        //
        // Superblock at superblock_index
        // root node at superblock_index+1
        // free space list for rest
        BTreeNode newsuperblock(BTREE_SUPERBLOCK, superblock.info.keysize, superblock.info.valuesize,
                                buffercache->GetBlockSize());
        newsuperblock.info.rootnode = superblock_index + 1;
        newsuperblock.info.freelist = superblock_index + 2;
        newsuperblock.info.numkeys = 0;

        buffercache->NotifyAllocateBlock(superblock_index);

        rc = newsuperblock.Serialize(buffercache, superblock_index);

        if (rc) {
            return rc;
        }

        BTreeNode newrootnode(BTREE_ROOT_NODE, superblock.info.keysize, superblock.info.valuesize,
                              buffercache->GetBlockSize());
        newrootnode.info.rootnode = superblock_index + 1;
        newrootnode.info.freelist = superblock_index + 2;
        newrootnode.info.numkeys = 0;

        buffercache->NotifyAllocateBlock(superblock_index + 1);

        rc = newrootnode.Serialize(buffercache, superblock_index + 1);

        if (rc) {
            return rc;
        }

        for (SIZE_T i = superblock_index + 2; i < buffercache->GetNumBlocks(); i++) {
            BTreeNode newfreenode(BTREE_UNALLOCATED_BLOCK, superblock.info.keysize, superblock.info.valuesize,
                                  buffercache->GetBlockSize());
            newfreenode.info.rootnode = superblock_index + 1;
            newfreenode.info.freelist = ((i + 1) == buffercache->GetNumBlocks()) ? 0 : i + 1;

            rc = newfreenode.Serialize(buffercache, i);

            if (rc) {
                return rc;
            }

        }
    }

    // OK, now, mounting the btree is simply a matter of reading the superblock

    return superblock.Unserialize(buffercache, initblock);
}


ERROR_T BTreeIndex::Detach(SIZE_T &initblock) {
    return superblock.Serialize(buffercache, superblock_index);
}


ERROR_T BTreeIndex::LookupOrUpdateInternal(const SIZE_T &node, const BTreeOp op, const KEY_T &key, VALUE_T &value) {
    BTreeNode b;
    ERROR_T rc;
    SIZE_T offset;
    KEY_T testkey;
    SIZE_T ptr;

    rc = b.Unserialize(buffercache, node);

    if (rc != ERROR_NOERROR) {
        return rc;
    }

    switch (b.info.nodetype) {
        case BTREE_ROOT_NODE:
        case BTREE_INTERIOR_NODE:
            // Scan through key/ptr pairs
            //and recurse if possible
            for (offset = 0; offset < b.info.numkeys; offset++) {
                rc = b.GetKey(offset, testkey);
                if (rc) { return rc; }
                if (key < testkey || key == testkey) {
                    // OK, so we now have the first key that's larger
                    // so we ned to recurse on the ptr immediately previous to
                    // this one, if it exists
                    rc = b.GetPtr(offset, ptr);
                    if (rc) { return rc; }
                    return LookupOrUpdateInternal(ptr, op, key, value);
                }
            }
            // if we got here, we need to go to the next pointer, if it exists
            if (b.info.numkeys > 0) {
                rc = b.GetPtr(b.info.numkeys, ptr);
                if (rc) { return rc; }
                return LookupOrUpdateInternal(ptr, op, key, value);
            } else {
                // There are no keys at all on this node, so nowhere to go
                return ERROR_NONEXISTENT;
            }
            break;
        case BTREE_LEAF_NODE:
            // Scan through keys looking for matching value
            for (offset = 0; offset < b.info.numkeys; offset++) {
                rc = b.GetKey(offset, testkey);
                if (rc) { return rc; }
                if (testkey == key) {
                    if (op == BTREE_OP_LOOKUP) {
                        return b.GetVal(offset, value);
                    } else {
                        if ((rc = b.SetVal(offset, value))) return rc;
                        return b.Serialize(buffercache, node);
                    }
                }
            }
            return ERROR_NONEXISTENT;
            break;
        default:
            // We can't be looking at anything other than a root, internal, or leaf
            return ERROR_INSANE;
            break;
    }

    return ERROR_INSANE;
}


static ERROR_T PrintNode(ostream &os, SIZE_T nodenum, BTreeNode &b, BTreeDisplayType dt) {
    KEY_T key;
    VALUE_T value;
    SIZE_T ptr;
    SIZE_T offset;
    ERROR_T rc;
    unsigned i;

    if (dt == BTREE_DEPTH_DOT) {
        os << nodenum << " [ label=\"" << nodenum << ": ";
    } else if (dt == BTREE_DEPTH) {
        os << nodenum << ": ";
    } else {
    }

    switch (b.info.nodetype) {
        case BTREE_ROOT_NODE:
        case BTREE_INTERIOR_NODE:
            if (dt == BTREE_SORTED_KEYVAL) {
            } else {
                if (dt == BTREE_DEPTH_DOT) {
                } else {
                    os << "Interior: ";
                }
                for (offset = 0; offset <= b.info.numkeys; offset++) {
                    rc = b.GetPtr(offset, ptr);
                    if (rc) { return rc; }
                    os << "*" << ptr << " ";
                    // Last pointer
                    if (offset == b.info.numkeys) break;
                    rc = b.GetKey(offset, key);
                    if (rc) { return rc; }
                    for (i = 0; i < b.info.keysize; i++) {
                        os << key.data[i];
                    }
                    os << " ";
                }
            }
            break;
        case BTREE_LEAF_NODE:
            if (dt == BTREE_DEPTH_DOT || dt == BTREE_SORTED_KEYVAL) {
            } else {
                os << "Leaf: ";
            }
            for (offset = 0; offset < b.info.numkeys; offset++) {
                if (offset == 0) {
                    // special case for first pointer
                    rc = b.GetPtr(offset, ptr);
                    if (rc) { return rc; }
                    if (dt != BTREE_SORTED_KEYVAL) {
                        os << "*" << ptr << " ";
                    }
                }
                if (dt == BTREE_SORTED_KEYVAL) {
                    os << "(";
                }
                rc = b.GetKey(offset, key);
                if (rc) { return rc; }
                for (i = 0; i < b.info.keysize; i++) {
                    os << key.data[i];
                }
                if (dt == BTREE_SORTED_KEYVAL) {
                    os << ",";
                } else {
                    os << " ";
                }
                rc = b.GetVal(offset, value);
                if (rc) { return rc; }
                for (i = 0; i < b.info.valuesize; i++) {
                    os << value.data[i];
                }
                if (dt == BTREE_SORTED_KEYVAL) {
                    os << ")\n";
                } else {
                    os << " ";
                }
            }
            break;
        default:
            if (dt == BTREE_DEPTH_DOT) {
                os << "Unknown(" << b.info.nodetype << ")";
            } else {
                os << "Unsupported Node Type " << b.info.nodetype;
            }
    }
    if (dt == BTREE_DEPTH_DOT) {
        os << "\" ]";
    }
    return ERROR_NOERROR;
}


ERROR_T BTreeIndex::Lookup(const KEY_T &key, VALUE_T &value) {
    return LookupOrUpdateInternal(superblock.info.rootnode, BTREE_OP_LOOKUP, key, value);
}


SIZE_T BTreeIndex::IsFull(const SIZE_T &node) {
    BTreeNode b;
    b.Unserialize(buffercache, node);
    switch (b.info.nodetype) {
        case BTREE_ROOT_NODE:
        case BTREE_INTERIOR_NODE:
            return b.info.numkeys == b.info.GetNumSlotsAsInterior() ? 0 : 1;
        case BTREE_LEAF_NODE:
            return b.info.numkeys == b.info.GetNumSlotsAsLeaf() ? 0 : 1;
        default:
            return -1;
    }
    return -1;
}


ERROR_T BTreeIndex::SplitNode(const SIZE_T &node, SIZE_T &newNode, KEY_T &splitKey) {
    BTreeNode leftNode, rightNode;
    SIZE_T leftKeyNum, rightKeyNum;
    char *src, *dest;
    ERROR_T rc;

    leftNode.Unserialize(buffercache, node);
    rightNode = leftNode;
    if ((rc = AllocateNode(newNode))) return rc;
    if (leftNode.info.nodetype == BTREE_LEAF_NODE) {
        leftKeyNum = (leftNode.info.numkeys + 2) / 2;
        rightKeyNum = leftNode.info.numkeys - leftKeyNum;
        leftNode.GetKey(leftKeyNum - 1, splitKey);
        src = leftNode.ResolveKeyVal(leftKeyNum);
        dest = rightNode.ResolveKeyVal(0);
        memcpy(dest, src, rightKeyNum * (leftNode.info.keysize + leftNode.info.valuesize));
    } else {
        leftKeyNum = leftNode.info.numkeys / 2;
        rightKeyNum = leftNode.info.numkeys - leftKeyNum - 1;
        leftNode.GetKey(leftKeyNum, splitKey);
        src = leftNode.ResolvePtr(leftKeyNum + 1);
        dest = rightNode.ResolvePtr(0);
        memcpy(dest, src, rightKeyNum * (leftNode.info.keysize + sizeof(SIZE_T)) + sizeof(SIZE_T));
    }
    leftNode.info.numkeys = leftKeyNum;
    rightNode.info.numkeys = rightKeyNum;

    if ((rc = leftNode.Serialize(buffercache, node))) return rc;
    if ((rc = rightNode.Serialize(buffercache, newNode))) return rc;
    return ERROR_NOERROR;
}


ERROR_T BTreeIndex::AddKeyPtrVal(const SIZE_T node, const KEY_T &key, const VALUE_T &value, const SIZE_T &newNode) {
    BTreeNode b;
    KEY_T testkey;
    SIZE_T numkeys;
    SIZE_T offset;
    ERROR_T rc;
    char *src, *dest;

    b.Unserialize(buffercache, node);
    numkeys = b.info.numkeys;
    b.info.numkeys++;
    if (numkeys == 0) {
        if ((rc = b.SetKey(0, key))) return rc;
        if ((rc = b.SetVal(0, value))) return rc;
    } else {
        for (offset = 0; offset < numkeys; offset++) {
            if ((rc = b.GetKey(offset, testkey))) return rc;
            if (key < testkey) {
                src = b.ResolveKey(offset);
                dest = b.ResolveKey(offset + 1);
                if (b.info.nodetype == BTREE_LEAF_NODE) {
                    memmove(dest, src, (numkeys - offset) * (b.info.keysize + b.info.valuesize));
                    if ((rc = b.SetKey(offset, key))) return rc;
                    if ((rc = b.SetVal(offset, value))) return rc;
                } else {
                    memmove(dest, src, (numkeys - offset) * (b.info.keysize + sizeof(SIZE_T)));
                    if ((rc = b.SetKey(offset, key))) return rc;
                    if ((rc = b.SetPtr(offset + 1, newNode))) return rc;
                }
                break;
            }
            if (offset == numkeys - 1) {
                if (b.info.nodetype == BTREE_LEAF_NODE) {
                    if ((rc = b.SetKey(numkeys, key))) return rc;
                    if ((rc = b.SetVal(numkeys, value))) return rc;
                } else {
                    if ((rc = b.SetKey(numkeys, key))) return rc;
                    if ((rc = b.SetPtr(numkeys + 1, newNode))) return rc;
                }
                break;
            }
        }
    }
    return b.Serialize(buffercache, node);
}


ERROR_T BTreeIndex::InsertInternal(const SIZE_T &node, const KEY_T &key, const VALUE_T &value) {
    /*
     * In order to simplify the recursive function, here we define to split block when it is full.
     * Thus, there is minor difference between the model in textbook and ours.
     * */
    BTreeNode b;
    ERROR_T rc;
    SIZE_T offset;
    KEY_T testkey;
    SIZE_T ptr;
    SIZE_T newNode;
    KEY_T splitKey;

    b.Unserialize(buffercache, node);
    switch (b.info.nodetype) {
        case BTREE_ROOT_NODE:
        case BTREE_INTERIOR_NODE:
            for (offset = 0; offset < b.info.numkeys; offset++) {
                if ((rc = b.GetKey(offset, testkey))) return rc;
                if (key < testkey || key == testkey) {
                    if ((rc = b.GetPtr(offset, ptr))) return rc;
                    if ((rc = InsertInternal(ptr, key, value))) return rc;
                    if (!IsFull(ptr)) {
                        if ((rc = SplitNode(ptr, newNode, splitKey))) return rc;
                        return AddKeyPtrVal(node, splitKey, VALUE_T(), newNode);
                    }
                    return rc;
                }
            }
            if (b.info.numkeys > 0) {
                if ((rc = b.GetPtr(b.info.numkeys, ptr))) return rc;
                if ((rc = InsertInternal(ptr, key, value))) return rc;
                if (!IsFull(ptr)) {
                    if ((rc = SplitNode(ptr, newNode, splitKey))) return rc;
                    return AddKeyPtrVal(node, splitKey, VALUE_T(), newNode);
                }
                return rc;
            }
            return ERROR_INSANE;
        case BTREE_LEAF_NODE:
            return AddKeyPtrVal(node, key, value, 0);
        default:
            return ERROR_INSANE;
    }
}


ERROR_T BTreeIndex::Insert(const KEY_T &key, const VALUE_T &value) {
    VALUE_T v = value;
    if (Lookup(key, v) != ERROR_NONEXISTENT) {
        return ERROR_CONFLICT;
    }

    ERROR_T rc;
    BTreeNode b(BTREE_LEAF_NODE, superblock.info.keysize, superblock.info.valuesize, buffercache->GetBlockSize());
    BTreeNode rootNode;
    rootNode.Unserialize(buffercache, superblock.info.rootnode);

    if (rootNode.info.numkeys == 0) {
        SIZE_T leftNode, rightNode;
        if ((rc = AllocateNode(leftNode))) return rc;
        if ((rc = AllocateNode(rightNode))) return rc;
        b.Serialize(buffercache, leftNode);
        b.Serialize(buffercache, rightNode);
        rootNode.info.numkeys = 1;
        rootNode.SetKey(0, key);
        rootNode.SetPtr(0, leftNode);
        rootNode.SetPtr(1, rightNode);
        rootNode.Serialize(buffercache, superblock.info.rootnode);
    }

    SIZE_T oldRoot, newNode;
    KEY_T splitKey;

    oldRoot = superblock.info.rootnode;
    rc = InsertInternal(superblock.info.rootnode, key, value);
    if (!IsFull(superblock.info.rootnode)) {
        SplitNode(oldRoot, newNode, splitKey);
        b.Unserialize(buffercache, oldRoot);
        b.Serialize(buffercache, oldRoot);
        b.Unserialize(buffercache, newNode);
        b.Serialize(buffercache, newNode);
        if ((rc = AllocateNode(superblock.info.rootnode))) return rc;
        rootNode.info.numkeys = 1;
        rootNode.SetKey(0, splitKey);
        rootNode.SetPtr(0, oldRoot);
        rootNode.SetPtr(1, newNode);
        return rootNode.Serialize(buffercache, superblock.info.rootnode);
    }
    return rc;
}


ERROR_T BTreeIndex::Update(const KEY_T &key, const VALUE_T &value) {
    VALUE_T v(value);
    return LookupOrUpdateInternal(superblock.info.rootnode, BTREE_OP_UPDATE, key, v);
}


ERROR_T BTreeIndex::Delete(const KEY_T &key) {
    // This is optional extra credit
    //
    //
    return ERROR_UNIMPL;
}


//
//
// DEPTH first traversal
// DOT is Depth + DOT format
//

ERROR_T BTreeIndex::DisplayInternal(const SIZE_T &node, ostream &o, BTreeDisplayType display_type) const {
    KEY_T testkey;
    SIZE_T ptr;
    BTreeNode b;
    ERROR_T rc;
    SIZE_T offset;

    rc = b.Unserialize(buffercache, node);

    if (rc != ERROR_NOERROR) {
        return rc;
    }

    rc = PrintNode(o, node, b, display_type);

    if (rc) { return rc; }

    if (display_type == BTREE_DEPTH_DOT) {
        o << ";";
    }

    if (display_type != BTREE_SORTED_KEYVAL) {
        o << endl;
    }

    switch (b.info.nodetype) {
        case BTREE_ROOT_NODE:
        case BTREE_INTERIOR_NODE:
            if (b.info.numkeys > 0) {
                for (offset = 0; offset <= b.info.numkeys; offset++) {
                    rc = b.GetPtr(offset, ptr);
                    if (rc) { return rc; }
                    if (display_type == BTREE_DEPTH_DOT) {
                        o << node << " -> " << ptr << ";\n";
                    }
                    rc = DisplayInternal(ptr, o, display_type);
                    if (rc) { return rc; }
                }
            }
            return ERROR_NOERROR;
            break;
        case BTREE_LEAF_NODE:
            return ERROR_NOERROR;
            break;
        default:
            if (display_type == BTREE_DEPTH_DOT) {
            } else {
                o << "Unsupported Node Type " << b.info.nodetype;
            }
            return ERROR_INSANE;
    }

    return ERROR_NOERROR;
}


ERROR_T BTreeIndex::Display(ostream &o, BTreeDisplayType display_type) const {
    ERROR_T rc;
    if (display_type == BTREE_DEPTH_DOT) {
        o << "digraph tree { \n";
    }
    rc = DisplayInternal(superblock.info.rootnode, o, display_type);
    if (display_type == BTREE_DEPTH_DOT) {
        o << "}\n";
    }
    return ERROR_NOERROR;
}


ERROR_T BTreeIndex::SanityCheckInternal(const SIZE_T &node, const KEY_T &key, const SIZE_T &isLeft) const {
    BTreeNode b;
    SIZE_T offset;
    KEY_T preKey;
    KEY_T curKey;

    b.Unserialize(buffercache, node);

    for (offset = 0; offset < b.info.numkeys; offset++) {
        if (offset == 0) {
            assert(b.GetKey(offset, curKey) == ERROR_NOERROR);
            if (isLeft ? key < curKey : (curKey < key || curKey == key)) {
                return ERROR_INSANE;
            }
        } else {
            preKey = curKey;
            assert(b.GetKey(offset, curKey) == ERROR_NOERROR);
            if ((curKey < preKey) || (isLeft ? key < curKey : (curKey < key || curKey == key))) {
                return ERROR_INSANE;
            }
        }
        if (b.info.nodetype != BTREE_LEAF_NODE) {
            SIZE_T leftNode, rightNode;
            assert(b.GetPtr(offset, leftNode) == ERROR_NOERROR);
            assert(b.GetPtr(offset + 1, rightNode) == ERROR_NOERROR);
            if (SanityCheckInternal(leftNode, curKey, 1) || SanityCheckInternal(rightNode, curKey, 0)) {
                return ERROR_INSANE;
            }
        }
    }
    return ERROR_NOERROR;
}


ERROR_T BTreeIndex::SanityCheck() const {
    BTreeNode b;
    SIZE_T offset;
    KEY_T preKey;
    KEY_T curKey;

    b.Unserialize(buffercache, superblock.info.rootnode);
    for (offset = 0; offset < b.info.numkeys; offset++) {
        if (offset == 0) {
            assert(b.GetKey(offset, curKey) == ERROR_NOERROR);
        } else {
            preKey = curKey;
            assert(b.GetKey(offset, curKey) == ERROR_NOERROR);
            if (curKey < preKey) {
                return ERROR_INSANE;
            }
        }
        SIZE_T leftNode, rightNode;
        assert(b.GetPtr(offset, leftNode) == ERROR_NOERROR);
        assert(b.GetPtr(offset + 1, rightNode) == ERROR_NOERROR);
        if (SanityCheckInternal(leftNode, curKey, 1) || SanityCheckInternal(rightNode, curKey, 0)) {
            return ERROR_INSANE;
        }
    }
    return ERROR_NOERROR;
}


ostream &BTreeIndex::Print(ostream &os) const {
    Display(os, BTREE_DEPTH_DOT);
    return os;
}




