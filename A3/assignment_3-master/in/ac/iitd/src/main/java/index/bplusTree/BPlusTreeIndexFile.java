package index.bplusTree;

import storage.AbstractFile;

import java.util.Queue;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Stack;

/*
    * Tree is a collection of BlockNodes
    * The first BlockNode is the metadata block - stores the order and the block_id of the root node

    * The total number of keys in all leaf nodes is the total number of records in the records file.
*/

public class BPlusTreeIndexFile<T> extends AbstractFile<BlockNode> {

    Class<T> typeClass;

    // Constructor - creates the metadata block and the root node
    public BPlusTreeIndexFile(int order, Class<T> typeClass) {
        
        super();
        this.typeClass = typeClass;
        BlockNode node = new BlockNode(); // the metadata block
        LeafNode<T> root = new LeafNode<>(typeClass);

        // 1st 2 bytes in metadata block is order
        byte[] orderBytes = new byte[2];
        orderBytes[0] = (byte) (order >> 8);
        orderBytes[1] = (byte) order;
        node.write_data(0, orderBytes);

        // next 2 bytes are for root_node_id, here 1
        byte[] rootNodeIdBytes = new byte[2];
        rootNodeIdBytes[0] = 0;
        rootNodeIdBytes[1] = 1;
        node.write_data(2, rootNodeIdBytes);

        // push these nodes to the blocks list
        blocks.add(node);
        blocks.add(root);
    }

    private boolean isFull(int id){
        // 0th block is metadata block
        assert(id > 0);
        return blocks.get(id).getNumKeys() == getOrder() - 1;
    }

    private int getRootId() {
        BlockNode node = blocks.get(0);
        byte[] rootBlockIdBytes = node.get_data(2, 2);
        return (rootBlockIdBytes[0] << 8) | (rootBlockIdBytes[1] & 0xFF);
    }

    public int getOrder() {
        BlockNode node = blocks.get(0);
        byte[] orderBytes = node.get_data(0, 2);
        return (orderBytes[0] << 8) | (orderBytes[1] & 0xFF);
    }

    private boolean isLeaf(BlockNode node){
        return node instanceof LeafNode;
    }

    private boolean isLeaf(int id){
        return isLeaf(blocks.get(id));
    }
    private boolean compare_greaterThan(T k1, T k2){
        if (k1 instanceof Integer && k2 instanceof Integer) {
            int x = (Integer) k1;
            int y = (Integer) k2;
//            System.out.println("x: " + x + " y: " + y);
            return (Integer) k1 > (Integer) k2;
        } else if (k1 instanceof Boolean && k2 instanceof Boolean) {
            return ((Boolean) k1 ? 1 : 0) > ((Boolean) k2 ? 1 : 0);
        } else if (k1 instanceof Float && k2 instanceof Float) {
            return (Float) k1 > (Float) k2;
        } else if (k1 instanceof String && k2 instanceof String) {
            return ((String) k1).compareTo((String) k2) > 0;
        } else if (k1 instanceof Double && k2 instanceof Double) {
            return (Double) k1 > (Double) k2;
        } else {
            return false;
        }
    }

    private boolean compare_equal(T k1, T k2){
        if (k1 instanceof Integer && k2 instanceof Integer) {
            return ((Integer) k1).equals((Integer) k2);
        } else if (k1 instanceof Boolean && k2 instanceof Boolean) {
            return ((Boolean) k1 ? 1 : 0) == ((Boolean) k2 ? 1 : 0);
        } else if (k1 instanceof Float && k2 instanceof Float) {
            return ((Float) k1).equals((Float) k2);
        } else if (k1 instanceof String && k2 instanceof String) {
            return ((String) k1).compareTo((String) k2) == 0;
        } else if (k1 instanceof Double && k2 instanceof Double) {
            return ((Double) k1).equals((Double) k2);
        } else {
            return false;
        }
    }
    // will be evaluated
    public void insert(T key, int block_id) {

        /* Write your code here */
        if (this != null) {
            if(key==null){
                return;
            }
            int root_id = getRootId();
            if (isLeaf(root_id)) {
                LeafNode<T> root_node = (LeafNode<T>) blocks.get(root_id);
                if (isFull(root_id)) {
                    int order = getOrder();
                    T[] keys = root_node.getKeys();
                    int[] block_ids = root_node.getBlockIds();
                    int i = 0;
                    while(i < keys.length) {
                        if (compare_equal(key, keys[i])) {
                            break;
                        } else if (compare_greaterThan(key, keys[i])) {
                            i++;
                        } else {
                            break;
                        }
                    }
                    T[] updated_keys = (T[]) new Object[order];
                    int[] updated_blockIds = new int[order];
                    int j = 0;
                    while(j < i) {
                        updated_keys[j] = keys[j];
                        updated_blockIds[j] = block_ids[j];
                        j++;
                    }
                    updated_keys[i] = key;
                    updated_blockIds[i] = block_id;
                    j++;
                    while(j < order) {
                        updated_keys[j] = keys[j-1];
                        updated_blockIds[j] = block_ids[j-1];
                        j++;
                    }
                    byte[] nextFreeOffsetBytes = new byte[2];
                    nextFreeOffsetBytes[0] = 0;
                    nextFreeOffsetBytes[1] = 8;
                    root_node.write_data(6, nextFreeOffsetBytes);

                    byte[] num_keys0 = new byte[2];
                    num_keys0[0] = (byte) (0);
                    num_keys0[1] = (byte) (0);
                    root_node.write_data(0, num_keys0);
                    int k = 0;
                    while (k < order/2) {
                        root_node.insert(updated_keys[k], updated_blockIds[k]);
                        k++;
                    }
                    T root_key = updated_keys[k];
                    LeafNode<T> right_leaf_node = new LeafNode<>(this.typeClass);
                    while (k < order) {
                        right_leaf_node.insert(updated_keys[k], updated_blockIds[k]);
                        k++;
                    }
                    blocks.add(right_leaf_node);
                    blocks.set(blocks.size()-1, right_leaf_node);
                    int right_child_id = blocks.size()-1;
                    InternalNode<T> new_root_node = new InternalNode<>(root_key, root_id, right_child_id, this.typeClass);
                    blocks.add(new_root_node);
                    blocks.set(blocks.size()-1, new_root_node);
                    BlockNode block0 = blocks.get(0);
                    byte[] root_block_id = new byte[2];
                    root_block_id[0] = (byte) (((blocks.size()-1) >> 8) & 0xFF);
                    root_block_id[1] = (byte) ((blocks.size()-1) & 0xFF);
                    block0.write_data(2, root_block_id);

                }
                else {
                    root_node.insert(key, block_id);
                }
            }
            else {
                int id = getRootId();
                Stack<Integer> stack = new Stack<>();
                while (isLeaf(id) == false) {
                    InternalNode<T> internal_node = (InternalNode<T>) blocks.get(id);
                    stack.push(id);
                    T[] keys = internal_node.getKeys();
                    int[] children = internal_node.getChildren();
                    int i = 0;
                    while (i < keys.length) {
                        if (compare_equal(key, keys[i])) {
                            break;
                        } else if (compare_greaterThan(key, keys[i])) {
                            i++;
                        } else {
                            break;
                        }
                    }
                    id = children[i];
                }
                LeafNode<T> leaf_node = (LeafNode<T>) blocks.get(id);
                if (isFull(id)) {
                    int order = getOrder();
                    T[] keys = leaf_node.getKeys();
                    int[] block_ids = leaf_node.getBlockIds();

                    int i = 0;
                    while (i < keys.length) {
                        if (compare_equal(key, keys[i])) {
                            break;
                        } else if (compare_greaterThan(key, keys[i])) {
                            i++;
                        } else {
                            break;
                        }
                    }
                    T[] updated_keys = (T[]) new Object[order];
                    int[] updated_block_ids = new int[order];

                    int j = 0;
                    while (j < i) {
                        updated_keys[j] = keys[j];
                        updated_block_ids[j] = block_ids[j];
                        j++;
                    }
                    updated_keys[i] = key;
                    updated_block_ids[i] = block_id;
                    j++;

                    while (j < order) {
                        updated_keys[j] = keys[j - 1];
                        updated_block_ids[j] = block_ids[j - 1];
                        j++;
                    }
                    byte[] nextFreeOffsetBytes = new byte[2];
                    nextFreeOffsetBytes[0] = 0;
                    nextFreeOffsetBytes[1] = 8;
                    leaf_node.write_data(6, nextFreeOffsetBytes);

                    byte[] num_keys1 = new byte[2];
                    num_keys1[0] = (byte) (0);
                    num_keys1[1] = (byte) (0);
                    leaf_node.write_data(0, num_keys1);
                    int k = 0;
                    while (k < order / 2) {
                        leaf_node.insert(updated_keys[k], block_ids[k]);
                        k++;
                    }
                    LeafNode<T> right_leaf_node = new LeafNode<>(this.typeClass);
                    T root_key = updated_keys[k];
                    while (k < order) {
                        right_leaf_node.insert(updated_keys[k], updated_block_ids[k]);
                        k++;
                    }
                    blocks.add(right_leaf_node);
                    blocks.set(blocks.size() - 1, right_leaf_node);
                    int right_child_id = blocks.size() - 1;
                    while (stack.isEmpty() == false) {
                        int inode_id = stack.pop();
                        InternalNode<T> inode = (InternalNode<T>) blocks.get(inode_id);
                        if (isFull(inode_id)) {
                            T[] inode_keys = inode.getKeys();
                            int[] inode_children = inode.getChildren();
                            int ii = 0;
                            while (ii < inode_keys.length) {
                                if (compare_equal(root_key, inode_keys[ii])) {
                                    break;
                                } else if (compare_greaterThan(root_key, inode_keys[ii])) {
                                    ii++;
                                } else {
                                    break;
                                }
                            }
                            T[] inode_updated_keys = (T[]) new Object[order];
                            int[] inode_updated_children = new int[order + 1];
                            int jj = 0;
                            inode_updated_children[jj] = inode_children[0];
                            while (jj < ii) {
                                inode_updated_keys[jj] = inode_keys[jj];
                                inode_updated_children[jj + 1] = inode_children[jj + 1];
                                jj++;
                            }
                            inode_updated_keys[jj] = root_key;
                            inode_updated_children[jj + 1] = right_child_id;
                            jj++;

                            while (jj < order) {
                                inode_updated_keys[jj] = inode_keys[jj - 1];
                                inode_updated_children[jj + 1] = inode_children[jj];
                                jj++;
                            }
                            byte[] next_free_offset_bytes = new byte[2];
                            next_free_offset_bytes[0] = 0;
                            next_free_offset_bytes[1] = 6;
                            inode.write_data(2, next_free_offset_bytes);

                            byte[] num_keys2 = new byte[2];
                            num_keys2[0] = (byte) (0);
                            num_keys2[1] = (byte) (0);
                            inode.write_data(0, num_keys2);

                            byte[] child0 = new byte[2];
                            child0[0] = (byte) ((inode_updated_children[0] >> 8) & 0xFF);
                            child0[1] = (byte) (inode_updated_children[0] & 0xFF);
                            inode.write_data(4, child0);
                            int kk = 0;
                            while (kk < order / 2) {
                                inode.insert(inode_updated_keys[kk], inode_updated_children[kk + 1]);
                                kk++;
                            }
                            root_key = inode_updated_keys[kk];
                            kk++;
                            InternalNode<T> right_inode = new InternalNode<>(inode_updated_keys[kk], inode_updated_children[kk], inode_updated_children[kk + 1], this.typeClass);
                            kk++;
                            while (kk < order) {
                                right_inode.insert(inode_updated_keys[kk], inode_updated_children[kk + 1]);
                                kk++;
                            }
                            blocks.add(right_inode);
                            blocks.set(blocks.size() - 1, right_inode);
                            right_child_id = blocks.size() - 1;
                            int[] right_children_ids = right_inode.getChildren();
                            if (stack.isEmpty()) {
                                InternalNode<T> new_root = new InternalNode<>(root_key, root_id, right_child_id, this.typeClass);
                                blocks.add(new_root);
                                blocks.set(blocks.size() - 1, new_root);
                                BlockNode root_node = blocks.get(0);
                                byte[] root_block_id = new byte[2];
                                root_block_id[0] = (byte) (((blocks.size() - 1) >> 8) & 0xFF);
                                root_block_id[1] = (byte) ((blocks.size() - 1) & 0xFF);
                                root_node.write_data(2, root_block_id);

                            }
                        } else {
                            inode.insert(root_key, right_child_id);
                            break;
                        }
                    }
                } else {
                    leaf_node.insert(key, block_id);
                }
            }
        }
        return;
    }

    // will be evaluated
    // returns the block_id of the leftmost leaf node containing the key
    public int search(T key) {

        /* Write your code here */
        if(this != null) {
            if(key==null){
                return -1;
            }
            int id = getRootId();
            boolean flag = true;
            while (!isLeaf(id)) {
                InternalNode<T> internal_node = (InternalNode<T>) blocks.get(id);
                T[] keys = internal_node.getKeys();
                int[] children = internal_node.getChildren();
                int i = 0;
                while(i < keys.length) {
                    if (compare_equal(key, keys[i])) {
                        break;
                    } else if(compare_greaterThan(key, keys[i])) {
                        i++;
                    } else {
                        break;
                    }
                }
                if(i!=keys.length){
                    flag=false;
                }

                id = children[i];
            }
            LeafNode<T> leaf_node = (LeafNode<T>) blocks.get(id);
            T[] k_l = leaf_node.getKeys();
            if(flag==true && compare_greaterThan(key,k_l[k_l.length -1])){
                return -1;
            }
            else{
                return id;
            }
        }
        return -1;
    }

    // returns true if the key was found and deleted, false otherwise
    // (Optional for Assignment 3)
    public boolean delete(T key) {

        /* Write your code here */
        return false;
    }

    // DO NOT CHANGE THIS - will be used for evaluation
    public void print_bfs() {
        int root = getRootId();
        Queue<Integer> queue = new LinkedList<>();
        queue.add(root);
        while(!queue.isEmpty()) {
            int id = queue.remove();
            if(isLeaf(id)) {
                ((LeafNode<T>) blocks.get(id)).print();
            }
            else {
                ((InternalNode<T>) blocks.get(id)).print();
                int[] children = ((InternalNode<T>) blocks.get(id)).getChildren();
                for(int i = 0; i < children.length; i++) {
                    queue.add(children[i]);
                }
            }
        }
        return;
    }

    // DO NOT CHANGE THIS - will be used for evaluation
    public ArrayList<T> return_bfs() {
        int root = getRootId();
        Queue<Integer> queue = new LinkedList<>();
        ArrayList<T> bfs = new ArrayList<>();
        queue.add(root);
        while(!queue.isEmpty()) {
            int id = queue.remove();
            if(isLeaf(id)) {
                T[] keys = ((LeafNode<T>) blocks.get(id)).getKeys();
                for(int i = 0; i < keys.length; i++) {
                    bfs.add((T) keys[i]);
                }
            }
            else {
                T[] keys = ((InternalNode<T>) blocks.get(id)).getKeys();
                for(int i = 0; i < keys.length; i++) {
                    bfs.add((T) keys[i]);
                }
                int[] children = ((InternalNode<T>) blocks.get(id)).getChildren();
                for(int i = 0; i < children.length; i++) {
                    queue.add(children[i]);
                }
            }
        }
        return bfs;
    }

    public void print() {
        print_bfs();
        return;
    }

}