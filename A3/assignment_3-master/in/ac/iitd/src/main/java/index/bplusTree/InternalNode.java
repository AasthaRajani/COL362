package index.bplusTree;

/*
    * Internal Node - num Keys | ptr to next free offset | P_1 | len(K_1) | K_1 | P_2 | len(K_2) | K_2 | ... | P_n
    * Only write code where specified

    * Remember that each Node is a block in the Index file, thus, P_i is the block_id of the child node
 */
public class InternalNode<T> extends BlockNode implements TreeNode<T> {

    // Class of the key
    Class<T> typeClass;

    // Constructor - expects the key, left and right child ids
    public InternalNode(T key, int left_child_id, int right_child_id, Class<T> typeClass) {

        super();
        this.typeClass = typeClass;

        byte[] numKeysBytes = new byte[2];
        numKeysBytes[0] = 0;
        numKeysBytes[1] = 0;

        this.write_data(0, numKeysBytes);

        byte[] child_1 = new byte[2];
        child_1[0] = (byte) ((left_child_id >> 8) & 0xFF);
        child_1[1] = (byte) (left_child_id & 0xFF);

        this.write_data(4, child_1);

        byte[] nextFreeOffsetBytes = new byte[2];
        nextFreeOffsetBytes[0] = 0;
        nextFreeOffsetBytes[1] = 6;

        this.write_data(2, nextFreeOffsetBytes);

        // also calls the insert method
        this.insert(key, right_child_id);
        return;
    }

    // returns the keys in the node - will be evaluated
    @Override
    public T[] getKeys() {

        int numKeys = getNumKeys();
        T[] keys = (T[]) new Object[numKeys];

        /* Write your code here */
        int offset = 4;
        for (int i = 0; i<numKeys; i++) {
            offset += 2;
            byte[] key_len_bytes = this.get_data(offset, 2);
            int key_len = ((key_len_bytes[0] & 0xFF) << 8) | (key_len_bytes[1] & 0xFF);
            offset += 2;

            byte[] key_bytes = this.get_data(offset, key_len);
            keys[i] = convertBytesToT(key_bytes, this.typeClass);
            offset += key_len;
        }

        return keys;
    }

    // can be used as helper function - won't be evaluated
    private int getOffset_for_newNode(int j) {
        int offset = 6;

        for (int i = 0; i < j; i++) {
            byte[] key_len_bytes = this.get_data(offset, 2);
            int key_len = ((key_len_bytes[0] & 0xFF) << 8) | (key_len_bytes[1] & 0xFF);
            offset += 2 + key_len + 2;
        }
        return offset;
    }
    private boolean compare_greaterThan(T k1, T k2){
        if (k1 instanceof Integer && k2 instanceof Integer) {
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
    @Override
    public void insert(T key, int right_block_id) {
        /* Write your code here */
        T[] keys = getKeys();
        int[] children = getChildren();
        byte[] key_bytes = convertTtoBytes(key);
        int num_keys = getNumKeys();
        int i = 0;
        while (i < num_keys) {
            if (compare_equal(key, keys[i])) {
//                break;
                i++;
            } else if (compare_greaterThan(key, keys[i])) {
                i++;
            } else {
                break;
            }
        }
        int offset = getOffset_for_newNode(i);
        byte[] keylen_bytes = new byte[2];
        keylen_bytes[0] = (byte) ((key_bytes.length >> 8) & 0xFF);
        keylen_bytes[1] = (byte) (key_bytes.length & 0xFF);
        this.write_data(offset, keylen_bytes);
        offset += 2;

        this.write_data(offset, key_bytes);
        offset += key_bytes.length;
        byte[] right_childId_bytes = new byte[2];
        right_childId_bytes[0] = (byte) ((right_block_id >> 8) & 0xFF);
        right_childId_bytes[1] = (byte) (right_block_id & 0xFF);
        this.write_data(offset, right_childId_bytes);
        offset += 2;
        for (int j = i; j < num_keys; j++) {
            byte[] key_len_bytes = new byte[2];
//            byte[] keyBytes = keys[j].toString().getBytes();
            byte[] keyBytes = convertTtoBytes(keys[j]);
            int key_length = keyBytes.length;
            key_len_bytes[0] = (byte) ((key_length >> 8) & 0xFF);
            key_len_bytes[1] = (byte) (key_length & 0xFF);
            this.write_data(offset, key_len_bytes);
            offset += 2;

            this.write_data(offset, keyBytes);
            offset += key_length;

            byte[] child_id_bytes = new byte[2];
            child_id_bytes[0] = (byte) (children[j+1] >> 8 & 0xFF);
            child_id_bytes[1] = (byte) (children[j+1] & 0xFF);
            this.write_data(offset, child_id_bytes);
            offset += 2;
        }
        byte[] nextFreeOffsetBytes = new byte[2];
        nextFreeOffsetBytes[0] = (byte) ((offset >> 8) & 0xFF);
        nextFreeOffsetBytes[1] = (byte) (offset & 0xFF);
        this.write_data(2, nextFreeOffsetBytes);

        num_keys++;
        byte[] numEntriesBytes = new byte[2];
        numEntriesBytes[0] = (byte) ((num_keys >> 8) & 0xFF);
        numEntriesBytes[1] = (byte) (num_keys & 0xFF);
        this.write_data(0, numEntriesBytes);

    }

    // can be used as helper function - won't be evaluated
    @Override
    public int search(T key) {
        /* Write your code here */
        int num_keys = getNumKeys();
        T[] keys = getKeys();

        int i = 0;
        while (i < num_keys) {
            if (compare_equal(key, keys[i])) {
                return i;
            } else if (compare_greaterThan(key, keys[i])) {
                i++;
            } else {
                return -1;
            }
        }
        return -1;
    }

    // should return the block_ids of the children - will be evaluated
    public int[] getChildren() {

        byte[] numKeysBytes = this.get_data(0, 2);
        int numKeys = (numKeysBytes[0] << 8) | (numKeysBytes[1] & 0xFF);

        int[] children = new int[numKeys + 1];

        /* Write your code here */
        int offset = 4;
        for(int i=0;i<=numKeys;i++){
            byte[] child_id_bytes = this.get_data(offset, 2);
            children[i] = ((child_id_bytes[0] & 0xFF) << 8) | (child_id_bytes[1] & 0xFF);
            offset += 2;

            byte[] key_len_bytes = this.get_data(offset, 2);
            int key_len = ((key_len_bytes[0] & 0xFF) << 8) | (key_len_bytes[1] & 0xFF);
            offset += 2+key_len;
        }

        return children;

    }

}
