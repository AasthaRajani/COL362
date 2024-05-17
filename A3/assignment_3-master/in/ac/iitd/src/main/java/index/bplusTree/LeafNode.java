package index.bplusTree;

/*
    * A LeafNode contains keys and block ids.
    * Looks Like -
    * # entries | prev leafnode | next leafnode | ptr to next free offset | blockid_1 | len(key_1) | key_1 ...
    *
    * Note: Only write code where specified!
 */
public class LeafNode<T> extends BlockNode implements TreeNode<T>{

    Class<T> typeClass;

    public LeafNode(Class<T> typeClass) {
        
        super();
        this.typeClass = typeClass;

        // set numEntries to 0
        byte[] numEntriesBytes = new byte[2];
        numEntriesBytes[0] = 0;
        numEntriesBytes[1] = 0;
        this.write_data(0, numEntriesBytes);

        // set ptr to next free offset to 8
        byte[] nextFreeOffsetBytes = new byte[2];
        nextFreeOffsetBytes[0] = 0;
        nextFreeOffsetBytes[1] = 8;
        this.write_data(6, nextFreeOffsetBytes);

        return;
    }

    // returns the keys in the node - will be evaluated
    @Override
    public T[] getKeys() {

        int numKeys = getNumKeys();
        T[] keys = (T[]) new Object[numKeys];

        /* Write your code here */
        int offset = 8;
        for (int i = 0; i < numKeys; i++) {
//            System.out.print("i is : " + i + "\n");
            offset += 2;
            byte[] key_len_bytes = this.get_data(offset, 2);
            int key_len = ((key_len_bytes[0] & 0xFF) << 8) | (key_len_bytes[1] & 0xFF);
            offset += 2;
            byte[] key_bytes = this.get_data(offset, key_len);
            keys[i] = convertBytesToT(key_bytes, this.typeClass);
//            System.out.print("Exited from the function\n");
            offset += key_len;
        }

        return keys;

    }

    // returns the block ids in the node - will be evaluated
    public int[] getBlockIds() {

        int numKeys = getNumKeys();

        int[] block_ids = new int[numKeys];

        /* Write your code here */
        int offset = 8;
        for (int i = 0; i < numKeys; i++) {
            byte[] block_id_bytes = this.get_data(offset, 2);
            int block_id = ((block_id_bytes[0] & 0xFF) << 8) | (block_id_bytes[1] & 0xFF);
            block_ids[i] = block_id;
            offset += 2;

            byte[] key_len_bytes = this.get_data(offset, 2);
            int key_len = ((key_len_bytes[0] & 0xFF) << 8) | (key_len_bytes[1] & 0xFF);
            offset += 2 + key_len;
        }
        return block_ids;
    }

    // can be used as helper function - won't be evaluated
    private int getOffset_for_newNode(int j) {
        int offset = 8;

        for (int i = 0; i < j; i++) {
            offset += 2;
            byte[] key_len_bytes = this.get_data(offset, 2);
            int key_len = ((key_len_bytes[0] & 0xFF) << 8) | (key_len_bytes[1] & 0xFF);
            offset += 2 + key_len;
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
    public void insert(T key, int block_id) {


        /* Write your code here */
        T[] keys = getKeys();
        int[] blockIDs = getBlockIds();
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
        byte[] blockId_bytes = new byte[2];
        blockId_bytes[0] = (byte) ((block_id >> 8) & 0xFF);
        blockId_bytes[1] = (byte) (block_id & 0xFF);
        this.write_data(offset, blockId_bytes);
        offset += 2;
        byte[] keylen_bytes = new byte[2];
        int key_l = key_bytes.length;
        keylen_bytes[0] = (byte) ((key_l >> 8) & 0xFF);
        keylen_bytes[1] = (byte) (key_l & 0xFF);
        this.write_data(offset, keylen_bytes);
        offset += 2;
        this.write_data(offset, key_bytes);
        offset += key_bytes.length;
        for (int j = i; j<num_keys ; j++) {
            byte[] block_id_bytes = new byte[2];
            block_id_bytes[0] = (byte) ((blockIDs[j] >> 8) & 0xFF);
            block_id_bytes[1] = (byte) (blockIDs[j] & 0xFF);
            this.write_data(offset, block_id_bytes);
            offset += 2;
            byte[] key_len_bytes = new byte[2];
            byte[] keyBytes = convertTtoBytes(keys[j]);
            int key_length = keyBytes.length;
            key_len_bytes[0] = (byte) ((key_length >> 8) & 0xFF);
            key_len_bytes[1] = (byte) (key_length & 0xFF);
            this.write_data(offset, key_len_bytes);
            offset += 2;
            this.write_data(offset, keyBytes);
            offset += key_length;
        }
        byte[] nextFreeOffsetBytes = new byte[2];
        nextFreeOffsetBytes[0] = (byte) ((offset >> 8) & 0xFF);
        nextFreeOffsetBytes[1] = (byte) (offset & 0xFF);
        this.write_data(6, nextFreeOffsetBytes);

        num_keys++;
        byte[] numEntriesBytes = new byte[2];
        numEntriesBytes[0] = (byte) ((num_keys >> 8) & 0xFF);
        numEntriesBytes[1] = (byte) (num_keys & 0xFF);
        this.write_data(0, numEntriesBytes);
        return;

    }

    // can be used as helper function - won't be evaluated
    @Override
    public int search(T key) {

        /* Write your code here */
        int num_keys = getNumKeys();
        int offset = 8;

        for (int i = 0; i<num_keys; i++) {
            byte[] block_id_bytes = this.get_data(offset, 2);
            int block_id = (block_id_bytes[0] << 8) | (block_id_bytes[1] & 0xFF);
            offset += 2;

            byte[] key_len_bytes = this.get_data(offset, 2);
            int key_len = (key_len_bytes[0] << 8) | (key_len_bytes[1] & 0xFF);
            offset += 2;

            byte[] key_bytes = this.get_data(offset, key_len);
            T key_search = convertBytesToT(key_bytes, this.typeClass);
            offset += key_len;

            if (key == key_search) {
                return block_id;
            }
        }
        return -1;
    }

}
