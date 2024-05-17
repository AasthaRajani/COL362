package index.bplusTree;

// TreeNode interface - will be implemented by InternalNode and LeafNode
public interface TreeNode <T> {

    public T[] getKeys();
    public void insert(T key, int block_id);

    public int search(T key);

    // DO NOT modify this - may be used for evaluation
    default public void print() {
        T[] keys = getKeys();
        for (T key : keys) {
            System.out.print(key + " ");
        }
        return;
    }
    
    // Might be useful for you - will not be evaluated
    default public T convertBytesToT(byte[] bytes, Class<T> typeClass){
        
        /* Write your code here */
        if (bytes == null || bytes.length == 0) {
            return null;
        }
        int intBits;
        if (typeClass == String.class) {
            return typeClass.cast(new String(bytes));
        } else if (typeClass == Integer.class) {
//            System.out.println("Converting bytes to Int: key_len: " + bytes.length + "\n");
            intBits = ((bytes[0] & 0xFF) << 24) | ((bytes[1] & 0xFF) << 16) | ((bytes[2] & 0xFF) << 8) | (bytes[3] & 0xFF);

            return typeClass.cast(intBits);
        } else if (typeClass == Boolean.class) {
            boolean val = bytes[0] != 0;
            return typeClass.cast(val);
        } else if (typeClass == Float.class) {
            intBits = ((bytes[0] & 0xFF) << 24) | ((bytes[1] & 0xFF) << 16) | ((bytes[2] & 0xFF) << 8) | (bytes[3] & 0xFF);
            float val = Float.intBitsToFloat(intBits);
            return typeClass.cast(val);
        } else if (typeClass == Double.class) {
            long longBits = ((bytes[0] & 0xFFL) << 56) | ((bytes[1] & 0xFFL) << 48) | ((bytes[2] & 0xFFL) << 40) |
                    ((bytes[3] & 0xFFL) << 32) | ((bytes[4] & 0xFFL) << 24) | ((bytes[5] & 0xFFL) << 16) |
                    ((bytes[6] & 0xFFL) << 8) | (bytes[7] & 0xFFL);
            double val = Double.longBitsToDouble(longBits);
            return typeClass.cast(val);
        } else {
            throw new IllegalArgumentException("Unsupported type for conversion");
        }
    }
    default public byte[] convertTtoBytes(T value) {
        if (value == null) {
            return null;
        }

        if (value instanceof Integer) {
            int intValue = (Integer) value;
            byte[] bytes = new byte[4];
            for (int i = 0; i < 4; i++) {
                bytes[i] = (byte) (intValue >> (8 * (3 - i)));
            }
            return bytes;
        } else if (value instanceof String) {
            return ((String) value).getBytes();
        } else if (value instanceof Boolean) {
            byte boolByte = (byte) (((Boolean) value) ? 1 : 0);
            return new byte[]{boolByte};
        } else if (value instanceof Float) {
            int floatValue = Float.floatToIntBits((Float) value);
            byte[] bytes = new byte[4];
            for (int i = 0; i < 4; i++) {
                bytes[i] = (byte) (floatValue >> (8 * (3 - i)));
            }
            return bytes;
        } else if (value instanceof Double) {
            long doubleValue = Double.doubleToLongBits((Double) value);
            byte[] bytes = new byte[8];
            for (int i = 0; i < 8; i++) {
                bytes[i] = (byte) (doubleValue >> (8 * (7 - i)));
            }
            return bytes;
        } else {
            throw new IllegalArgumentException("Unsupported type for conversion");
        }
    }


}