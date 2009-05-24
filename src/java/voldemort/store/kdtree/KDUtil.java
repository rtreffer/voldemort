package voldemort.store.kdtree;


public class KDUtil {

    /**
     * Decode a byte array key into an double array.
     * 
     * @param key The key to decode.
     * @return The position array of the key.
     */
    public final static double[] getDimensionsForKey(final byte[] key) {
        double dim[] = new double[key.length / 8];
        for(int i = 0; i < key.length; i += 8) {
            long k = 0;
            for(int j = 0; j < 8; j++) {
                k = (k << 8) + (0xFFl & key[i + j]);
            }
            dim[i / 8] = Double.longBitsToDouble(k);
        }
        return dim;
    }

}
