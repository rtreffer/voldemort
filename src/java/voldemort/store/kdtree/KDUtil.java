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

    public final static byte[] getBytesForKey(final double[] key) {
        byte bytes[] = new byte[key.length * 8];
        for(int i = 0; i < key.length; i++) {
            long l = Double.doubleToRawLongBits(i);
            bytes[i * 8] = (byte) (l >>> 56);
            bytes[i * 8 + 1] = (byte) (l >>> 48);
            bytes[i * 8 + 2] = (byte) (l >>> 40);
            bytes[i * 8 + 3] = (byte) (l >>> 32);
            bytes[i * 8 + 4] = (byte) (l >>> 24);
            bytes[i * 8 + 5] = (byte) (l >>> 16);
            bytes[i * 8 + 6] = (byte) (l >>> 8);
            bytes[i * 8 + 7] = (byte) (l);
        }
        return bytes;
    }
}
