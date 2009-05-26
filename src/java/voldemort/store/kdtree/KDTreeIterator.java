package voldemort.store.kdtree;

import java.util.List;

import voldemort.utils.ByteArray;
import voldemort.utils.ClosableIterator;
import voldemort.utils.Pair;
import voldemort.versioning.Versioned;

/**
 * Iterate over a kd tree.
 */
public class KDTreeIterator implements ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> {

    /**
     * The current position (kd node).
     */
    private KDNode<List<Versioned<byte[]>>> position;
    /**
     * The current value (returned on next().
     */
    private Pair<ByteArray, Versioned<byte[]>> value;
    /**
     * The data object associated with the current value.
     */
    private List<Versioned<byte[]>> data;
    /**
     * The position of the next pair value in data.
     */
    private int pos;
    /**
     * The position used for the last next() call.
     */
    private KDNode<List<Versioned<byte[]>>> lastPosition;
    /**
     * The value used for the last next() call.
     */
    private Pair<ByteArray, Versioned<byte[]>> lastValue;
    /**
     * The data used for the last next() call.
     */
    private List<Versioned<byte[]>> lastData;

    /**
     * Create a new Iterator for a given tree.
     * 
     * @param root The tree root.
     */
    public KDTreeIterator(KDNode<List<Versioned<byte[]>>> root) {
        if(root.getParent() != null) {
            throw new IllegalArgumentException("KDTreeIterator needs a tree root");
        }
        position = root;
    }

    /**
     * Close the current iterator, releases all pointers.
     */
    @Override
    public void close() {
        position = null;
        value = null;
        data = null;
        lastPosition = null;
        lastValue = null;
        lastData = null;
    }

    /**
     * Ask for the existence of a next element.
     * 
     * @return true if there is at least one more element.
     */
    @Override
    public boolean hasNext() {
        return value != null;
    }

    /**
     * Get the next element. Note that this method will immediately start to
     * search for another next element and cache it.
     * 
     * @return The next key/version&lt;value&gt; pair
     */
    @Override
    public Pair<ByteArray, Versioned<byte[]>> next() {
        lastValue = value;
        lastData = data;
        lastPosition = position;
        scanNext(0);
        return lastValue;
    }

    /**
     * Search for the next element in the tree.
     * 
     * @param startpos The start position within the child array.
     */
    private void scanNext(final int startpos) {
        data = position.getData();
        if(data != null) {
            synchronized(data) {
                if(pos < data.size()) {
                    value = Pair.create(new ByteArray(KDUtil.getBytesForKey(position.getKDVector()
                                                                                    .getDimensionArray())),
                                        data.get(pos++));
                    return;
                }
            }
        }
        pos = 0;
        for(int i = startpos; i < position.getMaxChildCount(); i++) {
            KDNode<List<Versioned<byte[]>>> child = position.getChildAt(i);
            if(child == null) {
                continue;
            }
            position = child;
            scanNext(0);
            if(value != lastValue) {
                return;
            }
        }
        if(position.getParent() == null) {
            position = null;
            data = null;
            value = null;
            return;
        }
        int lastpos = position.getParent()
                              .getKDVector()
                              .binaryDeltaDirection(position.getKDVector());
        position = position.getParent();
        scanNext(lastpos + 1);
    }

    /**
     * Remove the last returned element.
     */
    @Override
    public void remove() {
        if(lastData == null) {
            return;
        }
        synchronized(lastData) {
            lastData.remove(lastValue.getSecond());
            if(lastData.size() > 0) {
                return;
            }
            synchronized(lastPosition) {
                if(lastPosition.getData() == lastData) {
                    lastPosition.remove();
                }
            }
        }
    }

}
