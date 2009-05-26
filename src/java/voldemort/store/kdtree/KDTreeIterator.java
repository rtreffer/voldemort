package voldemort.store.kdtree;

import java.util.List;

import voldemort.utils.ByteArray;
import voldemort.utils.ClosableIterator;
import voldemort.utils.Pair;
import voldemort.versioning.Versioned;

public class KDTreeIterator implements ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> {

    private KDNode<List<Versioned<byte[]>>> position;
    private Pair<ByteArray, Versioned<byte[]>> value;
    private List<Versioned<byte[]>> data;
    private KDNode<List<Versioned<byte[]>>> lastPosition;
    private Pair<ByteArray, Versioned<byte[]>> lastValue;
    private List<Versioned<byte[]>> lastData;
    private int pos;

    public KDTreeIterator(KDNode<List<Versioned<byte[]>>> root) {
        position = root;
    }

    @Override
    public void close() {
        position = null;
        value = null;
        data = null;
        lastPosition = null;
        lastValue = null;
        lastData = null;
    }

    @Override
    public boolean hasNext() {
        return value != null;
    }

    @Override
    public Pair<ByteArray, Versioned<byte[]>> next() {
        lastValue = value;
        lastData = data;
        lastPosition = position;
        scanNext(0);
        return lastValue;
    }

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
            return;
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
