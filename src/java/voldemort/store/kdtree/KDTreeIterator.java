package voldemort.store.kdtree;

import java.util.List;

import voldemort.utils.ByteArray;
import voldemort.utils.ClosableIterator;
import voldemort.utils.Pair;
import voldemort.versioning.Versioned;

public class KDTreeIterator implements ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> {

    private KDNode<List<Versioned<byte[]>>> position;
    private KDNode<List<Versioned<byte[]>>> lposition;
    private Pair<ByteArray, Versioned<byte[]>> lvalue;
    private Pair<ByteArray, Versioned<byte[]>> value;
    private List<Versioned<byte[]>> data;
    private List<Versioned<byte[]>> ldata;
    private int pos;

    public KDTreeIterator(KDNode<List<Versioned<byte[]>>> root) {
        position = root;
    }

    @Override
    public void close() {}

    @Override
    public boolean hasNext() {
        return value != null;
    }

    @Override
    public Pair<ByteArray, Versioned<byte[]>> next() {
        lvalue = value;
        ldata = data;
        lposition = position;
        scanNext(0);
        return lvalue;
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
        if(ldata == null) {
            return;
        }
        synchronized(ldata) {
            ldata.remove(lvalue.getSecond());
            if(ldata.size() > 0) {
                return;
            }
            synchronized(lposition) {
                if(lposition.getData() == ldata) {
                    lposition.remove();
                }
            }
        }
    }

}
