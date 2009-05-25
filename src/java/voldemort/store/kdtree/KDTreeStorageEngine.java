package voldemort.store.kdtree;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import voldemort.VoldemortException;
import voldemort.store.NoSuchCapabilityException;
import voldemort.store.StorageEngine;
import voldemort.store.StoreCapabilityType;
import voldemort.store.StoreUtils;
import voldemort.utils.ByteArray;
import voldemort.utils.ClosableIterator;
import voldemort.utils.Pair;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.Occured;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;
import cern.colt.Arrays;

/**
 * Inmemory store that will keep key/value pairs in KDTree.
 */
public class KDTreeStorageEngine implements StorageEngine<ByteArray, byte[]> {

    // Wish/todo list:
    //
    // 1. query interface
    // get nearby(d1,d2,d3) limit 1000
    // 2. integrate with rebalancing
    // keep log(n) lookup speed
    // 3. range / order by queries
    // "order by distance(field1, field2) asc, field3 desc limit 1000"
    // 4. Non-numeric keys (find abac...def, up to 1k elements)
    // field1 in 'abac'..'ebad' limit 1000

    /**
     * The name of this store
     */
    public static final String TYPE_NAME = "kdtree";

    /**
     * The root of the kdtree
     */
    private KDNode<List<Versioned<byte[]>>> root;

    /**
     * Instantiate a new store, based on the number of dimensions and the name.
     * 
     * @param name
     * @param dimensions
     */
    public KDTreeStorageEngine(String name, int dimensions) {
        this.name = name;
        root = new KDNode<List<Versioned<byte[]>>>(new double[dimensions]);
    }

    /**
     * The name of this store.
     */
    private final String name;

    @Override
    public ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> entries() {
        return new KDTreeIterator(root);
    }

    /**
     * Close this store.
     */
    @Override
    public void close() throws VoldemortException {
    // Nothing to do
    }

    @Override
    public boolean delete(ByteArray key, Version version) throws VoldemortException {
        StoreUtils.assertValidKey(key);
        KDNode<List<Versioned<byte[]>>> result = root.getNode(KDUtil.getDimensionsForKey(key.get()));
        if(result == null) {
            return false;
        }
        synchronized(result) {
            if(version == null) {
                final boolean success = result.getData() != null;
                result.remove();
                return success;
            }

            List<Versioned<byte[]>> values = result.getData();
            if(values == null) {
                return false;
            }

            boolean deletedSomething = false;
            synchronized(values) {
                Iterator<Versioned<byte[]>> iterator = values.iterator();
                while(iterator.hasNext()) {
                    Versioned<byte[]> item = iterator.next();
                    if(item.getVersion().compare(version) == Occured.BEFORE) {
                        iterator.remove();
                        deletedSomething = true;
                    }
                }
                if(values.size() == 0) {
                    result.remove();
                }
            }

            return deletedSomething;
        }
    }

    /**
     * Retrieve a version array for a given key.
     * 
     * @param key The key.
     * @return The list of versioned entries.
     */
    @Override
    public List<Versioned<byte[]>> get(ByteArray key) throws VoldemortException {
        StoreUtils.assertValidKey(key);
        KDNode<List<Versioned<byte[]>>> result = root.getNode(KDUtil.getDimensionsForKey(key.get()));
        if(result == null) {
            // NOT FOUND
            return new ArrayList<Versioned<byte[]>>(0);
        }
        List<Versioned<byte[]>> data = result.getData();
        if(data == null) {
            // Value deleted :S
            return new ArrayList<Versioned<byte[]>>(0);
        }
        // Got results :)
        return data;
    }

    @Override
    public Map<ByteArray, List<Versioned<byte[]>>> getAll(Iterable<ByteArray> keys)
            throws VoldemortException {
        StoreUtils.assertValidKeys(keys);
        Map<ByteArray, List<Versioned<byte[]>>> map = new HashMap<ByteArray, List<Versioned<byte[]>>>();
        for(ByteArray key: keys) {
            List<Versioned<byte[]>> value = get(key);
            if(value != null && value.size() > 0) {
                map.put(key, value);
            }
        }
        return map;
    }

    @Override
    public Object getCapability(StoreCapabilityType capability) {
        throw new NoSuchCapabilityException(capability, getName());
    }

    @Override
    public String getName() {
        return name;
    }

    /**
     * Write a new/newer value to the kd store.
     * 
     * @param key The key for the value
     * @param value The versioned value
     * @throws VoldemortException ObsoleteVersionException if a newer value was
     *         found
     */
    @Override
    public void put(ByteArray key, Versioned<byte[]> value) throws VoldemortException {
        StoreUtils.assertValidKey(key);
        final double dim[] = KDUtil.getDimensionsForKey(key.get());
        KDNode<List<Versioned<byte[]>>> result = root.getNode(dim);
        if(result == null) {
            synchronized(root) {
                result = root.getNode(dim);
                if(result == null) {
                    result = new KDNode<List<Versioned<byte[]>>>(dim);
                    result.setData(new ArrayList<Versioned<byte[]>>(2));
                    root.add(result);
                }
            }
        }
        final List<Versioned<byte[]>> data = result.getData();
        if(data == null) {
            synchronized(result) {
                result.setData(new ArrayList<Versioned<byte[]>>(2));
            }
        }
        synchronized(data) {
            final Version version = value.getVersion();
            List<Versioned<byte[]>> itemsToRemove = new ArrayList<Versioned<byte[]>>(data.size());
            for(Versioned<byte[]> versioned: data) {
                Occured occured = version.compare(versioned.getVersion());
                if(occured == Occured.BEFORE) {
                    throw new ObsoleteVersionException("Obsolete version for key '"
                                                       + Arrays.toString(dim) + "': "
                                                       + value.getVersion());
                } else if(occured == Occured.AFTER) {
                    itemsToRemove.add(versioned);
                }
            }
            data.removeAll(itemsToRemove);
            data.add(value);
        }

    }
}
