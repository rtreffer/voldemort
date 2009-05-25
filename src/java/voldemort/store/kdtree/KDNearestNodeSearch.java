package voldemort.store.kdtree;

import java.util.Comparator;
import java.util.TreeSet;

/**
 * Find the nearest nodes. Nearest is relative to a reference vector.
 */
public class KDNearestNodeSearch {

    /**
     * A cache for (dimension,deltavector) -&gt; node ordering.
     */
    private final static int[][][] DIMENSION_MAP;

    /**
     * Initialize the dimension map.
     */
    static {
        DIMENSION_MAP = new int[10][][];
        for(int dim = 0; dim < 10; dim++) {
            DIMENSION_MAP[dim] = new int[1 << dim][];
        }
    }

    /**
     * Find an order for visiting child nodes, based on the chance to hit a low
     * delta match.
     * 
     * @param dim The dimension we'll search.
     * @param delta The binary delta or child position of the best match.
     * @return An 2^dim element array containing [0..2^dim-1] well sorted.
     */
    private final static int[] getChildOrdering(int dim, int delta) {
        int[] result = DIMENSION_MAP[dim][delta];
        if(result != null) {
            return result;
        }
        synchronized(DIMENSION_MAP) {
            result = new int[1 << dim];
            int pos = 0;
            for(int i = 0; i <= dim; i++) {
                for(int j = 0; j < result.length; j++) {
                    if(bitcount(delta ^ j) == i) {
                        result[pos++] = j;
                    }
                }
            }
            DIMENSION_MAP[dim][delta] = result;
        }
        return result;
    }

    /**
     * The root node.
     */
    private KDNode<?> node;
    /**
     * The intial maximum distance.
     */
    private double distance;
    /**
     * The maximum number of elements.
     */
    private final int count;
    /**
     * The requested position.
     */
    private final KDVector pos;
    /**
     * The result of this search.
     */
    protected final TreeSet<KDNode<?>> result;

    protected final Comparator<KDNode<?>> comparator;

    /**
     * Create a new search object, bound to a root node and a destination
     * position, limited by a maximum distance and a maximum result count.
     * 
     * @param root The root of the search tree.
     * @param pos The destination position.
     * @param idistance The maximum distance.
     * @param icount The maximum number of result nodes.
     */
    public KDNearestNodeSearch(KDNode<?> root, KDVector pos, double idistance, int icount) {
        node = root;
        this.pos = pos;
        distance = idistance;
        count = icount;
        comparator = new KDDistanceComparator(pos);
        result = new TreeSet<KDNode<?>>(comparator);
    }

    /**
     * Perform the actual search.
     * 
     * @return The nearest results, sorted by distance.
     */
    public KDNode<?>[] search() {
        search(node, pos.getDimensionArray());
        return result.toArray(new KDNode<?>[result.size()]);
    }

    protected void add(KDNode<?> node) {
        result.add(node);
        if(result.size() > count) {
            result.remove(result.last());
            distance = result.last().getKDVector().squaredDistance(pos);
        }
    }

    /**
     * Internal search recursion. The method takes the current node and a
     * mimimum guaranteed distance vector.
     * 
     * @param ref
     * @param deltav
     */
    private void search(final KDNode<?> ref, final double deltav[]) {

        final double nodedistance = pos.squaredDistance(ref.getKDVector());

        if(nodedistance <= distance) {
            add(ref);
        }

        final double refdistance = pos.squaredDistance(deltav);

        if(refdistance > distance) {
            return;
        }

        double refv[] = ref.getKDVector().getDimensionArray();
        final int delta = ref.getKDVector().binaryDeltaDirection(pos);
        final int nodeorder[] = getChildOrdering(pos.getDimension(), delta);

        double deltax[] = null;
        for(int childpos: nodeorder) {
            KDNode<?> child = ref.getChildAt(childpos);

            if(child == null) {
                continue;
            }

            int diff = delta ^ childpos;

            if(diff == 0) {
                search(child, deltav);
                if(refdistance > distance) {
                    return;
                }
                continue;
            }

            if(deltax == null) {
                deltax = new double[deltav.length];
            }
            System.arraycopy(deltav, 0, deltax, 0, deltav.length);
            for(int i = 0; diff > 0 && i < deltax.length; i++) {
                final int bit = diff & (1 << i);
                if(bit > 0) {
                    deltax[i] = refv[i];
                    diff &= ~bit;
                }
            }
            search(child, deltax);
            if(refdistance > distance) {
                return;
            }
        }
    }

    /**
     * Helper to count the nonzero bits of an integer.
     * 
     * @param i The integer to count
     * @return The number of nonzero bits.
     */
    public static int bitcount(int i) {
        int result = 0;
        while(i != 0) {
            i &= ~(-i);
            result++;
        }
        return result;
    }

}
