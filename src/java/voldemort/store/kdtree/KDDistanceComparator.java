package voldemort.store.kdtree;

import java.util.Comparator;

/**
 * A comparator for KDNodes (based on a given reference point). Collisions
 * (based on distance) will be resolved by comparing the vectors at both nodes.
 */
public class KDDistanceComparator implements Comparator<KDNode<?>> {

    /**
     * The reference point.
     */
    private final KDVector referencePoint;

    /**
     * Create a new comparator that will sort nodes based on the distance from
     * the reference point.
     * 
     * @param v The reference point for distance computation.
     */
    public KDDistanceComparator(final KDVector v) {
        this.referencePoint = v;
    }

    /**
     * Compare two nodes. The first node is smaller if its nearer to the
     * reference point. In the case of same distnace the node 1 is regarded as
     * smaller if its dimensional array is smaller.
     * 
     * @param o1 The first node
     * @param o2 The second node
     * @return -1,0,1 if the first node is smaller, same size or bigger than the
     *         second node.
     */
    @Override
    public strictfp final int compare(final KDNode<?> o1, final KDNode<?> o2) {
        double v = o1.squareDistance(referencePoint) - o2.squareDistance(referencePoint);
        if(v != 0d) {
            return (int) Math.signum(v);
        }
        double d1[] = o1.getKDVector().getDimensionArray();
        double d2[] = o2.getKDVector().getDimensionArray();
        for(int i = 0; i < d1.length; i++) {
            if(d1[i] != d2[i]) {
                return (int) Math.signum(d1[i] - d2[i]);
            }
        }
        return 0;
    }

}
