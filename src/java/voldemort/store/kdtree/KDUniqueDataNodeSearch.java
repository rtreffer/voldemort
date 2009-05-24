package voldemort.store.kdtree;

import java.util.HashMap;

/**
 * A nearest node search that will never add a single value twice. This is
 * needed e.g. to route data do guaranteed different nodes.
 */
public class KDUniqueDataNodeSearch extends KDNearestNodeSearch {

    private HashMap<Object, KDNode<?>> data;

    public KDUniqueDataNodeSearch(KDNode<?> root, KDVector pos, double idistance, int icount) {
        super(root, pos, idistance, icount);
        data = new HashMap<Object, KDNode<?>>();
    }

    /**
     * Add a node to the result set. This method guards the normal search add
     * method against duplicated values.
     */
    @Override
    protected void add(KDNode<?> node) {
        KDNode<?> oldNode = data.get(node.getData());
        if(oldNode != null) {
            if(comparator.compare(node, oldNode) >= 0) {
                // New data added at a higher distance -> drop
                return;
            }
            // remove old version of the node
            result.remove(oldNode);
        }
        data.put(node.getData(), node);
        super.add(node);
    }

}
