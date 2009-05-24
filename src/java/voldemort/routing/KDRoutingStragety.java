package voldemort.routing;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.NotImplementedException;

import voldemort.cluster.Node;
import voldemort.store.kdtree.KDNode;
import voldemort.store.kdtree.KDUtil;
import voldemort.store.kdtree.KDVector;

/**
 * A RoutingStragety that will buid a randomized KD-Tree out of a list of nodes
 * and a partition to position vector mapping. The key used by this store is a
 * n-dimensional vector containing double values.
 */
public class KDRoutingStragety implements RoutingStrategy {

    /**
     * The internal root node. Might be any node in the tree.
     */
    private KDNode<Node> root;

    /**
     * Number of nodes to keep a single datum.
     */
    private final int replicationFactor;

    /**
     * Create a new RoutingStragety based on
     * <ul>
     * <li>The number of available nodes/partition</li>
     * <li>A discrete partition to vector mapping</li>
     * <li>Number of replications</li>
     * </ul>
     * 
     * @param nodes
     * @param nodemapping
     */
    public KDRoutingStragety(Collection<Node> nodes,
                             Map<Integer, double[]> nodemapping,
                             int replicationFactor) {
        this.replicationFactor = replicationFactor;
        HashSet<KDNode<Node>> treenodes = new HashSet<KDNode<Node>>(nodemapping.size() * 2);
        for(Node node: nodes) {
            for(int partition: node.getPartitionIds()) {
                KDNode<Node> treenode = new KDNode<Node>(nodemapping.get(partition));
                treenode.setData(node);
                treenodes.add(treenode);
            }
        }
        for(KDNode<Node> node: treenodes) {
            if(root == null) {
                root = node;
            } else {
                root.add(node);
            }
        }
    }

    /**
     * Not yet implemented
     */
    @Override
    public Set<Node> getNodes() {
        // TODO request method contract/docs
        throw new NotImplementedException("getNodes() not implemented on KDRoutingStragety");
    }

    /**
     * Not yet implemented
     */
    @Override
    public List<Integer> getPartitionList(byte[] key) {
        // TODO request method contract/docs
        throw new NotImplementedException("getPartitionList(key) not implemented on KDRoutingStragety");
    }

    /**
     * Return the target nodes for a given key.
     * 
     * @param key The key to route.
     * @return The target nodes, up to replication factor.
     */
    @Override
    public List<Node> routeRequest(byte[] key) {
        KDNode<Node>[] nodes = root.getNearestUniqueValueNodes(new KDVector(KDUtil.getDimensionsForKey(key)),
                                                               replicationFactor,
                                                               Double.MAX_VALUE);
        Node result[] = new Node[nodes.length];
        for(int i = 0; i < result.length; i++) {
            result[i] = nodes[i].getData();
        }
        return Arrays.asList(result);
    }

}
