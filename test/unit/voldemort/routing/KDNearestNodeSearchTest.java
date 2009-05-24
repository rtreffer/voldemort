package voldemort.routing;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.TreeSet;

import voldemort.store.kdtree.KDDistanceComparator;
import voldemort.store.kdtree.KDNearestNodeSearch;
import voldemort.store.kdtree.KDNode;
import voldemort.store.kdtree.KDVector;

import junit.framework.TestCase;

public class KDNearestNodeSearchTest extends TestCase {

    public void testNodeGeneration() {
        KDNode<?>[] nodes = generateAll(new double[][] { { 0, 0.35, 0.7 }, { 0.1, 0.45, 0.8 },
                { 0.2, 0.55, 0.9 }, { 0.3, 0.65, 1 } });
        assertEquals(nodes.length, 81);
    }

    public void testNearestNodeSearch1() {
        KDNode<Object>[] nodes = (KDNode<Object>[]) generateAll(new double[][] { { 0, 0.35, 0.7 },
                { 0.1, 0.45, 0.8 }, { 0.2, 0.55, 0.9 }, { 0.3, 0.65, 1 } });
        assertEquals(nodes.length, 81);
        KDNode<Object> root = nodes[0];
        for(int i = 1; i < nodes.length; i++) {
            root.add(nodes[i]);
        }
        for(int i = 1; i < 81; i++) {
            KDNode<Object>[] search = (KDNode<Object>[]) new KDNearestNodeSearch(root,
                                                                                 nodes[i].getKDVector(),
                                                                                 Double.MAX_VALUE,
                                                                                 i).search();
            HashSet<KDNode<Object>> found = new HashSet<KDNode<Object>>();
            found.addAll(Arrays.asList(search));
            assertEquals(found.size(), i); // Element count ok?
            TreeSet<KDNode<Object>> distanceSet = new TreeSet<KDNode<Object>>(new KDDistanceComparator(nodes[i].getKDVector()));
            distanceSet.addAll(Arrays.asList(nodes));
            Iterator<KDNode<Object>> iterator = distanceSet.iterator();
            for(int j = 0; j < i; j++) {
                KDNode<Object> pnode = iterator.next();
                if(!found.contains(pnode)) {
                    System.err.println("Searched for " + i + " nearest nodes, reference position "
                                       + nodes[i].getKDVector());
                    dump(found, distanceSet, nodes[i].getKDVector());
                }
                assertTrue("Probe element " + j + "/" + i, found.contains(pnode));
            }
        }
    }

    public void testNearestNodeSearch2() {
        KDNode<Object>[] nodes = (KDNode<Object>[]) generateAll(new double[][] {
                { 0, 0.2, 0.4, 0.6, 0.8 }, { 0.08, 0.28, 0.48, 0.68, 0.88 },
                { 0.16, 0.36, 0.56, 0.76, 0.96 } });
        assertEquals(nodes.length, 125);
        KDNode<Object> root = nodes[0];
        for(int i = 1; i < nodes.length; i++) {
            root.add(nodes[i]);
        }
        long searchtime = 0;
        for(int i = 1; i < 125; i++) {
            searchtime = searchtime - System.currentTimeMillis();
            KDNode<Object>[] search = (KDNode<Object>[]) new KDNearestNodeSearch(root,
                                                                                 nodes[i].getKDVector(),
                                                                                 Double.MAX_VALUE,
                                                                                 3).search();
            HashSet<KDNode<Object>> found = new HashSet<KDNode<Object>>();
            searchtime += System.currentTimeMillis();
            found.addAll(Arrays.asList(search));
            assertEquals(found.size(), 3); // Element count ok?
            TreeSet<KDNode<Object>> distanceSet = new TreeSet<KDNode<Object>>(new KDDistanceComparator(nodes[i].getKDVector()));
            for(KDNode<Object> n: nodes) {
                if(distanceSet.size() > 3) {
                    distanceSet.remove(distanceSet.last());
                }
                distanceSet.add(n);
            }
            Iterator<KDNode<Object>> iterator = distanceSet.iterator();
            for(int j = 0; j < 3; j++) {
                KDNode<Object> pnode = iterator.next();
                if(!found.contains(pnode)) {
                    System.err.println("Searched for 3 nearest nodes, reference position "
                                       + nodes[i].getKDVector());
                    dump(found, distanceSet, nodes[i].getKDVector());
                }
                assertTrue("Probe element " + j + "/" + i, found.contains(pnode));
            }
        }
        System.out.println("Total search time: " + searchtime + "ms");
        System.out.flush();
    }

    private final static void dump(Collection<KDNode<Object>> found,
                                   Collection<KDNode<Object>> expected,
                                   KDVector referencePos) {
        System.err.println("--- FOUND ---");
        Iterator<KDNode<Object>> iterator = found.iterator();
        while(iterator.hasNext()) {
            KDNode<?> node = iterator.next();
            System.err.println(node.getKDVector() + "::" + node.distance(referencePos));
        }
        System.err.println("--- EXPECTED ---");
        iterator = expected.iterator();
        while(iterator.hasNext()) {
            KDNode<?> node = iterator.next();
            System.err.println(node.getKDVector() + "::" + node.distance(referencePos));
        }
    }

    private final KDNode<?>[] generateAll(double[][] v) {
        int p[] = new int[v.length];
        // Hashing to randomize the nodes a bit
        HashSet<KDNode<?>> nodes = new HashSet<KDNode<?>>();
        for(;;) {
            double vec[] = new double[v.length];
            for(int i = 0; i < vec.length; i++) {
                vec[i] = v[i][p[i]];
            }
            nodes.add(new KDNode<Object>(vec));
            boolean con = true;
            for(int i = 0; con && i < v.length; i++) {
                p[i]++;
                if(p[i] >= v[i].length) {
                    p[i] = 0;
                } else {
                    con = false;
                }
            }
            if(con) {
                return nodes.toArray(new KDNode<?>[nodes.size()]);
            }
        }
    }

}
