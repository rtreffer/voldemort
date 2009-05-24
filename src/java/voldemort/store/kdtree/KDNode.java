package voldemort.store.kdtree;

/**
 * KDNode impements a kd-tree, a k-dimensional tree. This is the logical
 * successor of Quad and Octrees.<br/>
 * The tree is synchronized against concurrent modifications (big lock).
 * Deletions are stored as null values, null is not allowed as a value. Reads
 * are always unsynchronized, so there is no guarante that the read will reflect
 * the state of the tree at the time the read ended.
 * 
 * @param <T> The payload of this tree.
 */
public class KDNode<T> {

    /**
     * The content array of this nnode. Immutable.
     */
    private final KDVector dim;

    /**
     * All childs of this NNode, up to 2^dim.length
     */
    private volatile KDNode<T>[] childs;

    /**
     * The parent node (or null if we are the root)
     */
    private volatile KDNode<T> parent;

    /**
     * The payload of this tree.
     */
    private volatile T data;

    /**
     * The real childcount (excluding empty array slots)
     */
    private transient int childcount;

    /**
     * Create a NNode instance.
     * 
     * @param idim The n-dimensional
     */
    @SuppressWarnings("unchecked")
    public KDNode(final double[] idim) {
        super();
        this.dim = new KDVector(idim);
        childs = new KDNode[1 << dim.getDimension()];
    }

    /**
     * Add a NNode to the childs of this NNode (propably recursing into child
     * NNodes)
     * 
     * @param node The NNode to insert
     */
    public final void add(final KDNode<T> node) {

        final int pos = getPos(node);

        synchronized(childs) {
            if(childs[pos] == null) {
                node.parent = this;
                childs[pos] = node;
                childcount++;
            }
        }

        if(childs[pos] != node) {
            childs[pos].add(node);
        }

    }

    /**
     * Find the index of a child NNode.
     * 
     * @param node The NNode to probe
     * @return The possible index within the childs array.
     */
    public final int getPos(final KDNode<T> node) {
        return dim.binaryDeltaDirection(node.dim);
    }

    /**
     * Remove this NNode from its parent.
     */
    public final void remove() {
        if(parent != null) {
            parent.remove(this);
        } else {
            data = null;
        }
    }

    /**
     * Directly clear a child position and recurse if needed.
     * 
     * @param pos The index to clear
     */
    private void dremove(final int pos) {
        childs[pos] = null;
        childcount--;
        if((data == null) && (childcount == 0)) {
            remove();
        }
    }

    /**
     * Remove a NNode from the subtree.
     * 
     * @param node The NNode to remove.
     */
    public final void remove(final KDNode<T> node) {

        final int pos = getPos(node);
        final KDNode<T> child = childs[pos];

        if(child.equals(node)) {
            synchronized(childs) {
                if(childs[pos] == child) {
                    dremove(pos);
                    return;
                }
            }
        }

        child.remove(node);

    }

    /**
     * Retrieve the tree payload.
     * 
     * @return The trees payload.
     */
    public final T getData() {
        return data;
    }

    /**
     * Set this nodes payload.
     * 
     * @param idata The new node payload.
     */
    public final void setData(final T idata) {
        data = idata;
        synchronized(childs) {
            if(data == null && childcount == 0) {
                if(data == null && childcount == 0) {
                    remove();
                }
            }
        }
    }

    /**
     * Compute 2^dimension.
     * 
     * @return The maximum length of the childs array.
     */
    public int getMaxChildCount() {
        return 1 << dim.getDimension();
    }

    /**
     * Return the value of a very specific child.
     * 
     * @param pos The index of the child node.
     * @return The KDNode related to that index, or null if there is no child.
     */
    public KDNode<T> getChildAt(final int pos) {

        if(pos >= 0 && pos < childs.length) {
            return childs[pos];
        }

        return null;
    }

    /**
     * Return the child for a given key.
     * 
     * @param pos The dimensional position of the child.
     * @return The KDNode related to that index, or null if there is no child.
     */
    public KDNode<T> getChildAt(final double pos[]) {
        return getChildAt(dim.binaryDeltaDirection(pos));
    }

    /**
     * Retrieve the parent NNode of this NNode.
     * 
     * @return The parent NNode of this NNode
     */
    public KDNode<T> getParent() {
        return parent;
    }

    public KDNode<T> getNode(KDVector pos) {
        return getNode(pos.getDimensionArray());
    }

    public KDNode<T> getNode(double dim[]) {
        if(this.dim.equals(dim)) {
            return this;
        }
        KDNode<T> child = getChildAt(dim);
        return (child == null) ? null : child.getNode(dim);
    }

    /**
     * Retrieve a list of the nearest nodes.
     * 
     * @param position The target position.
     * @param count The maximum number of elements.
     * @param squaredDistance The maximum distance (sqaured euklid distance).
     * @return The nearest nodes.
     */
    @SuppressWarnings("unchecked")
    public KDNode<T>[] getNearestNodes(KDVector position, int count, double squaredDistance) {
        KDNearestNodeSearch search = new KDNearestNodeSearch(this, position, squaredDistance, count);
        return (KDNode<T>[]) search.search();
    }

    /**
     * Retrieve a list of the nearest nodes. This method will ensure that each
     * nodes value is unique. In case of a value collision the nearest node will
     * be returned.
     * 
     * @param position The target position.
     * @param count The maximum number of elements.
     * @param squaredDistance The maximum distance (sqaured euklid distance).
     * @return The nearest nodes.
     */
    @SuppressWarnings("unchecked")
    public KDNode<T>[] getNearestUniqueValueNodes(KDVector position,
                                                  int count,
                                                  double squaredDistance) {
        KDNearestNodeSearch search = new KDUniqueDataNodeSearch(this,
                                                                position,
                                                                squaredDistance,
                                                                count);
        return (KDNode<T>[]) search.search();
    }

    /**
     * Hashcode of this object, respects the position vector (key) and the value
     * only.
     * 
     * @return The hashcode of this node.
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((data == null) ? 0 : data.hashCode());
        result = prime * result + dim.hashCode();
        return result;
    }

    /**
     * Compares this instance to second Object, returning true if key (position
     * vector) and value are the same.
     */
    @SuppressWarnings("unchecked")
    @Override
    public boolean equals(Object obj) {
        if(this == obj)
            return true;
        if(obj == null)
            return false;
        if(getClass() != obj.getClass())
            return false;
        KDNode other = (KDNode) obj;
        if(data == null) {
            if(other.data != null)
                return false;
        } else if(!data.equals(other.data))
            return false;
        return dim.equals(other.dim);
    }

    /**
     * Retrieve the k-dimensional vector of this node.
     * 
     * @return
     */
    public final KDVector getKDVector() {
        return dim;
    }

    /**
     * The euclid distance between two positions in the KD-Space. Let
     * A=&lt;a1,&nbsp;a2,&nbsp;a3&gt; and B=&lt;b1,&nbsp;b2,&nbsp;b3&gt;.
     * d(A,B)=sqrt(sum(ai-bi)).
     * 
     * @param edim
     * @return
     */
    public final double distance(final KDVector v) {
        return Math.sqrt(squareDistance(v));
    }

    /**
     * Compute the squared euclid distance. This is enough for computing the
     * order of elements without a need to run the (expensive)
     * 
     * @param edim
     * @return
     */
    public final double squareDistance(final KDVector v) {
        return dim.squaredDistance(v);
    }

}
