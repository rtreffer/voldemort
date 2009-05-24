package voldemort.store.kdtree;

import java.util.Arrays;

/**
 * A k-dimensional vector (mathematic n-tuple).
 */
public class KDVector {

    /**
     * The content array of this nnode. Immutable.
     */
    private final double[] dim;

    /**
     * Create a new KDVector. This class does not clone the array dim. The
     * calleer must ensure that the array won't be altered afterwards.
     * 
     * @param dim The dimension array.
     */
    public KDVector(double dim[]) {
        this.dim = dim;
    }

    /**
     * This is the square of the normal distance (or in other words it's the
     * euclid distance without a squareroot.
     * 
     * @param dim The other dimensional array.
     * @return The squared distance.
     */
    public double squaredDistance(final double[] dim) {
        double distance = 0d;
        for(int i = 0; i < dim.length; i++) {
            final double delta = dim[i] - this.dim[i];
            distance += delta * delta;
        }
        return distance;
    }

    public double squaredDistance(final KDVector v) {
        return squaredDistance(v.dim);
    }

    public double distance(final KDVector v) {
        return Math.sqrt(squaredDistance(v.dim));
    }

    public int getDimension() {
        return dim.length;
    }

    public double[] getDimensionArray() {
        return dim;
    }

    public int binaryDeltaDirection(KDVector v) {
        return binaryDeltaDirection(v.dim);
    }

    public int binaryDeltaDirection(double[] pos) {
        int delta = 0;
        for(int i = 0; i < pos.length; i++) {
            if(pos[i] > dim[i]) {
                delta += 1 << i;
            }
        }
        return delta;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(dim);
    }

    public boolean equals(double dim[]) {
        return Arrays.equals(this.dim, dim);
    }

    @Override
    public boolean equals(Object obj) {
        if(this == obj)
            return true;
        if(obj == null)
            return false;
        if(getClass() != obj.getClass())
            return false;
        KDVector other = (KDVector) obj;
        return Arrays.equals(dim, other.dim);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(10 * dim.length);
        sb.append('<');
        sb.append(dim[0]);
        for(int i = 1; i < dim.length; i++) {
            sb.append(", ");
            sb.append(dim[i]);
        }
        sb.append('>');
        return sb.toString();
    }

}
