package schemas;

import org.apache.commons.math3.exception.DimensionMismatchException;
import org.apache.commons.math3.linear.ArrayRealVector;

import static util.AuxiliarFunctions.*;
import static cords.PageRank.VECTOR_ITEM_SEP;

public class ReferencesVector {
    private ArrayRealVector binaryVector;
    private ArrayRealVector countVector;
    private int size;

    public class ReferenceEntry {
        private double binaryValue;
        private double countValue;

        public ReferenceEntry(double binaryValue, double countValue) {
            this.binaryValue = binaryValue;
            this.countValue = countValue;
        }

        public double binaryValue() {
            return binaryValue;
        }

        public double countValue() {
            return countValue;
        }
    }

    public ReferencesVector(ArrayRealVector binaryVector, ArrayRealVector countVector) throws DimensionMismatchException {
        if (binaryVector.getDimension() != countVector.getDimension()) {
            throw new DimensionMismatchException(binaryVector.getDimension(), countVector.getDimension());
        }
        this.binaryVector = binaryVector;
        this.countVector = countVector;
        size = binaryVector.getDimension();
    }

    public ReferencesVector(int size) {
        this.size = size;
        binaryVector = new ArrayRealVector(size, 0.0);
        countVector = new ArrayRealVector(size, 0.0);
    }

    public ReferencesVector(String binaryString, String countString) {
        this(string2vector(binaryString, VECTOR_ITEM_SEP), string2vector(countString, VECTOR_ITEM_SEP));
    }

    public ArrayRealVector binary() {
        return binaryVector;
    }

    public ArrayRealVector count() {
        return countVector;
    }

    public String binary2string() {
        return vector2string(binaryVector, VECTOR_ITEM_SEP);
    }

    public String count2string() {
        return vector2string(countVector, VECTOR_ITEM_SEP);
    }

    public void setEntries(int index, ReferenceEntry referenceEntry) {
        binaryVector.setEntry(index, referenceEntry.binaryValue());
        countVector.setEntry(index, referenceEntry.countValue());
    }

    public ReferenceEntry getEntries(int index) {
        return new ReferenceEntry(binaryVector.getEntry(index), countVector.getEntry(index));
    }
}

