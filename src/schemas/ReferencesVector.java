package schemas;

import org.apache.commons.math3.linear.ArrayRealVector;

import static util.AuxiliarFunctions.*;
import static cords.PageRank.VECTOR_ITEM_SEP;

public class ReferencesVector {
    private ArrayRealVector binaryVector;
    private ArrayRealVector countVector;

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

    public ReferencesVector(ArrayRealVector binaryVector, ArrayRealVector countVector) {
        this.binaryVector = binaryVector;
        this.countVector = countVector;
    }

    public ReferencesVector(int size) {
        binaryVector = new ArrayRealVector(size);
        countVector = new ArrayRealVector(size);
    }

    public ReferencesVector(String binaryString, String countString) {
        this.binaryVector = string2vector(binaryString, VECTOR_ITEM_SEP);
        this.countVector = string2vector(countString, VECTOR_ITEM_SEP);
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

