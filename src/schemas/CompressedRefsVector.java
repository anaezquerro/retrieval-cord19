package schemas;

import cords.PageRank;
import org.apache.commons.math3.linear.ArrayRealVector;

import java.util.*;
import java.util.stream.IntStream;

public class CompressedRefsVector {
    private Map<Integer, Double> values;

    private int size;
    private float alpha = PageRank.alpha;
    public static String ITEM_VECTOR_SEP = " ";


    public CompressedRefsVector(int size) {
        values = new HashMap<>();
        this.size = size;
    }

    public void add(int index, double value) {
        values.put(index, value);
    }

    public ArrayRealVector toCountVector() {
        ArrayRealVector vector = new ArrayRealVector(size);
        values.entrySet().stream().forEach(
                entry -> {
                    vector.setEntry(entry.getKey(), entry.getValue());
                }
        );
        return vector;
    }

    private ArrayRealVector _normalize(ArrayRealVector vector) {
        double norm = vector.getL1Norm();
        if (norm == 0) {
            vector = new ArrayRealVector(size, 1/size);
            norm = 1;
        }
        vector.mapMultiplyToSelf(1/norm);
        vector.mapMultiplyToSelf(1-alpha);
        vector.mapMultiplyToSelf(alpha/size);
        return vector;
    }

    public Map<Integer, Double> values() {
        return values;
    }

    public ReferencesVector toReferencesVector(boolean norm) {
        ArrayRealVector binaryVector = new ArrayRealVector(size, 0);
        ArrayRealVector countVector = new ArrayRealVector(size, 0);

        values.entrySet().stream().forEach(
                entry -> {
                    binaryVector.setEntry(entry.getKey(), 1);
                    countVector.setEntry(entry.getKey(), entry.getValue());
                }

        );

        if (norm) {
            return new ReferencesVector(this._normalize(binaryVector), this._normalize(countVector));
        } else {
            return new ReferencesVector(binaryVector, countVector);
        }
    }
}
