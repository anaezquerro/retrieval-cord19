package schemas;

import org.apache.commons.math3.linear.ArrayRealVector;

import java.sql.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;

public class Embedding {
    private float[] floatEmbedding;
    private ArrayRealVector arrayEmbedding;
    private int size;

    public Embedding(String content) {
        this(content.split(" "));
    }

    public Embedding(String[] arrayContent) {
        size = arrayContent.length;
        arrayEmbedding = new ArrayRealVector(size);
        floatEmbedding = new float[size];
        IntStream.range(0, size).forEach(
                i -> {
                    arrayEmbedding.setEntry(i, Double.parseDouble(arrayContent[i]));
                    floatEmbedding[i] = Float.parseFloat(arrayContent[i]);
                }
        );
    }


    public Embedding(ArrayRealVector vector) {
        size = vector.getDimension();
        floatEmbedding = new float[size];
        IntStream.range(0, size).forEach(
                i -> {
                    floatEmbedding[i] = (float) vector.getEntry(i);
                }
        );
    }

     public float[] getFloat() {
        return floatEmbedding;
     }

     public ArrayRealVector getArray() {
        return arrayEmbedding;
     }

    @Override
    public String toString() {
        String[] content = new String[size];
        IntStream.range(0, size).forEach(i -> {content[i] =  String.valueOf(floatEmbedding[i]);});
        return String.join(" ", content);
    }

    public int size() {
        return size;
    }
}
