package es.udc.fi.irdatos.c2122.cords;
import java.util.List;
import java.util.Map;

public record DocEmbeddings(Map<String, Embedding> queryEmbedding) {
    public record Embedding(List<Float> embedding) {}
}
