package es.udc.fi.irdatos.c2122.cords;

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Stream;

import static es.udc.fi.irdatos.c2122.cords.CollectionReader.readQueryEmbeddings;
import static es.udc.fi.irdatos.c2122.cords.CollectionReader.readDocEmbeddings;
import org.apache.commons.math3.linear.ArrayRealVector;



public class ComputeSimilarity {
    private static final Map<String, Integer> docEmbeddings = readDocEmbeddings();
    private static final Path DEFAULT_COLLECTION_PATH = Paths.get("2020-07-16");
    private static String QUERY_EMBEDDINGS_FILENAME = "query_embeddings.json";
    private static String DOC_EMBEDDINGS_FILENAME = "cord_19_embeddings_2020-07-16.csv";
    private static Map<Integer, ArrayRealVector> queryEmbeddings = readQueryEmbeddings();


    public static class TopicReader implements Runnable {
        private int[] topicsID;

        public TopicReader(int[] topicsID) {
            this.topicsID = topicsID;
        }

        @Override
        public double[] void run() {
            Map<Integer, Map<String, Double>> queryDocSimilarities = new HashMap<>();

            for (int topicID : topicsID) {
                ArrayRealVector topicEmbedding = queryEmbeddings.get(topicID);
                Map<String, Double> docSimilarities = new HashMap<>();
                for (String docID : docEmbeddings.keySet()) {
                    ArrayRealVector docEmbedding = readDocEmbedding(docID);
                    double sim = cosineSimilarity(topicEmbedding, docEmbedding);
                    docSimilarities.put()
                }

            }
        }



    }



    public static ArrayRealVector readDocEmbedding(String docID) {
        ArrayRealVector embedding = null;
        try (Stream<String> lines = Files.lines(DEFAULT_COLLECTION_PATH.resolve(DOC_EMBEDDINGS_FILENAME))) {
            String[] lineContent = lines.skip(docEmbeddings.get(docID)).findFirst().get().split(",");
            lineContent = Arrays.copyOfRange(lineContent, 1, lineContent.length);
            embedding = new ArrayRealVector(Arrays.stream(lineContent).mapToDouble(
                    Double::parseDouble).toArray());
        } catch (IOException e) {
            System.out.println("IOException while reading doc embedding with ID=" + docID);
            e.printStackTrace();
        }
        return embedding;
    }

    public static double cosineSimilarity(ArrayRealVector doc, ArrayRealVector query) {
        return (doc.dotProduct(query)) / (doc.getNorm() * query.getNorm());
    }

    public static void main(String[] args) throws IOException {
        for (String docID : docEmbeddings.keySet()) {
            ArrayRealVector embedding = readDocEmbedding(docID);
        }
    }
}

