package es.udc.fi.irdatos.c2122.cords;

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static es.udc.fi.irdatos.c2122.cords.CollectionReader.readDocEmbedding;
import static es.udc.fi.irdatos.c2122.cords.CollectionReader.readQueryEmbeddings;
import static es.udc.fi.irdatos.c2122.cords.CollectionReader.readDocEmbeddings;
import static es.udc.fi.irdatos.c2122.cords.CollectionReader.readDocEmbedding;

import org.apache.commons.math3.linear.ArrayRealVector;



public class ComputeSimilarity {
    private static final Path DEFAULT_COLLECTION_PATH = Paths.get("2020-07-16");
    private static String QUERY_EMBEDDINGS_FILENAME = "query_embeddings.json";
    private static String DOC_EMBEDDINGS_FILENAME = "cord_19_embeddings_2020-07-16.csv";
    private static Map<Integer, ArrayRealVector> queryEmbeddings = readQueryEmbeddings();
    private static Map<String, Integer> docEmbeddings = readDocEmbeddings();


    public static class TopicReader implements Runnable {
        private List<Integer> topicsID;
        private volatile Map<Integer, Map<String, Double>> queryDocSimilarities = new HashMap<>();

        public TopicReader(List<Integer> topicsID) {
            this.topicsID = topicsID;
        }

        public static double cosineSimilarity(ArrayRealVector doc, ArrayRealVector query) {
            return (doc.dotProduct(query)) / (doc.getNorm() * query.getNorm());
        }

        @Override
        public void run() {
            for (int topicID : topicsID) {
                ArrayRealVector topicEmbedding = queryEmbeddings.get(topicID);
                Map<String, Double> docSimilarities = new HashMap<>();
                for (String docID : docEmbeddings.keySet()) {
                    ArrayRealVector docEmbedding = readDocEmbedding(docID, docEmbeddings);
                    double sim = cosineSimilarity(topicEmbedding, docEmbedding);
                    docSimilarities.put(docID, sim);
                }
                queryDocSimilarities.put(topicID, docSimilarities);
            }
        }

        public Map<Integer, Map<String, Double>> result() {
            return queryDocSimilarities;
        }
    }


    public static Map<Integer, Map<String, Double>> computeSimilarity() {
        final int numCores = Runtime.getRuntime().availableProcessors();

        List<Integer> topics = IntStream.rangeClosed(1, 50).boxed().collect(Collectors.toList());
        int topicsPerThread = (int) Math.ceil((double)topics.size()/(double)numCores);
        ExecutorService executor = Executors.newFixedThreadPool(numCores);
        List<TopicReader> workers = new ArrayList<>();

        for (int i=0; i < numCores; i++) {
            int start = i*topicsPerThread;
            int end = Math.min(queryEmbeddings.keySet().size(), (i+1)*topicsPerThread);
            List<Integer> topicsSlice = topics.subList(start, end);
            System.out.println("Thread " + i + " is indexing articles from " + start + " to " + end);
            TopicReader worker = new TopicReader(topicsSlice);
            executor.execute(worker);
            workers.add(worker);
        }

        // End the executor
        executor.shutdown();
        try {
            executor.awaitTermination(10, TimeUnit.MINUTES);
        } catch (final InterruptedException e) {
            e.printStackTrace();
            System.exit(-2);
        }

        Map<Integer, Map<String, Double>> queryDocSimilarities = new HashMap<>();
        for (TopicReader worker : workers) {
            Map<Integer, Map<String, Double>> result = worker.result();
            queryDocSimilarities.putAll(result);
        }
        return queryDocSimilarities;
    }

    public static void main(String[] args) {
        Map<Integer, Map<String, Double>> queryDocSimilarities = computeSimilarity();
        System.out.println(queryDocSimilarities);
    }

}

