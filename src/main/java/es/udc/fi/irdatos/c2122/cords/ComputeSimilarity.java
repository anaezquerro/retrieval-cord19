package es.udc.fi.irdatos.c2122.cords;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Array;
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

import static es.udc.fi.irdatos.c2122.cords.CollectionReader.readQueryEmbeddings;
import static es.udc.fi.irdatos.c2122.cords.CollectionReader.streamDocEmbeddings;

import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.lucene.search.TopDocs;


public class ComputeSimilarity {
    private static Map<Integer, ArrayRealVector> queryEmbeddings;
    private static final Path DEFAULT_COLLECTION_PATH = Paths.get("2020-07-16");



    public static class TopicReader implements Runnable {
        private List<Integer> topicsID;
        private int workerID;
        private volatile Map<Integer, Map<String, Double>> queryDocSimilarities = new HashMap<>();

        public TopicReader(List<Integer> topicsID, int workerID) {
            this.topicsID = topicsID;
            this.workerID = workerID;
        }

        public static double cosineSimilarity(ArrayRealVector doc, ArrayRealVector query) {
            return (doc.dotProduct(query)) / (doc.getNorm() * query.getNorm());
        }

        @Override
        public void run() {
            for (int topicID : topicsID) {
                System.out.println("Worker " + workerID + ": Computing similarity in topic " + topicID);

                // Obtain the query embedding
                ArrayRealVector queryEmbedding = queryEmbeddings.get(topicID);

                // Create Map object in which store for each document, the similarity with the current query
                Map<String, Double> docSimilarities = new HashMap<>();

                // Create document embeddings stream (in order to iterate over CSV lines)
                Stream<String> docEmbeddingsStream = streamDocEmbeddings();

                // For each line compute the similarity
                docEmbeddingsStream.forEach( line -> {
                    String[] lineContent = line.split(",");   // split content by comma delimiter
                    String docID = lineContent[0];                  // obtain the first item: i.e. docID

                    // Parse document embedding (all elements except first)
                    lineContent = Arrays.copyOfRange(lineContent, 1, lineContent.length);
                    ArrayRealVector docEmbedding = new ArrayRealVector(Arrays.stream(lineContent).mapToDouble(Double::parseDouble).toArray());

                    // Compute similarity and store it (we do not need document embeddings)
                    double sim = cosineSimilarity(docEmbedding, queryEmbedding);
                    docSimilarities.put(docID, sim);
                });

                // Store the document similarities in the global Map object for each topic
                queryDocSimilarities.put(topicID, docSimilarities);

                // Store in disk all results
                saveTopicEmbeddingsResults(topicID, docSimilarities);
            }
        }

        public Map<Integer, Map<String, Double>> result() {
            return queryDocSimilarities;
        }
    }

    public static Integer[] coealesce(int numWorkers, int N) {
        int futuresPerWorker = (int) Math.round((double) N / (double) numWorkers);
        int surplus = Math.floorMod(N, numWorkers);

        Integer[] indexes = new Integer[numWorkers + 1];
        indexes[0] = 0;
        for (int i = 1; i <= numWorkers; i++) {
            if (i <= surplus) {
                indexes[i] = indexes[i - 1] + futuresPerWorker + 1;
            } else {
                indexes[i] = indexes[i - 1] + futuresPerWorker;
            }
        }
        return indexes;
    }

    public static Map<Integer, Map<String, Double>> computeSimilarity() {
        final int numCores = Runtime.getRuntime().availableProcessors();

        List<Integer> topics = IntStream.rangeClosed(1, 50).boxed().collect(Collectors.toList());
        Integer[] workersDivision = coealesce(numCores, topics.size());
        ExecutorService executor = Executors.newFixedThreadPool(numCores);
        List<TopicReader> workers = new ArrayList<>();

        for (int i = 0; i < numCores; i++) {
            int start = workersDivision[i];
            int end = workersDivision[i+1];
            List<Integer> topicsSlice = topics.subList(start, end);
            System.out.println("Thread " + i + " is indexing articles from " + start + " to " + end);
            TopicReader worker = new TopicReader(topicsSlice, i);
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


    public static void saveTopicEmbeddingsResults(int topicID, Map<String, Double> queryDocSimilarity) {
        File folder = new File("cosineSimilarity");
        File file = new File(folder.toString() + "/" + topicID + ".txt");

        if (!folder.exists()) { folder.mkdirs(); }
        try {
            if (file.exists()) {
                file.delete();
            }
        } catch (Exception e) {
            System.out.println("IOException while removing "  + file.toString() + " folder");
        }

        FileWriter writer = null;
        try {
            writer = new FileWriter(file.toString());
        } catch (Exception e) {
            System.out.println("Exception occurred while creating the new file: " + file.toString());
            e.printStackTrace();
        }

        for (Map.Entry<String, Double> queryDocSim : queryDocSimilarity.entrySet()) {
            try {
                writer.write(String.join(" ", Integer.toString(topicID), queryDocSim.getKey(), Double.toString(queryDocSim.getValue())));
            } catch (IOException e) {
                System.out.println("IOException while saving results of document " + queryDocSim.getKey() + " in topic " + topicID);
                e.printStackTrace();
            }
        }

        try {
            writer.close();
        } catch (IOException e) {
            System.out.println("IOException while closing the txt writer");
            e.printStackTrace();
        }
    }


    public static void main(String[] args) {
        // Obtain query embeddings
        queryEmbeddings = readQueryEmbeddings();

        Map<Integer, Map<String, Double>> queryDocSimilarities = computeSimilarity();
    }

}

