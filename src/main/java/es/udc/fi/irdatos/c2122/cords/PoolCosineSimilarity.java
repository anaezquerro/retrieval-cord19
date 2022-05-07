package es.udc.fi.irdatos.c2122.cords;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static es.udc.fi.irdatos.c2122.cords.CollectionReader.readQueryEmbeddings;
import static es.udc.fi.irdatos.c2122.cords.CollectionReader.streamDocEmbeddings;

import es.udc.fi.irdatos.c2122.schemas.TopDocumentOrder;
import org.apache.commons.math3.linear.ArrayRealVector;
import es.udc.fi.irdatos.c2122.schemas.TopDocument;


public class PoolCosineSimilarity {
    private static Map<Integer, ArrayRealVector> queryEmbeddings;
    private static File folderResults;

    public PoolCosineSimilarity(Map<Integer, ArrayRealVector> queryEmbeddings) {
        this.queryEmbeddings = queryEmbeddings;
        this.folderResults = null;
    }

    public PoolCosineSimilarity(Map<Integer, ArrayRealVector> queryEmbeddings, String foldername) {
        this.queryEmbeddings = queryEmbeddings;
        this.folderResults = new File(foldername);
    }

    private class WorkerCS implements Runnable {
        private List<Integer> topicsID;
        private int workerID;
        private volatile Map<Integer, List<TopDocument>> queryDocSimilarities = new HashMap<>();

        public WorkerCS(List<Integer> topicsID, int workerID) {
            this.topicsID = topicsID;
            this.workerID = workerID;
        }

        @Override
        public void run() {
            for (int topicID : topicsID) {
                System.out.println("Worker " + workerID + ": Computing similarity in topic " + topicID);

                // Obtain the query embedding
                ArrayRealVector queryEmbedding = queryEmbeddings.get(topicID);

                // Create list object in which store for each document, the similarity with the current query
                List<TopDocument> docSimilarities = new ArrayList<>();

                // Create document embeddings stream (in order to iterate over CSV lines)
                Stream<String> docEmbeddingsStream = streamDocEmbeddings();

                // For each line compute the similarity
                docEmbeddingsStream.forEach( line -> {
                    String[] lineContent = line.split(",");   // split content by comma delimiter
                    String docID = lineContent[0];                  // obtain the first item: i.e. docID

                    // Parse document embedding (all elements except first)
                    lineContent = Arrays.copyOfRange(lineContent, 1, lineContent.length);
                    ArrayRealVector docEmbedding = new ArrayRealVector(Arrays.stream(lineContent).mapToDouble(Double::parseDouble).toArray());
                    if (docEmbedding.getDimension() != CollectionReader.EMBEDDINGS_DIMENSIONALITY) {
                        System.out.println(docID);
                        System.out.println(docEmbedding);
                    } else if (queryEmbedding.getDimension() != CollectionReader.EMBEDDINGS_DIMENSIONALITY) {
                        System.out.println(topicID);
                        System.out.println(queryEmbedding);
                    } else {
                        // Compute similarity and store it (we do not need document embeddings)
                        TopDocument topDoc = new TopDocument(docID, cosineSimilarity(docEmbedding, queryEmbedding), topicID);
                        docSimilarities.add(topDoc);
                    }
                });

                // Order docSimilarities by similarity value
                Collections.sort(docSimilarities, new TopDocumentOrder());


                // Store the document similarities in the global Map object for each topic
                queryDocSimilarities.put(topicID, docSimilarities);

                // Store in disk all results
                if (!Objects.isNull(folderResults)) {
                    saveQueryDocSimilarities(topicID, docSimilarities);

                }
            }
        }

        public Map<Integer, List<TopDocument>> result() {
            return queryDocSimilarities;
        }
    }

    private static Integer[] coalesce(int numWorkers, int N) {
        int futuresPerWorker = (int) Math.floor((double) N / (double) numWorkers);
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

    public Map<Integer, List<TopDocument>> computeSimilarity() {
        final int numCores = Runtime.getRuntime().availableProcessors();
        System.out.println("Computing Cosine Similarity with " + numCores + " cores");
        List<Integer> topics = IntStream.rangeClosed(1, 50).boxed().collect(Collectors.toList());
        Integer[] workersDivision = coalesce(numCores, topics.size());
        ExecutorService executor = Executors.newFixedThreadPool(numCores);
        List<WorkerCS> workers = new ArrayList<>();

        for (int i = 0; i < numCores; i++) {
            int start = workersDivision[i];
            int end = workersDivision[i+1];
            List<Integer> topicsSlice = topics.subList(start, end);
            System.out.println("Thread " + i + " is indexing articles from " + start + " to " + end);
            WorkerCS worker = new WorkerCS(topicsSlice, i);
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

        Map<Integer, List<TopDocument>> queryDocSimilarities = new HashMap<>();
        for (WorkerCS worker : workers) {
            Map<Integer, List<TopDocument>> result = worker.result();
            queryDocSimilarities.putAll(result);
        }
        return queryDocSimilarities;
    }

    public static double cosineSimilarity(ArrayRealVector doc, ArrayRealVector query) {
        return (doc.dotProduct(query)) / (doc.getNorm() * query.getNorm());
    }

    private void saveQueryDocSimilarities(int topicID, List<TopDocument> docSimilarities) {
        File file = new File(folderResults.toString() + "/" + topicID + ".txt");

        if (!folderResults.exists()) { folderResults.mkdirs(); }
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

        for (TopDocument docSim : docSimilarities) {
            try {
                writer.write(String.join(" ", Integer.toString(topicID), docSim.docID(), Double.toString(docSim.score()), "\n"));
            } catch (IOException e) {
                System.out.println("IOException while saving results of document " + docSim.docID() + " in topic " + topicID);
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
        PoolCosineSimilarity pool = new PoolCosineSimilarity(queryEmbeddings, "cosineSimilarity");
        pool.computeSimilarity();
    }

}

