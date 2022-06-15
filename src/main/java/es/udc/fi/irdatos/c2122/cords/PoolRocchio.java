package es.udc.fi.irdatos.c2122.cords;

import es.udc.fi.irdatos.c2122.schemas.TopDocument;
import es.udc.fi.irdatos.c2122.schemas.TopDocumentOrder;
import org.apache.commons.math3.analysis.interpolation.LinearInterpolator;
import org.apache.commons.math3.linear.ArrayRealVector;

import java.lang.reflect.Array;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static es.udc.fi.irdatos.c2122.cords.CollectionReader.readDocEmbeddings;
import static es.udc.fi.irdatos.c2122.cords.CollectionReader.streamDocEmbeddings;

public class PoolRocchio {
    private static Map<Integer, List<TopDocument>> initialResults;
    private static Map<Integer, ArrayRealVector> queryEmbeddings;
    private static Map<Integer, List<TopDocument>> newResults;
    private static Map<Integer, ArrayRealVector> newQueryEmbeddings;
    private static Map<String, ArrayRealVector> docEmbeddings = new HashMap<>();
    private static float alpha;
    private static float beta;
    private static float gamma;

    public PoolRocchio(Map<Integer, List<TopDocument>> initialResults, Map<Integer, ArrayRealVector> queryEmbeddings,
                       float alpha, float beta, float gamma) {
        this.initialResults = initialResults;
        this.queryEmbeddings = queryEmbeddings;
        this.alpha = alpha;
        this.beta = beta;
        this.gamma = gamma;
    }

    private class WorkerRocchio implements Runnable {
        private List<Integer> topicsID;
        private int workerID;
        private Map<Integer, ArrayRealVector> newWorkerQueryEmbeddings;
        private Map<Integer, List<TopDocument>> newWorkerResults;


        private WorkerRocchio(List<Integer> topicsID, int workerID) {
            this.topicsID = topicsID;
            this.workerID = workerID;
            this.newWorkerQueryEmbeddings = new HashMap<>();
            this.newWorkerResults = new HashMap<>();
        }

        @Override
        public void run() {
            for (int topicID : topicsID) {
                // Obtain new queries
                System.out.println("Worker " + workerID + ": Computing Rocchio for topic " + topicID);
                ArrayRealVector newQuery = computeRocchio(queryEmbeddings.get(topicID), initialResults.get(topicID));
                newWorkerQueryEmbeddings.put(topicID, newQuery);
            }

            // Compute again cosine similarities
            for (int topicID : topicsID) {
                System.out.println("Worker " + workerID + ": Computing again cosine similarities for topic " + topicID);
                List<TopDocument> topDocuments = new ArrayList<>();

                // Compute cosine similarities for all documents en store them in topDocuments list
                for (String docID : docEmbeddings.keySet()) {
                    ArrayRealVector docEmbedding = docEmbeddings.get(docID);
                    double sim = PoolCosineSimilarity.cosineSimilarity(docEmbedding, newWorkerQueryEmbeddings.get(topicID));
                    topDocuments.add(new TopDocument(docID, sim, topicID));
                }

                // Order topDocuments list by score
                Collections.sort(topDocuments, new TopDocumentOrder());

                // Save them in Map object
                newWorkerResults.put(topicID, topDocuments);
            }
        }


        private ArrayRealVector computeRocchio(ArrayRealVector queryEmbedding, List<TopDocument> relevant) {
            // Obtain only docIDs of relevant documents
            List<String> relevantDocs = relevant.stream().map(doc -> doc.cordID()).toList();

            // Read document embeddings file to compute the new query

            // relevantSum = vector sum of relevant documents, nonRelevantSum = vector sum of non relevant documents
            // relevantCount = number of relevant documents, nonRelevantCount = number of non relevant documents
            ArrayRealVector relevantSum = new ArrayRealVector(CollectionReader.EMBEDDINGS_DIMENSIONALITY);
            ArrayRealVector nonRelevantSum = new ArrayRealVector(CollectionReader.EMBEDDINGS_DIMENSIONALITY);
            int relevantCount = 0;
            int nonRelevantCount = 0;

            // Iterate over the embeddings document (is preferable storing similarities, not embeddings vectors)
            for (String docID : docEmbeddings.keySet()) {
                ArrayRealVector docEmbedding = docEmbeddings.get(docID);
                if (relevantDocs.contains(docID)) {
                    relevantSum = relevantSum.add(docEmbedding);
                    relevantCount++;
                } else {
                    nonRelevantSum = nonRelevantSum.add(docEmbedding);
                    nonRelevantCount++;
                }
            }

            // Compute the new query
            ArrayRealVector newQueryEmbedding = (ArrayRealVector) queryEmbedding.mapMultiply((double)alpha);
            newQueryEmbedding = newQueryEmbedding.add(relevantSum.mapMultiply((double)(beta/relevantCount)));
            newQueryEmbedding = newQueryEmbedding.subtract(nonRelevantSum.mapMultiply((double)(gamma/nonRelevantCount)));

            return newQueryEmbedding;
        }

        private Map<Integer, ArrayRealVector> newQueries() {
            return newWorkerQueryEmbeddings;
        }

        private Map<Integer, List<TopDocument>> newSimilarities() {
            return newWorkerResults;
        }


    }

    public void launch() {
        System.out.println("Computing Rocchio with alpha=" + alpha + ", beta=" + beta + ", gamma=" + gamma);
        // Read all documents embeddings
        docEmbeddings = readDocEmbeddings();

        final int numCores = Runtime.getRuntime().availableProcessors();
        System.out.println("Computing Rocchio Similarity with " + numCores + " cores");
        List<Integer> topics = queryEmbeddings.keySet().stream().toList();
        Integer[] workersDivision = coalesce(numCores, topics.size());
        ExecutorService executor = Executors.newFixedThreadPool(numCores);
        List<WorkerRocchio> workers = new ArrayList<>();


        for (int i = 0; i < numCores; i++) {
            int start = workersDivision[i];
            int end = workersDivision[i+1];
            List<Integer> topicsSlice = topics.subList(start, end);
            System.out.println("Thread " + i + " is computing topics from " + start + " to " + end);
            WorkerRocchio worker = new WorkerRocchio(topicsSlice, i);
            executor.execute(worker);
            workers.add(worker);
        }

        // End the executor
        executor.shutdown();
        try {
            executor.awaitTermination(1, TimeUnit.HOURS);
        } catch (final InterruptedException e) {
            e.printStackTrace();
            System.exit(-2);
        }

        newQueryEmbeddings = new HashMap<>();
        newResults = new HashMap<>();
        for (WorkerRocchio worker : workers) {
            newQueryEmbeddings.putAll(worker.newQueries());
            newResults.putAll(worker.newSimilarities());
        }

    }

    public Map<Integer, ArrayRealVector> getNewQueryEmbeddings() {
        return newQueryEmbeddings;
    }

    public Map<Integer, List<TopDocument>> getNewSimilarities() {
        return newResults;
    }

    public Integer[] coalesce(int numWorkers, int N) {
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

}
