package es.udc.fi.irdatos.c2122.cords;

import es.udc.fi.irdatos.c2122.schemas.TopDocument;
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

import static es.udc.fi.irdatos.c2122.cords.CollectionReader.streamDocEmbeddings;

public class PoolRocchio {
    private static Map<Integer, List<TopDocument>> initialResults;
    private static Map<Integer, ArrayRealVector> queryEmbeddings;
    private static float alpha = 1F;
    private static float beta = 0.75F;
    private static float gamma = 0.15F;

    public PoolRocchio(Map<Integer, List<TopDocument>> initialResults, Map<Integer, ArrayRealVector> queryEmbeddings) {
        this.initialResults = initialResults;
        this.queryEmbeddings = queryEmbeddings;
    }

    private class WorkerRocchio implements Runnable {
        private List<Integer> topicsID;
        private int workerID;
        private Map<Integer, ArrayRealVector> newQueryEmbeddings;


        private WorkerRocchio(List<Integer> topicsID, int workerID) {
            this.topicsID = topicsID;
            this.workerID = workerID;
            this.newQueryEmbeddings = new HashMap<>();
        }

        @Override
        public void run() {
            for (int topicID : topicsID) {
                ArrayRealVector newQuery = newQueryRocchio(queryEmbeddings.get(topicID), initialResults.get(topicID),
                        alpha, beta, gamma);
                newQueryEmbeddings.put(topicID, newQuery);
            }
        }

        private Map<Integer, ArrayRealVector> result() {
            return newQueryEmbeddings;
        }
    }

    private ArrayRealVector newQueryRocchio(ArrayRealVector queryEmbedding, List<TopDocument> relevant, float alpha, float beta, float gamma) {
        // Obtain only docIDs of relevant documents
        List<String> relevantDocs = relevant.stream().map(doc -> doc.docID()).toList();

        // Read document embeddings file to compute the sum
        Stream<String> docEmbeddingsStream = streamDocEmbeddings();
        ArrayRealVector relevantSum = new ArrayRealVector(CollectionReader.EMBEDDINGS_DIMENSIONALITY);
        ArrayRealVector nonRelevantSum = new ArrayRealVector(CollectionReader.EMBEDDINGS_DIMENSIONALITY);
        int relevantCount = 0;
        int nonRelevantCount = 0;

        for (Iterator<String> it = docEmbeddingsStream.iterator(); it.hasNext(); ) {
            String line = it.next();
            String[] lineContent = line.split(",");
            String docID = lineContent[0];
            lineContent = Arrays.copyOfRange(lineContent, 1, lineContent.length);
            ArrayRealVector docEmbedding = new ArrayRealVector(Arrays.stream(lineContent).mapToDouble(Double::parseDouble).toArray());

            if (relevantDocs.contains(docID)) {
                relevantSum = relevantSum.add(docEmbedding);
                relevantCount++;
            } else {
                nonRelevantSum = nonRelevantSum.add(docEmbedding);
                nonRelevantCount++;
            }
        }

        ArrayRealVector newQueryEmbedding = (ArrayRealVector) queryEmbedding.mapMultiply((double)alpha);
        newQueryEmbedding = newQueryEmbedding.add(relevantSum.mapMultiply((double)(beta/relevantCount)));
        newQueryEmbedding = newQueryEmbedding.subtract(nonRelevantSum.mapMultiply((double)(gamma/nonRelevantCount)));

        return newQueryEmbedding;
    }

    public Map<Integer, ArrayRealVector> computeRocchio() {
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
            executor.awaitTermination(10, TimeUnit.MINUTES);
        } catch (final InterruptedException e) {
            e.printStackTrace();
            System.exit(-2);
        }

        Map<Integer, ArrayRealVector> newQueryEmbeddings = new HashMap<>();
        for (WorkerRocchio worker : workers) {
            Map<Integer, ArrayRealVector> result = worker.result();
            newQueryEmbeddings.putAll(result);
        }
        return newQueryEmbeddings;

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
