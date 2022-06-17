package es.udc.fi.irdatos.c2122.cords;


import com.ctc.wstx.exc.WstxOutputException;
import es.udc.fi.irdatos.c2122.schemas.Article;
import es.udc.fi.irdatos.c2122.schemas.Metadata;
import es.udc.fi.irdatos.c2122.schemas.TopDocument;
import es.udc.fi.irdatos.c2122.schemas.TopDocumentOrder;
import org.apache.commons.io.FileUtils;
import org.apache.commons.math3.exception.OutOfRangeException;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;
import org.apache.commons.math3.util.MathUtils;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.*;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.LockObtainFailedException;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Array;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static es.udc.fi.irdatos.c2122.cords.AuxiliarFunctions.coalesce;
import static es.udc.fi.irdatos.c2122.cords.CollectionReader.readMetadata;
import static es.udc.fi.irdatos.c2122.cords.CollectionReader.streamDocEmbeddings;

public class ObtainTransitionMatrix {
    double alpha;
    int iterations;
    Map<Integer, List<TopDocument>> initialResults;
    List<Metadata> metadata;
    IndexSearcher isearcher;
    IndexReader ireader;


    public ObtainTransitionMatrix(IndexSearcher isearcher, IndexReader ireader,
            Map<Integer, List<TopDocument>> initialResults, double alpha, int iterations) {
        this.alpha = alpha;
        this.iterations = iterations;
        this.initialResults = initialResults;
        this.metadata = readMetadata();
        this.isearcher = isearcher;
        this.ireader = ireader;
    }

    private class WorkerPageRank implements Runnable {
        List<Integer> topicsSlice;
        Map<Integer, List<TopDocument>> newResultsSlice;
        int workerID;

        private WorkerPageRank(List<Integer> topicsSlice, int workerID) {
            this.topicsSlice = topicsSlice;
            this.workerID = workerID;
            this.newResultsSlice = new HashMap<>();
        }


        @Override
        public void run() {
            for (int topicID : topicsSlice) {
                List<TopDocument> initialResultsTopic = initialResults.get(topicID);
                Map<String, Integer> docs2index = new HashMap<>();
                for (int i = 0; i < initialResultsTopic.size(); i++) {
                    TopDocument topDocument = initialResultsTopic.get(i);
                    docs2index.put(topDocument.cordID(), i);
                }
                RealMatrix transitionMatrix = MatrixUtils.createRealMatrix(
                        initialResultsTopic.size(), initialResultsTopic.size());


                System.out.println("Worker " + workerID + " computing page rank for topic " + topicID);

                for (TopDocument topDocument : initialResultsTopic) {
                    if (!(new File(ReferencesIndexing.storingFolder + "/" + topDocument.cordID())).exists()) {
                        continue;
                    }
                    String[] references;
                    try {
                        references = new String(Files.readAllBytes(Paths.get(ReferencesIndexing.storingFolder, topDocument.cordID()))).split("\n");
                    } catch (IOException e) { e.printStackTrace(); return;}

                    for (String reference : references) {
                        if (reference.length() == 0) {
                            continue;
                        }
                        String refID = reference.split(" ")[0];
                        int count = Integer.parseInt(reference.split(" ")[1]);
                        if (docs2index.containsKey(refID)) {
                            transitionMatrix.setEntry(docs2index.get(topDocument.cordID()), docs2index.get(refID), count);
                        }
                    }
                }

                // Matrix normalization
                transitionMatrix = normalize(transitionMatrix);

                // Compute page rank
                ArrayRealVector pageRank = new ArrayRealVector(
                        initialResultsTopic.size(), (double) 1/ initialResultsTopic.size());
                ArrayRealVector transition;
                for (int i=0; i< iterations; i++) {
                    transition = (ArrayRealVector) transitionMatrix.preMultiply(pageRank);
                    if (pageRank.equals(transition)) {
                        break;
                    } else {
                        pageRank = transition;
                    }
                }

                // Compute new score for initial results
                List<TopDocument> newResultsTopics = new ArrayList<>();
                for (int i=0; i < initialResultsTopic.size(); i++) {
                    TopDocument initialDocument = initialResultsTopic.get(i);
                    double initialScore = initialDocument.score();
                    double newScore = pageRank.getEntry(docs2index.get(initialDocument.cordID())) * initialScore;
                    newResultsTopics.add(new TopDocument(initialDocument.cordID(), newScore, topicID));
                }
                Collections.sort(newResultsTopics, new TopDocumentOrder());
                newResultsSlice.put(topicID, newResultsTopics);
            }
        }

        public RealMatrix normalize(RealMatrix matrix) {
            int n = matrix.getRowDimension();
            for (int i=0; i < n; i++) {
                double normValue = matrix.getRowVector(i).getL1Norm();
                if (normValue != 0) {
                    matrix.setRowVector(i, matrix.getRowVector(i).mapMultiply(1/normValue));
                    matrix.setRowVector(i, matrix.getRowVector(i).mapMultiply(1-alpha));
                    matrix.setRowVector(i, matrix.getRowVector(i).mapAdd(alpha/n));
                } else {
                    RealVector newVector = matrix.getRowVector(i).mapAdd((double)1/n);
                    matrix.setRowVector(i, newVector);
                }
            }
            return matrix;
        }

        private Map<Integer, List<TopDocument>> result() {
            return newResultsSlice;
        }
    }

    public Map<Integer, List<TopDocument>> launch() {

        final int numCores = Runtime.getRuntime().availableProcessors();
        System.out.println("Computing Page Rank with " + numCores + " cores");
        List<Integer> topics = initialResults.keySet().stream().toList();
        Integer[] workersDivision = coalesce(numCores, topics.size());
        ExecutorService executor = Executors.newFixedThreadPool(numCores);
        List<WorkerPageRank> workers = new ArrayList<>();

        for (int i = 0; i < numCores; i++) {
            int start = workersDivision[i];
            int end = workersDivision[i+1];
            List<Integer> topicsSlice = topics.subList(start, end);
            System.out.println("Thread " + i + " is computing topics from " + start + " to " + end);
            WorkerPageRank worker = new WorkerPageRank(topicsSlice, i);
            executor.execute(worker);
            workers.add(worker);
        }

        // End the executor
        executor.shutdown();
        try {
            executor.awaitTermination(3, TimeUnit.HOURS);
        } catch (final InterruptedException e) {
            e.printStackTrace();
            System.exit(-2);
        }

        Map<Integer, List<TopDocument>> newResults = new HashMap<>();
        for (WorkerPageRank worker : workers) {
            newResults.putAll(worker.result());
        }
        return newResults;
    }
}
