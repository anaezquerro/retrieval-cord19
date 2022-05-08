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

        private Map<String, Integer> parseReferencesCount(Article article) {
            List<Article.Content> body_text = article.body_text();
            Map<String, Integer> referencesCount = new HashMap<>();
            for (Article.Content paragraph : body_text) {
                List<Article.Content.Cite> cites = paragraph.cite_spans();
                for (Article.Content.Cite cite : cites) {
                    if (!referencesCount.containsKey(cite.ref_id())) {
                        referencesCount.put(cite.ref_id(), 1);
                    } else {
                        int value = referencesCount.get(cite.ref_id());
                        referencesCount.put(cite.ref_id(), value + 1);
                    }
                }
            }
            Map<String, Article.Reference> bib_entries = article.bib_entries();
            for (String bib_entry : bib_entries.keySet()) {
                if (!referencesCount.containsKey(bib_entry)) {
                    referencesCount.put(bib_entry, 1);
                } else {
                    int value = referencesCount.get(bib_entry);
                    referencesCount.put(bib_entry, value + 1);
                }
            }
            return referencesCount;
        }

        public static String parse(String text) {
            String parsedText = text.replaceAll("\\[|\\]|\\(|\\)|/|-|\\'|\\:|\\\\|\"|\\}|\\{|\\*|\\?|\\!|\\^|\\~|\\+|\\;", " ");
            parsedText = parsedText.replaceAll("and|or|the|at|of|a|in|OR|AND", "");
            return parsedText;
        }

        @Override
        public void run() {
            for (int topicID : topicsSlice) {
                List<TopDocument> initialResultsTopic = initialResults.get(topicID);
                Map<String, Integer> docs2index = new HashMap<>();
                for (int i = 0; i < initialResultsTopic.size(); i++) {
                    TopDocument topDocument = initialResultsTopic.get(i);
                    docs2index.put(topDocument.docID(), i);
                }
                RealMatrix transitionMatrix = MatrixUtils.createRealMatrix(
                        initialResultsTopic.size(), initialResultsTopic.size());


                System.out.println("Worker " + workerID + " computing page rank for topic " + topicID);

                // Explore metadata rows to search article results for the topic
                for (Metadata rowMetadata : metadata) {
                    if (!docs2index.keySet().contains(rowMetadata.cordUid())) {
                        continue;
                    }

                    // Read the article of the path
                    Article article;
                    try {
                        if (rowMetadata.pmcFile().length() != 0) {
                            article = CollectionReader.ARTICLE_READER.readValue(CollectionReader.DEFAULT_COLLECTION_PATH
                                    .resolve(rowMetadata.pmcFile()).toFile());
                        } else if (rowMetadata.pdfFiles().size() != 0) {
                            article = CollectionReader.ARTICLE_READER.readValue(CollectionReader.DEFAULT_COLLECTION_PATH
                                    .resolve(rowMetadata.pdfFiles().get(0)).toFile());
                        } else {
                            continue;
                        }
                    } catch (IOException e) {
                        System.out.println("IOException while reading JSON file " + rowMetadata.pmcFile());
                        e.printStackTrace();
                        return;
                    }

                    // Now search in references and get their counts
                    Map<String, Article.Reference> references = article.bib_entries();
                    if (references.size() == 0) {
                        continue;
                    }

                    Map<String, Integer> referencesCount = parseReferencesCount(article);

                    for (Map.Entry<String, Article.Reference> reference : references.entrySet()) {
                        String parsedTitle = parse(reference.getValue().title());
                        List<String> authors = reference.getValue().authors().stream().map(x -> parse(x.last())).toList();
                        if ((parsedTitle.length() == 0) ||
                                (authors.size() == 0) ||
                                (String.join(" ", authors).strip().length() == 0)) {
                            continue;
                        }

                        BooleanQuery.Builder booleanQueryBuilder = new BooleanQuery.Builder();

                        // Search references title in the index
                        QueryParser parser = new QueryParser("title", new StandardAnalyzer());
                        Query query;
                        try {
                            query = parser.parse(parsedTitle);
                        } catch (ParseException e) {
                            System.out.println("ParseException while constructing the query for reference " +
                                    "in article " + rowMetadata.cordUid() + ": " + parsedTitle);
                            System.out.println(reference.getValue().title());
                            e.printStackTrace();
                            return;
                        }

                        booleanQueryBuilder.add(query, BooleanClause.Occur.MUST);

                        // Query for reference authors
                        QueryParser parserAuthor = new QueryParser("authors", new StandardAnalyzer());
                        Query queryAuthor;
                        try {
                            queryAuthor = parserAuthor.parse(String.join(" ", authors));
                        } catch (ParseException e) {
                            e.printStackTrace();
                            System.out.println("ParseException while constructing the query for reference author " +
                                    "in article " + rowMetadata.cordUid() + ": " + authors);
                            return;
                        }
                        booleanQueryBuilder.add(queryAuthor, BooleanClause.Occur.MUST);

                        // Build query and execute
                        BooleanQuery booleanQuery = booleanQueryBuilder.build();

                        // Make the query
                        TopDocs topDocs;
                        try {
                            topDocs = isearcher.search(booleanQuery, 1);
                        } catch (IOException e) {
                            e.printStackTrace();
                            return;
                        }

                        for (int i = 0; i < Math.min(topDocs.scoreDocs.length, topDocs.totalHits.value); i++) {
                            try {
                                String docID = ireader.document(topDocs.scoreDocs[i].doc).get("docID");
                                String title = ireader.document(topDocs.scoreDocs[i].doc).get("title");
                                List<Boolean> coincidences = Arrays.stream(parse(title).split(" ")).map(x -> parsedTitle.contains(x))
                                        .toList();
                                int mismatches = Collections.frequency(coincidences, false);
                                if (mismatches > 0.15*coincidences.size()) {
                                    continue;
                                }
                                if (docs2index.containsKey(docID)) {
                                    transitionMatrix.setEntry(
                                            docs2index.get(rowMetadata.cordUid()),
                                            docs2index.get(docID),
                                            referencesCount.get(reference.getKey())
                                    );
                                }
                            } catch (IOException e) {
                                e.printStackTrace();
                                return;
                            }
                        }
                    }
                }

                // Matrix normalization
                transitionMatrix = normalize(transitionMatrix);
                for (int i=0; i < transitionMatrix.getRowDimension(); i++) {
                    double norm = transitionMatrix.getRowVector(i).getL1Norm();
                }

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
                    double newScore = pageRank.getEntry(docs2index.get(initialDocument.docID())) * initialScore + initialScore;
                    newResultsTopics.add(new TopDocument(initialDocument.docID(), newScore, topicID));
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
            executor.awaitTermination(1, TimeUnit.HOURS);
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
