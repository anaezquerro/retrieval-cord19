package es.udc.fi.irdatos.c2122.cords;

import es.udc.fi.irdatos.c2122.schemas.Article;
import es.udc.fi.irdatos.c2122.schemas.Metadata;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.LMJelinekMercerSimilarity;
import org.apache.lucene.search.similarities.Similarity;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static es.udc.fi.irdatos.c2122.cords.AuxiliarFunctions.coalesce;
import static es.udc.fi.irdatos.c2122.cords.AuxiliarFunctions.deleteFolder;
import static es.udc.fi.irdatos.c2122.cords.CollectionReader.readMetadata;

public class SearchReferences {
    private static String REFERENCES_FOLDERNAME = "references";
    private static String REFERENCES_FILENAME = "references.txt";
    private static String INDEX_FOLDERNAME = "PageRankIndex";
    private static Similarity similarity = new BM25Similarity();

    private static class WorkerSearcher implements Runnable {
        private List<String> linesWrite;
        private List<Metadata> metadataSlice;
        private IndexSearcher wsearcher;
        private IndexReader wreader;
        private int workerID;

        public WorkerSearcher(List<Metadata> metadata, IndexSearcher wsearcher, IndexReader wreader, int workerID) {
            this.metadataSlice = metadata;
            this.linesWrite = new ArrayList<>();
            this.wsearcher = wsearcher;
            this.wreader = wreader;
            this.workerID = workerID;
        }

        public static String parse(String text) {
            String parsedText = text.replaceAll("\\[|\\]|\\(|\\)|/|-|\\'|\\:|\\\\|\"|\\}|\\{|\\*|\\?|\\!|\\^|\\~|\\+|\\;", " ");
            parsedText = parsedText.replaceAll("and|or|the|at|of|a|in|OR|AND", "");
            return parsedText;
        }

        @Override
        public void run() {
            // Create file in which store references
            String filename = "results " + workerID + ".txt";

            FileWriter writer;
            try {writer = new FileWriter(Paths.get(REFERENCES_FOLDERNAME).resolve(filename).toFile()); } catch (Exception e) {e.printStackTrace();return;}

            for (Metadata rowMetadata : metadataSlice) {
                // 1. Read article
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

                // 2. Write article ID in file
                try { writer.write(rowMetadata.cordUid());} catch (IOException e) {
                    e.printStackTrace();return;}

                // Obtain article references and search them in the index
                Map<String, Article.Reference> references = article.bib_entries();
                for (Article.Reference reference : references.values()) {
                    if (reference.title().length() == 0) {
                        continue;
                    }
                    BooleanQuery.Builder booleanQueryBuilder = new BooleanQuery.Builder();

                    // QueryParser for reference title
                    QueryParser parser = new QueryParser("title", new StandardAnalyzer());
                    Query query;
                    String parsedTitle = null;
                    try {
                        parsedTitle = parse(reference.title());
                        if (parsedTitle.strip().length() == 0) {
                            continue;
                        }
                        query = parser.parse(parsedTitle);
                    } catch (ParseException e) {
                        System.out.println("ParseException while constructing the query for reference " +
                                "in article " + rowMetadata.cordUid() + ": " + parsedTitle);
                        System.out.println(reference.title());
                        e.printStackTrace();
                        return;
                    }


                    booleanQueryBuilder.add(query, BooleanClause.Occur.SHOULD);

                    // Query for reference authors
                    for (int i = 0; i < Math.min(reference.authors().size(), 5); i++) {
                        Article.Author author = reference.authors().get(i);
                        if (author.last().length() == 0) {
                            continue;
                        }
                        QueryParser parserAuthor = new QueryParser("authors", new StandardAnalyzer());
                        Query queryAuthor;
                        String parsedAuthor = null;
                        try {
                            parsedAuthor = parse(author.last());
                            if (parsedAuthor.strip().length() == 0) {
                                continue;
                            }
                            queryAuthor = parserAuthor.parse(parsedAuthor);
                        } catch (ParseException e) {
                            e.printStackTrace();
                            System.out.println("ParseException while constructing the query for reference author " +
                                    "in article " + rowMetadata.cordUid() + ": " + parsedAuthor);
                            System.out.println(author.last());
                            return;
                        }
                        booleanQueryBuilder.add(queryAuthor, BooleanClause.Occur.SHOULD);
                    }


                    // Build query and execute
                    BooleanQuery booleanQuery = booleanQueryBuilder.build();

                    // Make the query
                    TopDocs topDocs;
                    try { topDocs = wsearcher.search(booleanQuery, 3);}
                    catch (IOException e) {e.printStackTrace();return;}

                    // Save results
                    for (int i = 0; i < Math.min(topDocs.scoreDocs.length, topDocs.totalHits.value); i++) {
                        try {
                            writer.write(", ");
                            writer.write(wreader.document(topDocs.scoreDocs[i].doc).get("docID"));
                        } catch (IOException e) {e.printStackTrace();return;}
                    }
                }
                try {writer.write("\n");} catch (IOException e) {e.printStackTrace();return;}
            }
            System.out.println("Worker " + workerID + "has ended");
        }

    }

    private static void queryReferences() {
        deleteFolder(REFERENCES_FOLDERNAME);
        new File(REFERENCES_FOLDERNAME).mkdirs();
        // Create IndexSearcher and IndexReader
        ReaderSearcher creation = new ReaderSearcher(Paths.get(INDEX_FOLDERNAME), new BM25Similarity());
        IndexReader ireader = creation.reader();
        IndexSearcher isearcher = creation.searcher();


        // Loop for each document and its references
        List<Metadata> metadata = readMetadata();
        int numCores = Runtime.getRuntime().availableProcessors();
        System.out.println("Computing Rocchio Similarity with " + numCores + " cores");
        Integer[] workersDivision = coalesce(numCores, metadata.size());
        ExecutorService executor = Executors.newFixedThreadPool(numCores);

        for (int i = 0; i < numCores; i++) {
            int start = workersDivision[i];
            int end = workersDivision[i+1];
            List<Metadata> metadataSlice = metadata.subList(start, end);
            System.out.println("Thread " + i + " is computing topics from " + start + " to " + end);
            WorkerSearcher worker = new WorkerSearcher(metadataSlice, isearcher, ireader, i);
            executor.execute(worker);
        }

        // End the executor
        executor.shutdown();
        try {
            executor.awaitTermination(2, TimeUnit.HOURS);
        } catch (final InterruptedException e) {
            e.printStackTrace();
            System.exit(-2);
        }
    }

    public static void mergeResults(int numCores) {
        FileWriter mergeWriter;
        try {mergeWriter = new FileWriter(Paths.get(REFERENCES_FOLDERNAME).resolve(REFERENCES_FILENAME).toFile()); }
        catch (Exception e) {e.printStackTrace();return;}

        // Write results
        for (int i = 0; i < numCores; i++) {
            String filename = "results " + i + ".txt";

            Stream<String> stream = null;
            try {
                stream = Files.lines(Paths.get(REFERENCES_FOLDERNAME).resolve(filename));
            } catch (IOException e) {
                e.printStackTrace();
                return;
            }

            stream.forEach(line -> {
                try {
                    mergeWriter.write(line);
                    mergeWriter.write("\n");
                } catch (Exception e) {
                    e.printStackTrace();
                    return;
                }
            });
        }
        try {
            mergeWriter.close();
        } catch (IOException e) {
            System.out.println("IOException while closing the txt writer");
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
//        queryReferences();
        mergeResults(8);
    }


}
