package cords;

import lucene.IdxReader;
import lucene.IdxSearcher;
import lucene.IdxWriter;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
import schemas.*;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static util.AuxiliarFunctions.*;
import static cords.PoolIndexing.INDEX_FOLDERNAME;


/**
 * Computes PageRank of the complete collection using the indexed title and authors references of each article.
 *
 * Check notation!
 * In order to represent the directional relations between documents, consider this notation:
 * oref -> tref               o = original, t = target
 *
 * orefVec  : Inverse references vector of a set vector. It gives info about which docs point the set vector.
 * trefVec  : References vector of a set vector. It gives info about which docs are pointed by the set vector.
 *
 * key: Please understand these definitions in order to clearly read the code.
 */
public class PageRank {
    /* Global variables (lucene objects)
    iwriter    [IdxWriter]      : Friendly-user implementation of the Apache Lucene IndexWriter class.
    ireader    [IdxReader]      : Friendly-user implementation of the Apache Lucene IndexReader class.
    isearcher  [IdxSearcher]    : Friendly-user implementation of the Apache Lucene IndexSearcher class.
     */
    private IdxWriter iwriter;
    private IdxReader ireader;
    private IdxSearcher isearcher;

    /* Global variables (paths)
    SAVE_INDEX_FOLDERNAME    [String]   : Path where a save copy of the Apache Lucene index is stored (in case of errors).
    TEMP_INDEX_FOLDERNAME    [String]   : Path where we store a temporary copy of the index that is being read.
     */
    private final String TEMP_PREFFIX = "temp";
    private final String SAVE_PREFFIX = "save";
    private final String SAVE_INDEX_FOLDERNAME = SAVE_PREFFIX + INDEX_FOLDERNAME;
    private final String TEMP_INDEX_FOLDERNAME = TEMP_PREFFIX + INDEX_FOLDERNAME;


    /* Global variables:
    VECTOR_ITEM_SEP   [String]              : String separator used to convert a vector to a string sequence.
    countPageRank     [ArrayRealVector]     : PageRank vector considering the references count.
    binaryPageRank    [ArrayRealVector]     : PageRank vector considering only binary references.
     */
    public static String VECTOR_ITEM_SEP = " ";
    private ArrayRealVector countPageRank;
    private ArrayRealVector binaryPageRank;

    /*
    Vectors notation to store them as String.
    trefCNVec           : Count (C) normalized (N) references vector (t).
    trefBNVec           : Binary (B) normalized (B) references vector (t).
    orefCNVec           : Count (C) normalized (N) inverse references vector (o).
    orefBNvec           : Binary (B) normalized (N) inverse references vector (o).
     */

    /*
    PageRank computing parameters:
    m          [int]   : Number of topDocs obtained in references searching that are used to create a match between a bib entry and a doc.
    iterations [int]   : Number of iterations of PageRank.
    alpha      [float] : PageRank parameter that defines the random probability.
     */
    int m = 2;
    private int iterations = 100;
    public static float alpha = 0.1F;
    private int numCoresInvert = 8;
    private int nbatchesInvert = 16;


    /**
     * Implements the process of searching a bibliography entry in the index to create a match between a bib entry and
     * a document of the index. This match results in a reference between the original doc that contains such entry and
     * the doc obtained from the retrieval ranking.
     */
    private class WorkerSearch implements Runnable {
        private int start;
        private int end;
        private int workerID;

        public WorkerSearch(int start, int end, int workerID) {
            this.start = start;
            this.end = end;
            this.workerID = workerID;
        }

        @Override
        public void run() {
            long tstart = System.currentTimeMillis();

            for (int docID = start; docID < end; docID++) {
                if (Math.floorMod(docID, 1000) == 0) {
                    System.out.println(workerID + ": is searching for matches in docID=" + docID);
                }

                CompressedRefsVector trefVec = new CompressedRefsVector(ireader.numDocs());
                ReferencesVector trefNormVec;
                Document doc = ireader.document(docID);

                if (!Objects.isNull(doc.get("trefVec"))) {
                    continue;
                }

                if (doc.get("references").length() > 0) {
                    List<String> trefs = Arrays.stream(doc.get("references").split(ParsedArticle.REFERENCES_SEPARATOOR)).toList();

                    // 2. Obtain article references and search them in the index
                    for (String tref : trefs) {
                        String[] trefItems = tref.split(ParsedArticle.ParsedReference.ITEM_REFS_SEPARATOR);
                        String trefTitle = trefItems[0];
                        String trefAuthors = trefItems[1];
                        int trefCount = Integer.parseInt(trefItems[2]);

                        // construct boolean query
                        BooleanQuery.Builder booleanQueryBuilder = new BooleanQuery.Builder();

                        // construct query parser for the title and authors of the tref
                        QueryParser parserTitle = new QueryParser("title", new StandardAnalyzer());
                        QueryParser parserAuthors = new QueryParser("authors", new StandardAnalyzer());
                        Query queryTitle;
                        Query queryAuthor;
                        try {
                            queryTitle = parserTitle.parse(trefTitle);
                            queryAuthor = parserAuthors.parse(trefAuthors);
                            booleanQueryBuilder.add(queryTitle, BooleanClause.Occur.SHOULD);
                            booleanQueryBuilder.add(queryAuthor, BooleanClause.Occur.SHOULD);
                        } catch (ParseException e) {
                            e.printStackTrace();
                            System.exit(-1);
                        }

                        // build the query and execute
                        BooleanQuery booleanQuery = booleanQueryBuilder.build();
                        TopDocs topDocs = isearcher.search(booleanQuery, 100);
                        int j = 0;
                        for (int i = 0; (i < Math.min(topDocs.scoreDocs.length, topDocs.totalHits.value)) && (j < m); i++) {
                            Document matchDoc = ireader.document(topDocs.scoreDocs[i].doc);
                            List<String> matchTitleWords = Arrays.stream(matchDoc.get("title").split(" ")).distinct().toList();

                            // number of words from doc that are not in ref
                            int mismatches = (int) matchTitleWords.stream().filter(x -> !trefTitle.contains(x)).count();
                            if (mismatches > 0.1 * matchTitleWords.size()) {
                                continue;
                            }

                            // number of words from ref that are not in doc
                            mismatches = (int) Arrays.stream(trefTitle.split("\\s+"))
                                    .filter(x -> !matchTitleWords.contains(x)).count();
                            if (mismatches > 0.1 * trefTitle.split("\\s+").length) {
                                continue;
                            }

                            // update vector entry and add cordID to the list accumulator
                            int matchCordID = Integer.parseInt(matchDoc.get("cordID"));
                            trefVec.add(matchCordID, trefCount);
                            j++;
                        }
                    }
                }

                // normalize the binary and count vectors
                trefNormVec = trefVec.toReferencesVector(true);

                // add this 3 vectors in string format to the Apache Lucene Index
                doc.add(new StoredField("trefVec", vector2string(trefVec.toCountVector(), VECTOR_ITEM_SEP)));
                doc.add(new StoredField("trefCNVec", trefNormVec.count2string()));
                doc.add(new StoredField("trefBNVec", trefNormVec.binary2string()));
                iwriter.addDocument(doc);
            }
            long tend = System.currentTimeMillis();
            System.out.println("WorkerSearch " + workerID + ": " + (tend-tstart));
        }

    }


    private class WorkerInverse implements Runnable {
        private int workerID;
        private int start;
        private int end;


        private WorkerInverse(int start, int end, int workerID) {
            this.workerID = workerID;
            this.start = start;
            this.end = end;
        }

        @Override
        public void run() {
            long tstart = System.currentTimeMillis();

            Map<Integer, ReferencesVector> orefVecs = new HashMap<>();
            IntStream.range(start, end).forEach(i -> {
                orefVecs.put(i, new ReferencesVector(ireader.numDocs()));
            });
            Map<Integer, Integer> cord2doc = new HashMap<>();

            for (int docID = 0; docID < ireader.numDocs(); docID++) {
                Document doc = ireader.document(docID);
                int ocordID = Integer.parseInt(doc.get("cordID"));
                if (start <= ocordID || ocordID < end) {
                    cord2doc.put(ocordID, docID);
                }
                ReferencesVector trefVec = new ReferencesVector(doc.get("trefBNVec"), doc.get("trefCNVec"));
                IntStream.range(start, end).forEach(
                        tcordID -> {
                            orefVecs.get(tcordID).setEntries(tcordID, trefVec.getEntries(tcordID));
                        }
                );
            }
            orefVecs.entrySet().stream().forEach(
                    entry -> {
                        Document doc = ireader.document(cord2doc.get(entry.getKey()));
                        doc.add(new StoredField("orefNCVec", orefVecs.get(entry.getKey()).count2string()));
                        doc.add(new StoredField("orefNBVec", orefVecs.get(entry.getKey()).binary2string()));
                        iwriter.addDocument(doc);
                    }
            );
            long tend = System.currentTimeMillis();
            System.out.println("WorkerInverse " + workerID + ": " + (tend-tstart));
        }
    }


    private ArrayRealVector updatePageRank(ArrayRealVector vectorPageRank, String fname) {
        for (int iter = 0; iter < iterations; iter++) {
            ArrayRealVector newVector = vectorPageRank.copy();
            ArrayRealVector oldVector = vectorPageRank.copy();
            IntStream.range(0, ireader.numDocs()).forEach(
                    docID -> {
                        Document doc = ireader.document(docID);
                        int cordID = Integer.parseInt(doc.get("cordID"));
                        newVector.setEntry(
                                cordID, oldVector.dotProduct(string2vector(doc.get(fname), VECTOR_ITEM_SEP))
                        );
                    }
            );
            if (newVector.equals(oldVector)) {
                break;
            } else {
                vectorPageRank = newVector.copy();
            }
        }
        return vectorPageRank;
    }

    private void computePageRank() {
        Map<String, ArrayRealVector> vectors = new HashMap<>() {{
            put("orefNBVec", binaryPageRank);
            put("orefNCVec", countPageRank);
        }};
        Map<String, ArrayRealVector> result = vectors.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> updatePageRank(entry.getValue(), entry.getKey())));
        binaryPageRank = result.get("orefNBVec");
        countPageRank = result.get("orefNCVec");

        for (int docID = 0; docID < ireader.numDocs(); docID++) {
            Document doc = ireader.document(docID);
            int cordID = Integer.parseInt(doc.get("cordID"));
            doc.add(new StoredField("binaryPageRank", binaryPageRank.getEntry(cordID)));
            doc.add(new StoredField("countPageRank", countPageRank.getEntry(cordID)));
            iwriter.addDocument(doc);
        }
    }


    private void searching() {

        int numCores = Runtime.getRuntime().availableProcessors();
        ExecutorService executor = Executors.newFixedThreadPool(numCores);

        duplicateFolder(INDEX_FOLDERNAME, SAVE_INDEX_FOLDERNAME);   // store a safe copy
        renameFolder(INDEX_FOLDERNAME, TEMP_INDEX_FOLDERNAME);

        iwriter = new IdxWriter(INDEX_FOLDERNAME);
        ireader = new IdxReader(TEMP_INDEX_FOLDERNAME);
        isearcher = new IdxSearcher(ireader);

        System.out.println("Applying PageRank searching over " + ireader.numDocs() + " docs with " + numCores + " cores");
        Integer[] workersDivision = coalesce(numCores, ireader.numDocs());

        for (int workerID=0; workerID < numCores; workerID++) {
            int start = workersDivision[workerID];
            int end = workersDivision[workerID + 1];
            System.out.println("Worker " + workerID + " is searching in docs " + start + " - " + end);
            WorkerSearch worker = new WorkerSearch(start, end, workerID);
            executor.execute(worker);
        }

        executor.shutdown();
        try {
            executor.awaitTermination(4, TimeUnit.HOURS);
        } catch (final InterruptedException e) {
            e.printStackTrace();
            System.exit(-2);
        }
        System.out.println("All tasks have finished successfully");

        // close IndexWriter and IndexReader
        iwriter.commit();
        iwriter.close();
        ireader.close();

        // remove temporary folder
        deleteFolder(TEMP_INDEX_FOLDERNAME);
        deleteFolder(SAVE_INDEX_FOLDERNAME);
    }

    public void inverting() {
        duplicateFolder(INDEX_FOLDERNAME, SAVE_INDEX_FOLDERNAME); // safe copy
        renameFolder(INDEX_FOLDERNAME, TEMP_INDEX_FOLDERNAME);
        iwriter = new IdxWriter(INDEX_FOLDERNAME);
        ireader = new IdxReader(TEMP_INDEX_FOLDERNAME);
        isearcher = new IdxSearcher(ireader);

        System.out.println("Applying PageRank inverting process over " + ireader.numDocs() +
                " docs with " + numCoresInvert + " in " + nbatchesInvert + " batches");

        Integer[] batchesDivision = coalesce(nbatchesInvert, ireader.numDocs());

        for (int batch=0; batch < nbatchesInvert; batch++) {
            System.out.println("Batch " + batch + " starting with docs " + batchesDivision[batch] + " - " + batchesDivision[batch+1]);

            ExecutorService executor = Executors.newFixedThreadPool(numCoresInvert);
            Integer[] workersDivision = coalesce(numCoresInvert, (batchesDivision[batch + 1] - batchesDivision[batch]));
            for (int workerID = 0; workerID < numCoresInvert; workerID++) {
                int start = workersDivision[workerID] + batchesDivision[batch];
                int end = workersDivision[workerID + 1] + batchesDivision[batch];
                System.out.println("Worker " + workerID + " is inverting references from " + start + " to " + end);
                WorkerInverse worker = new WorkerInverse(start, end, workerID);
                executor.execute(worker);
            }
            executor.shutdown();
            try {
                executor.awaitTermination(4, TimeUnit.HOURS);
            } catch (final InterruptedException e) {
                e.printStackTrace();
                System.exit(-2);
            }
            System.out.println("Batch " + batch + " has finished");
        }

        iwriter.commit();
        iwriter.close();
        ireader.close();
        deleteFolder(TEMP_INDEX_FOLDERNAME);
        deleteFolder(SAVE_INDEX_FOLDERNAME);
    }


    public void pagerank() {
        duplicateFolder(INDEX_FOLDERNAME, SAVE_INDEX_FOLDERNAME); // safe copy
        renameFolder(INDEX_FOLDERNAME, TEMP_INDEX_FOLDERNAME);
        iwriter = new IdxWriter(INDEX_FOLDERNAME);
        ireader = new IdxReader(TEMP_INDEX_FOLDERNAME);
        isearcher = null;
        binaryPageRank = new ArrayRealVector(ireader.numDocs(), (double) 1 / ireader.numDocs());
        countPageRank = new ArrayRealVector(ireader.numDocs(), (double) 1 / ireader.numDocs());

        long tstart = System.currentTimeMillis();
        computePageRank();
        long tend = System.currentTimeMillis();
        System.out.println("PageRank computing time: " + (tend-tstart));

        iwriter.commit();
        iwriter.close();
        ireader.close();
        deleteFolder(TEMP_INDEX_FOLDERNAME);
        deleteFolder(SAVE_INDEX_FOLDERNAME);
    }

    public void launch() {

        /**
         * -------- First stage SEARCHING --------
         * Compute searching of matches between references and documents.
         */
//        searching();

        /**
         * -------- Second stage INVERTING --------
         * Invert the references vector stored in REFS_INDEX_FOLDERNAME.
         */
//        inverting();


        /**
         * -------- Third stage PAGE RANK --------
         * Computes PageRank
         */
        pagerank();
    }


    public static void main(String[] args) {
        PageRank algorithm = new PageRank();
        algorithm.launch();
    }
}
