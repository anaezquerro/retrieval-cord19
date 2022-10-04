package es.udc.fi.irdatos.c2122.cords;

import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.*;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static es.udc.fi.irdatos.c2122.cords.AuxiliarFunctions.*;


/**
 * Computes PageRank of the complete collection using the indexed title and authors references of each article.
 */
public class PageRank {
    IndexWriter iwriter;
    IndexSearcher isearcher;
    IndexReader ireader;
    private boolean search;   // if it is true, assumes that references are stored in the referencesFolder
    private boolean show = true;
    private Map<String, Integer> cord2index = new HashMap<>();

    // folders to store information
    public static String referencesFolder = "references";
    public static String vectorsFolder = "referencesVector";

    // page rank parameters
    int m = 2;
    int iterations = 100;
    float alpha = 0.1F;


    public PageRank(IndexWriter iwriter, IndexReader ireader, IndexSearcher isearcher, boolean search) {
        this.iwriter = iwriter;
        this.isearcher = isearcher;
        this.ireader = ireader;
        this.search = search;
        if (search) {
            deleteCreateFolder(referencesFolder);
        }
        deleteCreateFolder(vectorsFolder);
    }

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
                if (show && Math.floorMod(docID, 500) == 0) {
                    System.out.println(workerID + ": is in docID=" + docID);
                }
                ArrayRealVector referencesVector = new ArrayRealVector(ireader.numDocs(), 0);
                ArrayRealVector referencesCountVector = new ArrayRealVector(ireader.numDocs(), 0);

                try {
                    // 1. Read document in the collection
                    Document doc = ireader.document(docID);

                    // consider this document has no references
                    if (doc.get("references").length() == 0) {
                        continue;
                    }

                    List<String> references = Arrays.stream(doc.get("references").split("\n")).toList();
                    List<String> newReferences = new ArrayList<>();
                    List<String> newReferencesID = new ArrayList<>();
                    List<String> newReferencesCount = new ArrayList<>();

                    // 2. Obtain article references and search them in the index
                    for (String reference : references) {
                        String[] refItems = reference.split("\t");
                        String refTitle = refItems[0];
                        String refAuthors = refItems[1];
                        String refCount = refItems[2];

                        // construct boolean query
                        BooleanQuery.Builder booleanQueryBuilder = new BooleanQuery.Builder();

                        // construct query parser for the title of the reference
                        QueryParser queryParser = new QueryParser("title", new StandardAnalyzer());
                        Query query = queryParser.parse(refTitle);
                        booleanQueryBuilder.add(query, BooleanClause.Occur.SHOULD);

                        // query for authors of the reference
                        QueryParser parserAuthors = new QueryParser("authors", new StandardAnalyzer());
                        Query queryAuthor = parserAuthors.parse(refAuthors);
                        booleanQueryBuilder.add(queryAuthor, BooleanClause.Occur.SHOULD);

                        // build the query and execute
                        BooleanQuery booleanQuery = booleanQueryBuilder.build();
                        List<String> docRefs = new ArrayList<>();
                        TopDocs topDocs = isearcher.search(booleanQuery, 100);
                        int j = 0;
                        for (int i = 0; (i < Math.min(topDocs.scoreDocs.length, topDocs.totalHits.value)) && (j<m); i++) {
                            int docRefID = topDocs.scoreDocs[i].doc;
                            List<String> docRefTitleWords = Arrays.stream(
                                    parse(ireader.document(docRefID).get("title")).split(" "))
                                    .distinct().toList();

                            // number of words from doc that are not in ref
                            int mismatches = (int) docRefTitleWords.stream()
                                    .filter(x -> !refTitle.contains(x)).count();
                            if (mismatches > 0.1*docRefTitleWords.size()) {continue;}

                            // number of words from ref that are not in doc
                            mismatches = (int) Arrays.stream(refTitle.split("\\s+"))
                                    .filter(x -> !docRefTitleWords.contains(x)).count();
                            if (mismatches > 0.1*refTitle.split("\\s+").length) {continue;}

                            // update vector entry and add cordID to the list accumulator
                            referencesVector.setEntry(docRefID, 1);
                            referencesCountVector.setEntry(docRefID, Double.parseDouble(refCount));
                            docRefs.add(ireader.document(docRefID).get("cordID"));
                            j++;
                        }

                        // update references list
                        if (docRefs.size() != 0) {
                            newReferences.add(reference + "\t" + String.join(" ", docRefs));
                            newReferencesID.addAll(docRefs);
                            String[] counts = new String[docRefs.size()];
                            Arrays.fill(counts, refCount);
                            newReferencesCount.addAll(Arrays.stream(counts).toList());
                        } else {
                            newReferences.add(reference);
                        }
                    }
                    if (referencesVector.getL1Norm() == 0.0) {
                        continue;
                    }
                    // normalize vectors and save them
                    referencesVector = normalize(referencesVector);
                    referencesCountVector = normalize(referencesCountVector);
                    saveVectors(referencesVector, referencesCountVector, String.valueOf(docID));

                    // save references in a file
                    FileWriter file = createFileWriter(referencesFolder + "/" + doc.get("cordID"));
                    file.write(String.join(" ", newReferencesID) + "\n");
                    file.write(String.join(" ", newReferencesCount));
                    file.close();

                    // add to the document new references
                    doc.removeField("references");
                    doc.add(new StoredField("references", String.join("\n", newReferences)));
                    iwriter.updateDocument(new Term("cordID", doc.get("cordID")), doc);
                }
                catch (IOException e) { e.printStackTrace(); return; }
                catch (ParseException e) {
                    System.out.println("ParseException while constructing the reference query of the " +
                            "document " + docID);
                    e.printStackTrace();
                    return;
                }
            }
            long tend = System.currentTimeMillis();
            System.out.println("worker" + workerID + " run time (in seconds): " + (tend-tstart)*0.001);
        }
    }

    private class WorkerPageRank implements Runnable {
        private int row;
        private String preffix;

        public WorkerPageRank(int row) {
            this.row = row;
            if (row == 0) {
                this.preffix = "ref";
            } else if (row == 1) {
                this.preffix = "count";
            }
        }

        private void transposeFiles() {
            System.out.println("Transposing files of preffix " + preffix);

            // 1. Create an array of file writers (columns)
            FileWriter[] files = new FileWriter[ireader.numDocs()];
            IntStream.range(0, ireader.numDocs())
                    .forEach(x -> {
                        files[x] = createFileWriter(vectorsFolder + "/" + preffix + x);
                    });

            // 2. For each document stored in `references` folder, add element at position i in files[i]
            for (int cordID = 0; cordID < ireader.numDocs(); cordID++) {
                System.out.println(preffix + ": " + cordID);
                String[] references;
                try {
                    if (!exists(vectorsFolder + "/" + cordID)) {
                        references = new String[ireader.numDocs()];
                        Arrays.fill(references, String.valueOf(1 / ireader.numDocs()));
                    } else {
                        references = new String(Files.readAllBytes(
                                Paths.get(vectorsFolder, String.valueOf(cordID)))).split("\n")[row].split(" ");
                    }
                    // write references
                    for (int refID = 0; refID < ireader.numDocs(); refID++) {
                        files[refID].write(references[refID] + " ");
                    }
                } catch (IOException e) {
                    System.out.println("IOException while reading file " + vectorsFolder + "/" + cordID);
                    e.printStackTrace();
                    return;
                }
            }
            Arrays.stream(files).forEach(file -> {
                try { file.close(); } catch (IOException e) {e.printStackTrace(); }
            });
            System.out.println(preffix + " has finished transposing");
        }

        private void computePageRank() {
            try {
                ArrayRealVector pageRank = new ArrayRealVector(ireader.numDocs(), (double) 1/ireader.numDocs());
                ArrayRealVector transition = new ArrayRealVector(ireader.numDocs());
                for (int iter = 0; iter < iterations; iter++) {
                    for (int irow = 0; irow < ireader.numDocs(); irow++) {  // iterate over matrix rows
                        // read references vector of the irow doc
                        String[] row = new String(Files.readAllBytes(Paths.get(referencesFolder, preffix + irow)))
                                .split(" ");
                        ArrayRealVector rowReference = new ArrayRealVector(Arrays.stream(row).
                                mapToDouble(x -> Double.parseDouble(x)).toArray());

                        // compute page rank
                        transition.setEntry(irow, pageRank.dotProduct(rowReference));
                    }
                    if (pageRank.equals(transition)) {break;}
                    else {pageRank = transition;}
                }

                // update page rank
                for (int docID = 0; docID < ireader.numDocs(); docID++) {
                    Document doc;
                    doc = ireader.document(docID);
                    doc.add(new StoredField("PageRank" + row, pageRank.getEntry(docID)));
                    iwriter.updateDocument(new Term("cordID", doc.get("cordID")), doc);
                }
            } catch (IOException e) {e.printStackTrace(); }
        }


        @Override
        public void run() {
            long tstart = System.currentTimeMillis();
            transposeFiles();
            long tend = System.currentTimeMillis();
            System.out.println("WorkerPageRank-" + row + " transposing runtime (in seconds): " + (tend-tstart)*0.001);

            tstart = System.currentTimeMillis();
            computePageRank();
            tend = System.currentTimeMillis();
            System.out.println("WorkerPageRank-" + row + " page rank runtime (in seconds): " + (tend-tstart)*0.001);
        }
    }

    private class WorkerWriter implements Runnable {
        String[] filesSlice;
        int workerID;

        public WorkerWriter(String[] filesSlice, int workerID) {
            this.filesSlice = filesSlice;
            this.workerID = workerID;
        }

        /**
         * Assumes references/ folder has all stored references per cordID and saves the corresponding vector.
         */
        @Override
        public void run() {
            for (String cordID : filesSlice) {
                try {
                    ArrayRealVector referencesVector = new ArrayRealVector(ireader.numDocs());
                    ArrayRealVector referencesVectorCount = new ArrayRealVector(ireader.numDocs());
                    String[] content = new String(
                            Files.readAllBytes(Paths.get(referencesFolder, cordID))).split("\n");
                    String[] references = content[0].split(" "); String[] referencesCount = content[1].split(" ");
                    for (int i=0; i < references.length; i++) {
                        referencesVector.setEntry(cord2index.get(references[i]), 1);
                        referencesVectorCount.setEntry(cord2index.get(references[i]), Double.parseDouble(referencesCount[i]));
                    }
                    saveVectors(normalize(referencesVector), normalize(referencesVectorCount), String.valueOf(cord2index.get(cordID)));
                } catch (IOException e) {e.printStackTrace(); return;}
            }
        }
    }

    public ArrayRealVector normalize(ArrayRealVector vector) {
        int n = vector.getDimension();
        double norm = vector.getL1Norm();
        vector.mapMultiplyToSelf(1/norm);
        vector.mapMultiplyToSelf(1-alpha);
        vector.mapMultiplyToSelf(alpha/n);
        return vector;
    }

    public void saveVectors(ArrayRealVector v1, ArrayRealVector v2, String filename) {
        FileWriter file = createFileWriter(vectorsFolder + "/" + filename);
        String[] s1 = new String[ireader.numDocs()];
        String[] s2 = new String[ireader.numDocs()];
        for (int docID = 0; docID < ireader.numDocs(); docID++) {
            s1[docID] = String.valueOf(v1.getEntry(docID));
            s2[docID] = String.valueOf(v2.getEntry(docID));
        }
        try {
            file.write(String.join(" ", s1));
            file.write("\n");
            file.write(String.join(" ", s2));
            file.close();
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("IOException while writing file " + vectorsFolder + "/" + filename);
        }
    }

    public void launch() {
        long tstart = System.currentTimeMillis();
        final int numCores = Runtime.getRuntime().availableProcessors();
        ExecutorService executor = Executors.newFixedThreadPool(numCores);

        if (search) {
            System.out.println("Indexing references with " + numCores + " cores");
            System.out.println("A total of " + ireader.numDocs() + " articles will be indexed");
            Integer[] workersDivision = coalesce(numCores, ireader.numDocs());

            for (int workerID = 0; workerID < numCores; workerID++) {
                int start = workersDivision[workerID];
                int end = workersDivision[workerID + 1];
                System.out.println("Thread " + workerID + " is indexing articles from " + start + " to " + end);
                WorkerSearch worker = new WorkerSearch(start, end, workerID);
                executor.execute(worker);
            }
        } else {
            IntStream.range(0, ireader.numDocs()).forEach(x -> {
                try {
                    Document doc = ireader.document(x);
                    cord2index.put(doc.get("cordID"), x);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
            String[] files = new File(referencesFolder).list();
            Integer[] workersDivision = coalesce(numCores, files.length);

            for (int workerID = 0; workerID < numCores; workerID++) {
                String[] filesSlice = Arrays.copyOfRange(files, workersDivision[workerID], workersDivision[workerID + 1]);
                System.out.println("Thread " + workerID + " is storing references vector of "
                        + filesSlice.length + " articles");
                WorkerWriter worker = new WorkerWriter(filesSlice, workerID);
                executor.execute(worker);
            }
        }
        executor.shutdown();
        try {
            executor.awaitTermination(4, TimeUnit.HOURS);
        } catch (final InterruptedException e) {
            e.printStackTrace();
            System.exit(-2);
        }
        long tend = System.currentTimeMillis();
        System.out.println("Total time for references indexing/storing: " + (tend-tstart)*0.001);


        // Compute Page Rank efficient algorithm in parallel (first worker will compute page rank with binary references
        // and the second worker will compute it with count references).
        tstart = System.currentTimeMillis();
        executor = Executors.newFixedThreadPool(2);

        for (int workerID = 0; workerID < 2; workerID++) {
            WorkerPageRank worker = new WorkerPageRank(workerID);
            executor.execute(worker);
        }
        executor.shutdown();
        try {
            executor.awaitTermination(4, TimeUnit.HOURS);
        } catch (final InterruptedException e) {
            e.printStackTrace();
            System.exit(-2);
        }
        tend = System.currentTimeMillis();
        System.out.println("Page Rank global time (in seconds): " + (tend-tstart)*0.001);

        // close the writer
        try {
            iwriter.commit();
            iwriter.close();
        } catch (CorruptIndexException e) {
            System.out.println("CorruptIndexException while closing the index writer");
            e.printStackTrace();
        } catch (IOException e) {
            System.out.println("IOException while closing the index writer");
            e.printStackTrace();
        }
    }


    public static void main(String[] args) {
        IndexWriter writer = createIndexWriter(PoolIndexing.INDEX_FOLDERNAME);
        ReaderSearcher objReaderSearcher = new ReaderSearcher(
                Paths.get(PoolIndexing.INDEX_FOLDERNAME),
                PoolIndexing.similarity);
        IndexSearcher searcher = objReaderSearcher.searcher();
        IndexReader reader = objReaderSearcher.reader();

        PageRank pool = new PageRank(writer, reader, searcher, false);
        pool.launch();

    }
}
