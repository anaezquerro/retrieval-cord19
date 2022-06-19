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
    int m = 2;
    public static String storingFolder = "references";
    int iterations = 100;
    float alpha = 0F;
    private boolean search;
    private boolean counting = false;


    public PageRank(IndexWriter iwriter, IndexReader ireader, IndexSearcher isearcher, boolean search) {
        this.iwriter = iwriter;
        this.isearcher = isearcher;
        this.ireader = ireader;
        this.search = search;
        if (search) {
            deleteCreateFolder(storingFolder);
        }
    }

    public PageRank(IndexWriter iwriter, IndexReader ireader, IndexSearcher isearcher, boolean search, boolean count) {
        this.iwriter = iwriter;
        this.isearcher = isearcher;
        this.ireader = ireader;
        this.search = search;
        if (search) {
            deleteCreateFolder(storingFolder);
        }
        this.counting = count;
    }

    private class WorkerReferences implements Runnable {
        private int start;
        private int end;
        private int workerID;

        public WorkerReferences(int start, int end, int workerID) {
            this.start = start;
            this.end = end;
            this.workerID = workerID;
        }

        @Override
        public void run() {
            for (int docID = start; docID < end; docID++) {
                if (Math.floorMod(docID, 500) == 0) {
                    System.out.println(workerID + ": is in docID=" + docID);
                }
                ArrayRealVector referencesVector = new ArrayRealVector(ireader.numDocs(), 0);

                try {
                    // 1. Read document in the collection
                    Document doc = ireader.document(docID);

                    // consider this document has no references
                    if (doc.get("references").length() == 0) {
                        continue;
                    }

                    List<String> references = Arrays.stream(doc.get("references").split("\n")).toList();
                    List<String> newReferences = new ArrayList<>();

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
                            if (counting) {referencesVector.setEntry(docRefID, Double.parseDouble(refCount)); }
                            else {referencesVector.setEntry(docRefID, 1);}
                            docRefs.add(ireader.document(docRefID).get("cordID") );
                            j++;
                        }

                        // update references list
                        if (docRefs.size() != 0) {
                            newReferences.add(reference + "\t" + String.join(", ", docRefs));
                        } else {
                            newReferences.add(reference);
                        }
                    }
                    // normalize referencesVector and save
                    referencesVector = normalize(referencesVector);
                    saveVector(referencesVector, String.valueOf(docID));

                    // add to the document the referencesVector
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
        }
    }

    /**
     * Saves vector elements in a text file where each element is separated with a space.
     * @param vector Vector to store.
     * @param filename File name where vector will be stored.
     */
    public void saveVector(ArrayRealVector vector, String filename) {
        FileWriter file = createFileWriter(storingFolder + "/" + filename);
        try {
            for (int i = 0; i < vector.getDimension(); i++) {
                file.write(vector.getEntry(i) + " ");
            }
            file.close();
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("IOException while writing file " + storingFolder + "/" + filename);
            return;
        }
    }

    /**
     * Computes vector normalization based on Page Rank algorithm with the given alpha (probability of random transition
     * between documents). This process is applied to each row of the transition matrix M.
     * @param vector Vector to be normalized.
     * @return Normalized vector.
     */
    public ArrayRealVector normalize(ArrayRealVector vector) {
        int n = vector.getDimension();
        double norm = vector.getL1Norm();
        if (norm != 0) {
            vector.mapMultiplyToSelf(1/norm);
            vector.mapMultiplyToSelf(1-alpha);
            vector.mapMultiplyToSelf(alpha/n);
        } else {
            vector.mapAddToSelf(1/n);
        }
        return vector;
    }


    public void launch() {
        if (search) {
            final int numCores = Runtime.getRuntime().availableProcessors();
            System.out.println("Indexing references with " + numCores + " cores");
            ExecutorService executor = Executors.newFixedThreadPool(numCores);
            System.out.println("A total of " + ireader.numDocs() + " articles will be indexed");
            Integer[] workersDivision = coalesce(numCores, ireader.numDocs());

            for (int i = 0; i < numCores; i++) {
                int start = workersDivision[i];
                int end = workersDivision[i+1];
                System.out.println("Thread " + i + " is indexing articles from " + start + " to " + end);
                WorkerReferences worker = new WorkerReferences(start, end, i);
                executor.execute(worker);
            }

            // end the executor
            executor.shutdown();
            try {
                executor.awaitTermination(4, TimeUnit.HOURS);
            } catch (final InterruptedException e) {
                e.printStackTrace();
                System.exit(-2);
            }
        }

        // compute Page Rank
        computePageRank();

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

    /**
     * Once launch() method has been computed, it is needed to "transpose" the content of all files (since
     * page rank vector is multiplied by columns of the transition matrix, not by rows).
     */
    private void transposeFiles() {
        // 1. Create an array of file writers (columns)
        FileWriter[] files = new FileWriter[ireader.numDocs()];
        IntStream.range(0, ireader.numDocs())
                .forEach(x -> {
                    files[x] = createFileWriter(storingFolder + "/ref" + x);
                });

        // 2. For each document stored in `references` folder, add element at position i in files[i]
        for (int cordID = 0; cordID < ireader.numDocs(); cordID++) {
            String[] references;
            try {
                if (!(new File(storingFolder + "/" + cordID).exists())) {
                    references = new String[ireader.numDocs()];
                    Arrays.fill(references, "0.0");
                } else {
                    String content = new String(Files.readAllBytes(Paths.get(storingFolder, String.valueOf(cordID))));
                    references = content.split(" ");
                }
                for (int refID = 0; refID < ireader.numDocs(); refID++) {
                    files[refID].write(references[refID]);
                }
            } catch (IOException e) {
                System.out.println("IOException while reading file " + storingFolder + "/" + cordID);
                e.printStackTrace();
                return;
            }
        }
        Arrays.stream(files).forEach(file -> {
            try { file.close(); } catch (IOException e) {e.printStackTrace(); }
        });
    }

    /**
     * Once all references have been parsed, it is possible to compute Page Rank with text files saved in the
     * storingFolder. Since Page Rank computation requires storing an N*N matrix (where N is the collection size),
     * instead of constructing a huge matrix this implementation iteratively reads text files stored in references/
     * (which corresponds to each row of the transition matrix) and calculates the dot product with the page rank
     * vector.
     * Page Rank of each document will be stored as a new field.
     */
    public void computePageRank() {
        transposeFiles();
        try {
            // compute PageRank
            ArrayRealVector pageRank = new ArrayRealVector(ireader.numDocs(), (double) 1/ireader.numDocs());
            ArrayRealVector transition = new ArrayRealVector(ireader.numDocs());
            for (int iter = 0; iter < iterations; iter++) {
                for (int irow = 0; irow < ireader.numDocs(); irow++) {  // iterate over matrix rows
                    // read references vector of the irow doc
                    String[] row = new String(Files.readAllBytes(Paths.get(storingFolder, "ref" + irow)))
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
            for (int i = 0; i < ireader.numDocs(); i++) {
                Document doc;
                doc = ireader.document(i);
                doc.add(new StoredField("pagerank", pageRank.getEntry(i)));
                iwriter.updateDocument(new Term("cordID", doc.get("cordID")), doc);
            }

        } catch (IOException e) {e.printStackTrace(); }
    }

    public static void main(String[] args) {
        IndexWriter writer = createIndexWriter(PoolIndexing.INDEX_FOLDERNAME);
        ReaderSearcher objReaderSearcher = new ReaderSearcher(
                Paths.get(PoolIndexing.INDEX_FOLDERNAME),
                PoolIndexing.similarity);
        IndexSearcher searcher = objReaderSearcher.searcher();
        IndexReader reader = objReaderSearcher.reader();

        PageRank pool = new PageRank(writer, reader, searcher, true);
        pool.launch();

    }
}
