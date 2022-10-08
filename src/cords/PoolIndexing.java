package cords;

import lucene.IdxReader;
import lucene.IdxWriter;
import formats.Metadata;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.search.similarities.LMJelinekMercerSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import schemas.Embedding;
import schemas.ParsedArticle;

import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static util.AuxiliarFunctions.*;
import static cords.CollectionReader.*;

/**
 * Implementation of the parallel indexing process of the JSON files content once the metadata.csv and cord-embeddings
 * have been parsed.
 *
 * Global variables:
 *      POOL_COLLECTION_PATH: Path where JSON files are stored.
 *      INDEX_FOLDERNAME: Folder name index will be stored with.
 *      similarity: Similarity object to write the index.
 */
public class PoolIndexing {
    private Path POOL_COLLECTION_PATH = COLLECTION_PATH;
    private String TEMP_PREFFIX = "temp";
    public static String INDEX_FOLDERNAME = "Index-LMJelinekMercer";
    private String TEMP_INDEX_FOLDERNAME = TEMP_PREFFIX + INDEX_FOLDERNAME;
    public static IdxWriter iwriter;
    public static Similarity similarity = new LMJelinekMercerSimilarity(0.1F);
    public static Map<String, Embedding> docEmbeddings;
    private final int numCores =  Runtime.getRuntime().availableProcessors();


    private class WorkerIndexing implements Runnable {
        private List<Metadata> metadataSlice;   // worker slice of metadata rows
        private int numWorker;

        /**
         * Subclass of a Thread Process of the indexing Pool.
         * @param metadata List of Metadata objects representing each row of the metadata.csv file.
         * @param numWorker Worker ID.
         */
        private WorkerIndexing(List<Metadata> metadata, int numWorker) {
            this.metadataSlice = metadata;
            this.numWorker = numWorker;
        }

        /** 
        * When the thread starts its tasks, it is in charge of indexing the metadata fields:
         *      (cordUID, title, abstract, doc embedding, authors, body, references)
         */
        @Override
        public void run() {
            for (Metadata rowMetadata : metadataSlice) {
                ParsedArticle parsedArticle = parseRowMetadata(rowMetadata);
                if (Objects.isNull(parsedArticle)) {
                    continue;
                }

                Document doc = new Document();

                // Add rowMetadata UID as stored field
                doc.add(new StoredField("cordUID", rowMetadata.cordUid()));

                // title: stored, tokenized, term-vectorized
                FieldType titleFieldType = new FieldType();
                titleFieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
                titleFieldType.setStored(true);
                titleFieldType.setTokenized(true);
                titleFieldType.setStoreTermVectors(true);
                doc.add(new Field("title", rowMetadata.title(), titleFieldType));

                // abstract: stored, tokenized, term-vectorized
                FieldType abstractFieldType = new FieldType();
                abstractFieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
                abstractFieldType.setStored(true);
                abstractFieldType.setTokenized(true);
                abstractFieldType.setStoreTermVectors(true);
                doc.add(new Field("abstract", rowMetadata.abstrac(), abstractFieldType));

                // document embedding
                if (docEmbeddings.keySet().contains(rowMetadata.cordUid())) {
                    float[] docEmbedding = docEmbeddings.get(rowMetadata.cordUid()).getFloat();
                    doc.add(new KnnVectorField("embedding", docEmbedding));
                }

                // body: tokenized, term-vectorized, not stored
                FieldType bodyFieldType = new FieldType();
                bodyFieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
                bodyFieldType.setStored(false);
                bodyFieldType.setTokenized(true);
                bodyFieldType.setStoreTermVectors(true);
                doc.add(new Field("body", parsedArticle.body(), bodyFieldType));

                // authors: stored, tokenized, not term-vectorized
                FieldType authorsFieldType = new FieldType();
                authorsFieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS);
                authorsFieldType.setStored(true);
                authorsFieldType.setTokenized(true);
                authorsFieldType.setStoreTermVectors(false);
                doc.add(new Field("authors", parsedArticle.authors(), authorsFieldType));

                // references: stored
                FieldType refFieldType = new FieldType();
                refFieldType.setStored(true);
                refFieldType.setTokenized(false);
                refFieldType.setIndexOptions(IndexOptions.NONE);
                doc.add(new Field("references", parsedArticle.textReferences(), refFieldType));

                iwriter.addDocument(doc);
            }
            System.out.println("Worker " + numWorker + " : Finished");
        }
    }


    /**
     * Starts the executing pool for collection indexing.
     * -- First stage -- Basic indexing
     * 1) Prepare folders. If INDEX_FOLDERNAME and/or TEMP_INDEX_FOLDERNAME already exist, delete them and create a new
     * IndexWriter of a temporary folder
     * 2) Read metadata.csv and embeddings.csv.
     * 3) Create the executor service to launch parallel tasks.
     * 4) Launch tasks.
     * 5) Wait until termination and close the executor and the IndexWriter
     */
    public void launch(boolean getReferences) {
        // 1)
        deleteFolder(INDEX_FOLDERNAME);
        deleteFolder(TEMP_INDEX_FOLDERNAME);
        iwriter = new IdxWriter(TEMP_INDEX_FOLDERNAME);

        // 2)
        List<Metadata> metadata = readMetadata();
        docEmbeddings = readDocEmbeddings();

        // 3)
        System.out.println("Indexing metadata articles with " + numCores + " cores");
        ExecutorService executor = Executors.newFixedThreadPool(numCores);
        System.out.println("A total of " + metadata.size() + " articles will be parsed and indexed");
        Integer[] workersDivision = coalesce(numCores, metadata.size());

        // 4)
        for (int i = 0; i < numCores; i++) {
            int start = workersDivision[i];
            int end = workersDivision[i + 1];
            List<Metadata> metadataSlice = metadata.subList(start, end);
            System.out.println("Thread " + i + " is indexing articles from " + start + " to " + end);
            WorkerIndexing worker = new WorkerIndexing(metadataSlice, i);
            executor.execute(worker);
        }

        // 5)
        executor.shutdown();
        try {
            executor.awaitTermination(20, TimeUnit.MINUTES);

        } catch (final InterruptedException e) {
            e.printStackTrace();
            System.exit(-2);
        }

        iwriter.commit();
        iwriter.close();


        /**
         * -- Second stage -- Adding surrogate keys to the final index
         * 1) Create a IndexReader from the temporary folder where the index is stored.
         * 2) Create a new IndexWriter where the final index will be stored.
         * 3) Sequentially, read all documents saved in the temporary folder and add them in the final folder
         * with the new surrogate key (cordID).
         * 4) Close de IndexReader and IndexWriter.
         * 5) Delete the temporary folder.
         */

        // 1)
        IdxReader ireader = new IdxReader(TEMP_INDEX_FOLDERNAME);

        // 2)
        iwriter = new IdxWriter(INDEX_FOLDERNAME);

        // 3)
        int cordID = 0;
        for (int docID = 0; docID < ireader.numDocs(); docID++) {
            Document doc = ireader.document(docID);
            doc.add(new StoredField("cordID", cordID));
            iwriter.addDocument(doc);
            cordID++;
        }

        // 4)
        ireader.close();
        iwriter.commit();
        iwriter.close();

        // 5)
        deleteFolder(TEMP_INDEX_FOLDERNAME);
    }


    public static void main(String[] args) {
        PoolIndexing pool = new PoolIndexing();
        long start;
        long end;
        start = System.currentTimeMillis();
        pool.launch(true);
        end = System.currentTimeMillis();
        System.out.println("Indexing time (seconds): " + (end-start)*0.001);
    }

}
