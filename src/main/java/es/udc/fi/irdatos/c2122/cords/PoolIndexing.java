package es.udc.fi.irdatos.c2122.cords;

import es.udc.fi.irdatos.c2122.schemas.Metadata;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.similarities.LMJelinekMercerSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.LockObtainFailedException;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static es.udc.fi.irdatos.c2122.cords.AuxiliarFunctions.*;
import static es.udc.fi.irdatos.c2122.cords.CollectionReader.*;

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
    private Path POOL_COLLECTION_PATH = CollectionReader.DEFAULT_COLLECTION_PATH;
    public static String INDEX_FOLDERNAME = "Index-LMJelinekMercer";
    public static IndexWriter iwriter;
    public static Similarity similarity = new LMJelinekMercerSimilarity(0.1F);
    public static Map<String, float[]> docEmbeddings;


    private class WorkerIndexing implements Runnable {
        private List<Metadata> metadataSlice;   // worker slice of metadata rows
        private int numWorker;

        /**
         * Subclass of a Thread Proccess of the indexing Pool.
         * @param metadata List of Metadata objects representing each row of the metadata.csv file.
         * @param numWorker Worker ID.
         */
        private WorkerIndexing(List<Metadata> metadata, int numWorker) {
            this.metadataSlice = metadata;
            this.numWorker = numWorker;
        }

        /** 
        * When the thread starts its tasks, it is in charge of indexing the metadata fields (docID, title, abstract) 
         * and the article content referenced in the PMC and PDF paths.
         */
        @Override
        public void run() {
            for (Metadata rowMetadata : metadataSlice) {
                // Read PMC and PDF paths
                ParsedArticle parsedArticle = parseArticle(rowMetadata);
                if (Objects.isNull(parsedArticle)) {
                    continue;
                }

                Document doc = new Document();

                // Add rowMetadata UID as stored field
                doc.add(new StoredField("docID", rowMetadata.cordUid()));

                // Add title information as stored, tokenized and term-vectorized
                FieldType titleFieldType = new FieldType();
                titleFieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
                titleFieldType.setStored(true);
                titleFieldType.setTokenized(true);
                titleFieldType.setStoreTermVectors(true);
                doc.add(new Field("title", rowMetadata.title(), titleFieldType));

                // Add abstract information as stored, tokenized and term-vectorized
                FieldType abstractFieldType = new FieldType();
                abstractFieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
                abstractFieldType.setStored(true);
                abstractFieldType.setTokenized(true);
                abstractFieldType.setStoreTermVectors(true);
                doc.add(new Field("abstract", rowMetadata.abstrac(), abstractFieldType));

                // Add document embedding as a KnnVectorField
                if (docEmbeddings.keySet().contains(rowMetadata.cordUid())) {
                    float[] docEmbedding = docEmbeddings.get(rowMetadata.cordUid());
                    doc.add(new KnnVectorField("embedding", docEmbedding));
                }

                // Add body text as tokenized and term-vectorized (but not stored)
                FieldType bodyFieldType = new FieldType();
                bodyFieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
                bodyFieldType.setStored(false);
                bodyFieldType.setTokenized(true);
                bodyFieldType.setStoreTermVectors(true);
                doc.add(new Field("body", parsedArticle.body(), bodyFieldType));

                // Add authors as stored, tokenized but not term-vectorized
                FieldType authorsFieldType = new FieldType();
                authorsFieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS);
                authorsFieldType.setStored(true);
                authorsFieldType.setTokenized(true);
                authorsFieldType.setStoreTermVectors(false);
                doc.add(new Field("authors", parsedArticle.authors(), authorsFieldType));

                // Add PMC or PDF file as stored (it will be useful for computing PageRank)
                FieldType fileFieldType = new FieldType();
                fileFieldType.setStored(true);
                fileFieldType.setTokenized(false);
                fileFieldType.setIndexOptions(IndexOptions.NONE);
                if (rowMetadata.pmcFile().length() != 0) {
                    doc.add(new Field("file", rowMetadata.pmcFile(), fileFieldType));
                } else {
                    doc.add(new Field("file", rowMetadata.pdfFiles().get(0), fileFieldType));
                }

                // write the document in the index
                try {
                    iwriter.addDocument(doc);
                } catch (CorruptIndexException e) {
                    System.out.println("CorruptIndexException while trying to write the document " + rowMetadata.cordUid());
                    e.printStackTrace();
                } catch (IOException e) {
                    System.out.println("IOException while trying to write the document " + rowMetadata.cordUid());
                    e.printStackTrace();
                }
            }
            System.out.println("Worker " + numWorker + " : Finished");
        }
    }


    /**
     * Starts the executing pool for collection indexing. By default, the number of workers will be the number of
     * cores in the machine.
     */
    public void launch(boolean getReferences) {
        // Read metadata.csv and document embeddings
        List<Metadata> metadata = readMetadata();
        docEmbeddings = readDocEmbeddingsFloating();

        // create IndexWriter and configure it
        deleteFolder(INDEX_FOLDERNAME);
        iwriter = createIndexWriter(INDEX_FOLDERNAME);

        // Create the ExecutorService
        final int numCores = Runtime.getRuntime().availableProcessors();
        System.out.println("Indexing metadata articles with " + numCores + " cores");
        ExecutorService executor = Executors.newFixedThreadPool(numCores);

        // Give a slice of articles to each worker
        System.out.println("A total of " + metadata.size() + " articles will be parsed and indexed");
        Integer[] workersDivision = coalesce(numCores, metadata.size());

        for (int i=0; i < numCores; i++) {
            int start = workersDivision[i];
            int end = workersDivision[i+1];
            List<Metadata> metadataSlice = metadata.subList(start, end);
            System.out.println("Thread " + i + " is indexing articles from " + start + " to " + end);
            WorkerIndexing worker = new WorkerIndexing(metadataSlice, i);
            executor.execute(worker);
        }

        // End the executor
        executor.shutdown();
        try {
            executor.awaitTermination(20, TimeUnit.MINUTES);
        } catch (final InterruptedException e) {
            e.printStackTrace();
            System.exit(-2);
        }


        if (getReferences) {
            System.out.println("Adding references to the index...");
            try { iwriter.commit(); }
            catch (IOException e) { e.printStackTrace(); return; }

            ReaderSearcher objReaderSearcher = new ReaderSearcher(
                    Paths.get(INDEX_FOLDERNAME), similarity);
            IndexReader reader = objReaderSearcher.reader();
            IndexSearcher searcher = objReaderSearcher.searcher();

            ReferencesIndexing referencesIndexing = new ReferencesIndexing(iwriter, reader, searcher, true);
            referencesIndexing.launch();
        } else {
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
    }

    public static void main(String[] args) {
        PoolIndexing pool = new PoolIndexing();
        if (args.length > 0) {
            pool.launch(true);
        } else {
            pool.launch(false);
        }

    }

}
