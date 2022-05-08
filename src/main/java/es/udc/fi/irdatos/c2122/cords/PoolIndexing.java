package es.udc.fi.irdatos.c2122.cords;

import com.fasterxml.jackson.datatype.jsr310.deser.key.LocalDateKeyDeserializer;
import es.udc.fi.irdatos.c2122.schemas.Metadata;
import es.udc.fi.irdatos.c2122.schemas.Topics;
import org.apache.commons.io.FileUtils;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.search.similarities.LMJelinekMercerSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.LockObtainFailedException;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static es.udc.fi.irdatos.c2122.cords.AuxiliarFunctions.*;
import static es.udc.fi.irdatos.c2122.cords.CollectionReader.*;

/**
 * Implements the parallel indexing process of the JSON files content once the metadata.csv has been parsed
 */
public class PoolIndexing {
    private Path POOL_COLLECTION_PATH = CollectionReader.DEFAULT_COLLECTION_PATH;
    private String INDEX_FOLDERNAME = "Index-StandardAnalyzer";
    private Similarity similarity = new LMJelinekMercerSimilarity(0.1F);
    private static Map<String, float[]> docEmbeddings;

    public class WorkerIndexing implements Runnable {
        private IndexWriter iwriter;            // global IndexWriter for the inverted index
        private List<Metadata> metadataSlice;   // worker slice of metadata rows
        private int numWorker;

        /**
         * Subclass of a Thread Proccess of the indexing Pool.
         * @param iwriter Index writer (thread-safe) to index the collection documents.
         * @param metadata List of Metadata objects representing each row of the metadata.csv file.
         */
        public WorkerIndexing(IndexWriter iwriter, List<Metadata> metadata, int numWorker) {
            this.iwriter = iwriter;
            this.metadataSlice = metadata;
            this.numWorker = numWorker;
        }

        /** 
        * When the thread starts its tasks, it is in charge of indexing the metadata fields (docID, title, abstract) 
         * and the article content referenced in the PMC and PDF paths.
         */
        @Override
        public void run() {
            for (Metadata article : metadataSlice) {

                Document doc = new Document();

                // Add article UID as stored field
                doc.add(new StoredField("docID", article.cordUid()));

                // Add title information
                FieldType titleFieldType = new FieldType();
                titleFieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
                titleFieldType.setStored(true);
                titleFieldType.setTokenized(true);
                titleFieldType.setStoreTermVectors(true);
                doc.add(new Field("title", article.title(), titleFieldType));

                // Add abstract information
                FieldType abstractFieldType = new FieldType();
                abstractFieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
                abstractFieldType.setStored(true);
                abstractFieldType.setTokenized(true);
                abstractFieldType.setStoreTermVectors(true);
                doc.add(new Field("abstract", article.abstrac(), abstractFieldType));

                // Add document embedding
                if (docEmbeddings.keySet().contains(article.cordUid())) {
                    float[] docEmbedding = docEmbeddings.get(article.cordUid());
                    doc.add(new KnnVectorField("embedding", docEmbedding));
                }


                // Read PMC and PDF paths (check README-iteration1 to understand this block of code)
                List<Path> pdfPaths = article.pdfFiles().stream().map(pdfPath -> POOL_COLLECTION_PATH.resolve(pdfPath)).collect(Collectors.toList());
                ParsedArticle parsedArticle;
                if (article.pmcFile().length() != 0) {
                    parsedArticle = readArticle(POOL_COLLECTION_PATH.resolve(article.pmcFile()));
                } else if (article.pdfFiles().size() >= 1) {
                    List<Path> pmcpdfPaths = new ArrayList<>();
                    pmcpdfPaths.addAll(pdfPaths);
                    parsedArticle = readArticles(pmcpdfPaths);
                } else {
                    parsedArticle = new ParsedArticle("", "");
                }

                // Add body text
                FieldType bodyFieldType = new FieldType();
                bodyFieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
                bodyFieldType.setStored(false);
                bodyFieldType.setTokenized(true);
                bodyFieldType.setStoreTermVectors(true);
                doc.add(new Field("body", parsedArticle.body(), bodyFieldType));

                // Add authors
                FieldType authorsFieldType = new FieldType();
                authorsFieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
                authorsFieldType.setStored(true);
                authorsFieldType.setTokenized(true);
                authorsFieldType.setStoreTermVectors(false);
                doc.add(new Field("authors", parsedArticle.authors(), authorsFieldType));

                // Write the document in the index
                try {
                    iwriter.addDocument(doc);
                } catch (CorruptIndexException e) {
                    System.out.println("CorruptIndexException while trying to write the document " + article.cordUid());
                    e.printStackTrace();
                } catch (IOException e) {
                    System.out.println("IOException while trying to write the document " + article.cordUid());
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
    public void launch() {
        // Read metadata.csv
        List<Metadata> metadata = readMetadata();

        // Create IndexWriter
        Path indexPath = deleteFolder(INDEX_FOLDERNAME);
        IndexWriterConfig config = new IndexWriterConfig(new StandardAnalyzer());
        config.setSimilarity(similarity);
        IndexWriter writer = null;

        try {
            writer = new IndexWriter(FSDirectory.open(indexPath), config);
        } catch (CorruptIndexException e) {
            System.out.println("CorruptIndexException while creating IndexWriter at " + indexPath.toString());
            e.printStackTrace();
        } catch (LockObtainFailedException e) {
            System.out.println("LockObtainFailedException while creating IndexWriter at " + indexPath.toString());
            e.printStackTrace();
        } catch (IOException e) {
            System.out.println("IOException while creating IndexWriter at " + indexPath.toString());
            e.printStackTrace();
        }

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
            WorkerIndexing worker = new WorkerIndexing(writer, metadataSlice, i);
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

        // Close the writer
        try {
            writer.commit();
            writer.close();
        } catch (CorruptIndexException e) {
            System.out.println("CorruptIndexException while closing the index writer");
            e.printStackTrace();
        } catch (IOException e) {
            System.out.println("IOException while closing the index writer");
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        // Read document embeddings
        docEmbeddings = readDocEmbeddingsFloating();

        PoolIndexing pool = new PoolIndexing();
        pool.launch();
    }

}
