package es.udc.fi.irdatos.c2122.cords;

import es.udc.fi.irdatos.c2122.schemas.Metadata;
import org.apache.lucene.document.*;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static es.udc.fi.irdatos.c2122.cords.CollectionReader.readArticle;
import static es.udc.fi.irdatos.c2122.cords.CollectionReader.readArticles;

/**
 * Implements the parallel indexing process of the JSON files content once the metadata.csv has been parsed
 */
public class IndexingPool {
    public static class WorkerThread implements Runnable {
        private IndexWriter iwriter;
        private List<Metadata> metadata;
        private Path collectionPath;
        private int numWorker;

        /**
         * Subclass of a Thread Proccess of the indexing Pool.
         * @param iwriter Index writer (thread-safe) to index the collection documents.
         * @param metadata List of Metadata objects representing each row of the metadata.csv file.
         * @param collectionPath Path to collection where the articles JSON files are stored.
         */
        public WorkerThread(IndexWriter iwriter, List<Metadata> metadata, Path collectionPath, int numWorker) {
            this.iwriter = iwriter;
            this.metadata = metadata;
            this.collectionPath = collectionPath;
            this.numWorker = numWorker;
        }

        /** 
        * When the thread starts its tasks, it is in charge of indexing the metadata fields (docID, title, abstract) 
         * and the article content referenced in the PMC and PDF paths.
         */
        @Override
        public void run() {
            for (Metadata article : metadata) {
                // Create the document and add the docID, title and abstract of the metadata row as Lucene Fields
                Document doc = new Document();
                doc.add(new StoredField("docID", article.cordUid()));
                doc.add(new TextField("title", article.title(), Field.Store.YES));
                doc.add(new TextField("abstract", article.abstrac(), Field.Store.YES));

                // Read PMC and PDF paths (check README-iteration1 to understand this block of code)
                List<Path> pdfPaths = article.pdfFiles().stream().map(pdfPath -> collectionPath.resolve(pdfPath)).collect(Collectors.toList());
                String[] articleContent;
                if (article.pmcFile().length() != 0) {
                    articleContent = readArticle(collectionPath.resolve(article.pmcFile()));
                } else if (article.pdfFiles().size() >= 1) {
                    List<Path> pmcpdfPaths = new ArrayList<>();
                    pmcpdfPaths.addAll(pdfPaths);
                    articleContent = readArticles(pmcpdfPaths);
                } else {
                    articleContent = new String[] {"", "", ""};
                }

                // Save body, references and figure notes as new fields in the document
                FieldType bodyFieldType = new FieldType();
                bodyFieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
                doc.add(new Field("body", articleContent[0], bodyFieldType));
                doc.add(new TextField("references", articleContent[1], Field.Store.YES));
                doc.add(new TextField("figures", articleContent[2], Field.Store.YES));

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
     * @param writer IndexWriter that will be used by all workers to index documents.
     * @param metadata List of Metadata.java instances representing each row of the metadata.csv file.
     * @param collectionPath Path where is stored the collection files.
     */
    public static void indexMetadataPool(IndexWriter writer, List<Metadata> metadata, Path collectionPath) {
        // Create the ExecutorService
        final int numCores = Runtime.getRuntime().availableProcessors();
        System.out.println("Indexing metadata articles with " + numCores + " cores");
        ExecutorService executor = Executors.newFixedThreadPool(numCores);

        // Give a slice of articles to each worker
        System.out.println("A total of " + metadata.size() + " articles will be parsed and indexed");
        int articlesPerThread = (int) Math.ceil((double)metadata.size()/(double)numCores);
        System.out.println("Each thread will parse and index " + articlesPerThread + " articles");
        for (int i=0; i < numCores; i++) {
            int start = i*articlesPerThread;
            int end = Math.min(metadata.size(), (i+1)*articlesPerThread);
            List<Metadata> metadataSlice = metadata.subList(start, end);
            System.out.println("Thread " + i + " is indexing articles from " + start + " to " + end);
            Runnable worker = new WorkerThread(writer, metadataSlice, collectionPath, i);
            executor.execute(worker);
        }

        // End the executor
        executor.shutdown();
        try {
            executor.awaitTermination(10, TimeUnit.MINUTES);
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

}
