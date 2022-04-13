package es.udc.fi.irdatos.c2122.cords;

import com.fasterxml.jackson.databind.ObjectReader;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.LockObtainFailedException;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static es.udc.fi.irdatos.c2122.cords.ReadMetadata.readArticle;
import static es.udc.fi.irdatos.c2122.cords.ReadMetadata.readArticles;


public class IndexMetadataPool {
    public static class WorkerThread implements Runnable {
        private IndexWriter iwriter;
        private List<MetadataArticle> metadata;
        private Path collectionPath;

        public WorkerThread(IndexWriter iwriter, List<MetadataArticle> metadata, Path collectionPath) {
            this.iwriter = iwriter;
            this.metadata = metadata;
            this.collectionPath = collectionPath;
        }

        @Override
        public void run() {
            for (MetadataArticle article : metadata) {
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
                bodyFieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS);
                doc.add(new Field("body", articleContent[0], bodyFieldType));
                doc.add(new TextField("references", articleContent[1], Field.Store.YES));
                doc.add(new TextField("figures", articleContent[2], Field.Store.YES));

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
            System.out.println("One thread has finished");
        }
    }


    public static void indexMetadataPool(List<MetadataArticle> metadata, Path indexPath, Path collectionPath) {
        // Create the ExecutorService
        final int numCores = Runtime.getRuntime().availableProcessors();
        System.out.println("Indexing metadata articles with " + numCores + " cores");
        ExecutorService executor = Executors.newFixedThreadPool(numCores);

        // Create the IndexWriter
        IndexWriterConfig config = new IndexWriterConfig(new StandardAnalyzer());
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

        // Launch a slice of articles to index to each worker
        System.out.println("A total of " + metadata.size() + " articles will be parsed and indexed");
        int articlesPerThread = (int) Math.ceil((double)metadata.size()/(double)numCores);
        System.out.println("Each thread will parse and index " + articlesPerThread + " articles");
        for (int i=0; i < numCores; i++) {
            int start = i*articlesPerThread;
            int end = Math.min(metadata.size(), (i+1)*articlesPerThread);
            List<MetadataArticle> metadataSlice = metadata.subList(start, end);
            System.out.println("Thread " + i + " is indexing articles from " + start + " to " + end);
            Runnable worker = new WorkerThread(writer, metadataSlice, collectionPath);
            executor.execute(worker);
        }

        executor.shutdown();
        System.out.println("No more indexing tasks will be launched");
        try {
            executor.awaitTermination(5, TimeUnit.MINUTES);
        } catch (final InterruptedException e) {
            e.printStackTrace();
            System.exit(-2);
        }
        System.out.println("Indexing processes have finished");


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
