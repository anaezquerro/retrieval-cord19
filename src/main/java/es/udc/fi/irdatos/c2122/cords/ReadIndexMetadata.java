package es.udc.fi.irdatos.c2122.cords;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import es.udc.fi.irdatos.c2122.schemas.Metadata;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.*;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.commons.io.FileUtils;

import static es.udc.fi.irdatos.c2122.cords.CollectionReader.readMetadata;
import static es.udc.fi.irdatos.c2122.cords.IndexMetadataPool.indexMetadataPool;


/**
 * Implements the reading and parsing proccess of the collection and calls the indexing pool to create the collection
 * index.
 */
public class ReadIndexMetadata {
    private static String INDEX_FOLDER = "Index-StandardAnalyzer";
    private static final Path DEFAULT_COLLECTION_PATH = Paths.get("2020-07-16");


    /**
     * @param args Optionally, the folder path to store the index can be passed as an argument. If no arguments are
     * passed, the INDEX_FOLDER variable will be used.
     */
    public static void main(String[] args) {
        String indexFolder;
        if (args.length == 0) {
            indexFolder = INDEX_FOLDER;
        } else {
            indexFolder = args[0];
        }
        // If the index path already exists, it must be deleted
        if (new File(indexFolder).exists()) {
            try {
                FileUtils.deleteDirectory(new File(indexFolder));
            } catch (IOException e) {
                System.out.println("IOException while removing " + indexFolder + " folder");
            }
        }

        // Create the indexPath
        Path indexPath = Paths.get(indexFolder);

        // Read metadata.csv
        List<Metadata> metadata = readMetadata();

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

        // Start the index process by passing the index writer, the metadata information, the index ath
        indexMetadataPool(writer, metadata, DEFAULT_COLLECTION_PATH);
    }
}
