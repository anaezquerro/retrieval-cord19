package lucene;

import cords.PoolIndexing;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.LockObtainFailedException;

import java.io.IOException;
import java.nio.file.Paths;

import static util.AuxiliarFunctions.deleteFolder;

public class IdxWriter {
    private IndexWriter writer;
    private String foldername;

    public IdxWriter(String foldername) {
        deleteFolder(foldername);
        IndexWriterConfig config = new IndexWriterConfig(new StandardAnalyzer());
        config.setSimilarity(PoolIndexing.similarity);

        this.foldername = foldername;

        try {
            writer = new IndexWriter(FSDirectory.open(Paths.get(foldername)), config);
        } catch (CorruptIndexException e) {
            System.out.println("CorruptIndexException while creating IndexWriter at " + foldername);
            e.printStackTrace();
        } catch (LockObtainFailedException e) {
            System.out.println("LockObtainFailedException while creating IndexWriter at " + foldername);
            e.printStackTrace();
        } catch (IOException e) {
            System.out.println("IOException while creating IndexWriter at " + foldername);
            e.printStackTrace();
        }
    }

    public void addDocument(Document doc) {
        try {
            writer.addDocument(doc);
        } catch (IOException e) {
            System.out.println("IOException while adding document with cordID=" + doc.get("cordID") + " in " +
                    "index " + foldername);
            e.printStackTrace();
        }
    }

    public void close() {
        try {
            writer.close();
        } catch (IOException e) {
            System.out.println("IOException while closing IndexWriter in " + foldername);
            e.printStackTrace();
        }
    }

    public void commit() {
        try {
            writer.commit();
        } catch (IOException e) {
            System.out.println("IOException while commit IndexWriter in " + foldername);
            e.printStackTrace();
        }
    }
}
