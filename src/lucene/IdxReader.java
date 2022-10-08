package lucene;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.*;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.nio.file.Paths;

public class IdxReader {
    private IndexReader reader;
    private String foldername;

    public IdxReader(String foldername) {
        this.foldername = foldername;
        try {
            Directory directory = FSDirectory.open(Paths.get(foldername));
            reader = DirectoryReader.open(directory);
        } catch (CorruptIndexException e) {
            System.out.println("CorruptIndexEception while reading " + foldername);
            e.printStackTrace();
        } catch (IOException e) {
            System.out.println("IOException while reading " + foldername);
            e.printStackTrace();
        }
    }

    public int numDocs() {
        return reader.numDocs();
    }

    public Document document(int docID) {
        Document doc = null;
        try {
            doc = reader.document(docID);
        } catch (IOException e) {
            System.out.println("IOException while reading document with docID=" + docID + " in " + foldername);
            e.printStackTrace();
        }
        return doc;
    }

    public String foldername() {
        return foldername;
    }

    public IndexReader reader() {
        return reader;
    }

    public void close() {
        try {
            reader.close();
        } catch (IOException e) {
            System.out.println("IOException while closing IndexReader in " + foldername);
            e.printStackTrace();
        }
    }

    public Terms getTermVector(int docID, String fieldname) {
        Terms vector;
        try {
            vector = reader.getTermVector(docID, fieldname);
        } catch (IOException e) {
            System.out.println("IOException while reading term vector of document " + docID);
            e.printStackTrace();
            return null;
        }
        return vector;
    }

    public Double docFreq(Term term) {
        Integer freq = null;
        try {
            freq = reader.docFreq(term);
        } catch (IOException e) {
            System.out.println("IOException while calling the doc frequency of term " + term.text() + " in " +
                    "field" + term.field());
            e.printStackTrace();
        }
        return (double) freq;
    }
}
