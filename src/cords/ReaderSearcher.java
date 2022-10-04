package es.udc.fi.irdatos.c2122.cords;

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

public class ReaderSearcher {
    public IndexReader ireader;
    public IndexSearcher isearcher;
    public Path indexPath;
    public Similarity similarity;

    public ReaderSearcher(Path indexPath, Similarity similarity) {
        this.indexPath = indexPath;
        try {
            Directory directory = FSDirectory.open(indexPath);
            this.ireader = DirectoryReader.open(directory);
        } catch (CorruptIndexException e) {
            System.out.println("CorruptIndexEception while reading " + indexPath.toString());
            e.printStackTrace();
        } catch (IOException e) {
            System.out.println("IOException while reading " + indexPath.toString());
            e.printStackTrace();
        }

        // Create IndexSearcher
        this.isearcher = new IndexSearcher(ireader);
        this.similarity = similarity;
        this.isearcher.setSimilarity(similarity);
    }

    public IndexSearcher searcher() {return this.isearcher;}

    public IndexReader reader() {return this.ireader;}
}
