package lucene;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;

import java.io.IOException;

public class IdxSearcher {
    private IndexSearcher searcher;
    private String foldername;

    public IdxSearcher(IdxReader reader) {
        this.foldername = reader.foldername();
        searcher = new IndexSearcher(reader.reader());
    }

    public TopDocs search(Query query, int top) {
        TopDocs topDocs = null;
        try {
            topDocs = searcher.search(query, top);
        } catch (IOException e) {
            System.out.println("IOException while searching in " + foldername + " the query " + query.toString());
            System.exit(-1);
        }
        return topDocs;
    }

    public IndexSearcher searcher() {
        return searcher;
    }

    public String toString() {
        return this.foldername;
    }
}
