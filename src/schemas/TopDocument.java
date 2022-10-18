package schemas;
import org.apache.lucene.document.Document;

import java.util.Objects;

/**
 * Stores only required attributes of each document and query in order to compute the designed reranking methods and
 * results.
 */
public class TopDocument {
    private final String cordUID;
    private double score;
    private int topicID;
    private Embedding embedding;
    private String title;
    private String authors;
    private int docID;
    private double binaryPageRank;
    private double countPageRank;

    public TopDocument(Document doc, int docID, double score) {
        this.cordUID = doc.get("cordUID");
        this.docID = docID;
        this.score = score;
        this.title = doc.get("title");
        this.authors = doc.get("authors");
        this.embedding = new Embedding(doc.get("embedding"));
        try {
            this.binaryPageRank = Double.parseDouble(doc.get("binaryPageRank"));
            this.countPageRank = Double.parseDouble(doc.get("countPageRank"));
        } catch (NullPointerException e) {}
    }


    public String toString() {
        String out = cordUID + ": " + score + " (topic " + topicID + ")";
        return out;
    }

    public String cordUID() {
        return cordUID;
    }

    public double score() {
        return score;
    }

    public String title() {
        return title;
    }

    public int docID() {
        return docID;
    }

    public void setScore(double score) {
        this.score = score;
    }

}