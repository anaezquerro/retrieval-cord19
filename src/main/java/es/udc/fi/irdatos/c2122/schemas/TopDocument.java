package es.udc.fi.irdatos.c2122.schemas;

import org.apache.commons.math3.linear.ArrayRealVector;

/**
 * Stores only required attributes of each document and query in order to compute the designed reranking methods and
 * results.
 */
public class TopDocument {
    private final String cordID;
    private double score;
    private final int topicID;
    private ArrayRealVector embedding;
    private String title;
    private String authors;
    private int docID;

    /**
     * TopDocument initialization with:
     * @param cordID: Article identifier in TREC-COVID collection.
     * @param score: Achieved score for a given query (query is not stored).
     * @param topicID: Topic identifier of the query.
     */
    public TopDocument(String cordID, double score, int topicID) {
        this.cordID = cordID;
        this.score = score;
        this.topicID = topicID;
    }

    /**
     * TopDocument initialization with:
     * @param cordID: Article identifier in TREC-COVID collection.
     * @param score: Achieved score for a given query (query is not stored).
     * @param topicID: Topic identifier of the query.
     * @param title: Title of the article.
     * @param authors: Sequence of authors names.
     */
    public TopDocument(String cordID, double score, int topicID, String title, String authors) {
        this.cordID = cordID;
        this.score = score;
        this.topicID = topicID;
        this.title = title;
        this.authors = authors;
    }

    /**
     * Returns information about the document attributes.
     * @returns: Article ID, score and topicID.
     */
    public String toString() {
        String out = cordID + ": " + score + " (topic " + topicID + ")";
        return out;
    }

    /**
     * Stores article embedding.
     * @param embedding: Embedding of the article.
     */
    public ArrayRealVector setEmebdding(ArrayRealVector embedding) {
        this.embedding = embedding;
        return embedding;
    }

    /**
     * Updates score attribute.
     * @param newScore: New score of the document.
     */
    public void setScore(double newScore) {
        score = newScore;
    }

    /**
     * Stores document identifier in the index.
     * @param docID: Document identifier (integer) in the index.
     */
    public void setDocID(int docID) {
        this.docID = docID;
    }


    public String cordID() {
        return cordID;
    }

    public int topicID() {
        return topicID;
    }

    public double score() {
        return score;
    }

    public ArrayRealVector embedding() {
        return embedding;
    }

    public String title() {
        return title;
    }

    public String authors() {
        return authors;
    }


    public int docID() {
        return docID;
    }
}