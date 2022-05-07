package es.udc.fi.irdatos.c2122.schemas;

import org.apache.commons.math3.linear.ArrayRealVector;

public class TopDocument {
    private final String docID;
    private double score;
    private final int topicID;
    private ArrayRealVector embedding;

    public TopDocument(String docID, double score, int topicID) {
        this.docID = docID;
        this.score = score;
        this.topicID = topicID;
    }

    public String toString() {
        String out = docID + ": " + score + " (topic " + topicID + ")";
        return out;
    }

    public ArrayRealVector setEmebdding(ArrayRealVector embedding) {
        this.embedding = embedding;
        return embedding;
    }

    public ArrayRealVector embedding() {
        return embedding;
    }

    public String docID() {
        return docID;
    }

    public int topicID() {
        return topicID;
    }

    public double score() {
        return score;
    }

    public void setScore(double newScore) {
        score = newScore;
    }
}