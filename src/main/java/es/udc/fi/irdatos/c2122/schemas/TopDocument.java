package es.udc.fi.irdatos.c2122.schemas;

public record TopDocument(String docID, double score, int topicID) {
    public String toString() {
        String out = docID + ": " + score + " (topic " + topicID + ")";
        return out;
    }
}