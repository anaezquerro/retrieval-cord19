package es.udc.fi.irdatos.c2122.schemas;

import java.util.Comparator;

public class TopDocumentOrder implements Comparator<TopDocument> {
    public int compare(TopDocument doc1, TopDocument doc2) {
        if (doc1.score() < doc2.score()) {
            return 1;
        } else if (doc1.score() > doc2.score()) {
            return -1;
        } else {
            return 0;
        }
    }
}