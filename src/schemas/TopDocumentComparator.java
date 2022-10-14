package schemas;

import java.util.Comparator;

public class TopDocumentComparator implements Comparator<TopDocument> {
    @Override
    public int compare(TopDocument doc1, TopDocument doc2) {
        return Double.compare(doc1.score(), doc2.score());
    }
}
