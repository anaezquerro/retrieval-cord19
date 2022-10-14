package models;

import lucene.IdxReader;
import lucene.IdxSearcher;
import org.apache.lucene.document.Document;
import schemas.TopDocument;
import schemas.TopDocumentComparator;
import schemas.TopicQuery;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class PageRankModel extends RetrievalModel {
    private IdxReader reader;
    private IdxSearcher searcher;
    private BooleanModel baseModel;
    private String countField = "countPageRank";
    private String binaryField = "binaryPageRank";
    private String field;

    public PageRankModel(IdxReader reader, IdxSearcher searcher, boolean count) {
        super(reader, searcher);
        this.reader = reader;
        this.searcher = searcher;
        baseModel = new BooleanModel(reader, searcher);
        if (count) {
            field = countField;
        } else {
            field = binaryField;
        }
    }

    public List<TopDocument> query(TopicQuery topicQuery, int topN) {
        List<TopDocument> initialResults = baseModel.query(topicQuery, topN);

        List<TopDocument> finalResults = new ArrayList<>();
        for (TopDocument topDoc : initialResults) {
            Document doc = reader.document(topDoc.docID());
            topDoc.setScore(
                    Double.parseDouble(doc.get(field))*topDoc.score()
            + topDoc.score());
            finalResults.add(topDoc);
        }
        Collections.sort(finalResults, new TopDocumentComparator());
        return finalResults;
    }
}
