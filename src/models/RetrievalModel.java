package models;

import lucene.IdxReader;
import lucene.IdxSearcher;
import org.apache.lucene.document.Document;
import org.apache.lucene.search.TopDocs;
import schemas.TopDocument;
import schemas.TopicQuery;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public abstract class RetrievalModel {

    protected IdxReader reader;
    protected IdxSearcher searcher;

    public RetrievalModel(IdxReader reader, IdxSearcher searcher) {
        this.reader = reader;
        this.searcher = searcher;
    }



    public List<TopDocument> coerce(TopDocs topDocs, int topN) {
        List<TopDocument> topDocuments = Arrays.stream(topDocs.scoreDocs).map(topDoc -> {
            Document doc = reader.document(topDoc.doc);
            TopDocument topDocument = new TopDocument(doc, topDoc.doc, topDoc.score);
            return topDocument;
        }).toList();
        return topDocuments.subList(0, topN);
    }

    public abstract List<TopDocument> query(TopicQuery topicQuery, int topN);

}
