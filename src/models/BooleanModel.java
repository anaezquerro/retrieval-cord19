package models;

import lucene.IdxReader;
import lucene.IdxSearcher;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
import schemas.TopDocument;
import schemas.TopicQuery;

import java.util.HashMap;
import java.util.List;
import java.util.Map;



/**
 * Implementation of the classical Boolean Retrieval Model.
 */
public class BooleanModel extends RetrievalModel {

    private IdxReader reader;
    private IdxSearcher searcher;

    public BooleanModel(IdxReader reader, IdxSearcher searcher) {
        super(reader, searcher);
    }

    public List<TopDocument> query(TopicQuery topicQuery, int topN) {
        Map<String, Query> fieldQueries = parseQueries(topicQuery.fieldTexts());
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        for (Map.Entry<String, Query> fieldQuery : fieldQueries.entrySet()) {
            builder.add(
                    new BoostQuery(fieldQuery.getValue(), topicQuery.fieldWeights().get(fieldQuery.getKey())),
                    BooleanClause.Occur.SHOULD);
        }
        BooleanQuery booleanQuery = builder.build();
        TopDocs topDocs = searcher.search(booleanQuery, topN);
        return super.coerce(topDocs, topN);
    }

    private Map<String, Query> parseQueries(Map<String, String> fieldTexts) {
        Map<String, Query> queries = new HashMap<>();
        QueryParser parser;
        Query query;
        for (Map.Entry<String, String> entry : fieldTexts.entrySet()) {
            parser = new QueryParser(entry.getKey(), new StandardAnalyzer());
            try {
                query = parser.parse(entry.getValue());
            } catch (ParseException e) {
                System.out.println("IOException while parsing: " + entry.getValue());
                e.printStackTrace();
                return null;
            }
            queries.put(entry.getKey(), query);
        }
        return queries;
    }

}
