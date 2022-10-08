package cords;

import lucene.IdxReader;
import lucene.IdxSearcher;
import models.BooleanModel;
import models.ProbabilityModel;
import models.RetrievalModel;
import models.VectorModel;
import schemas.TopDocument;
import schemas.TopicQuery;

import java.util.*;
import java.util.stream.IntStream;

/**
 * Different query computations for the
 */
public class QueryComputation {
    private int n;
    private IdxSearcher isearcher;
    private IdxReader ireader;
    private Map<Integer, List<TopDocument>> results = new HashMap<>();
    private List<TopicQuery> topics;
    private String[] fields = {"title", "abstract", "body"};
    private float[] weights = {20F, 10F, 5F};



    public QueryComputation(IdxReader ireader, IdxSearcher isearcher, List<TopicQuery> topics, int n) {
        this.ireader = ireader;
        this.isearcher = isearcher;
        this.topics = topics;
        this.n = n;
    }

    public Map<Integer, List<TopDocument>> query(int typeQuery) {
        RetrievalModel model;
        if (typeQuery==1) {
            model = new BooleanModel(ireader, isearcher);
        } else if (typeQuery==2) {
            model = new VectorModel(ireader, isearcher, 0.1, 0.8, 0.5, 5);
        } else {
            model = new ProbabilityModel(ireader, isearcher, 2, new String[]{"title", "abstract"});
        }

        List<TopDocument> topDocs;
        for (TopicQuery topicQuery : topics) {
            setFieldsWeights(topicQuery);
            topDocs = model.query(topicQuery, n);
            results.put(topicQuery.topicID(), topDocs);
        }
        return results;
    }

    private void setFieldsWeights(TopicQuery topicQuery) {
        Map<String, String> fieldTexts = new HashMap<>();
        Map<String, Float> fieldWeights = new HashMap<>();
        IntStream.range(0, fields.length).forEach(
                i -> {
                    fieldTexts.put(fields[i], topicQuery.text());
                    fieldWeights.put(fields[i], weights[i]);
                }
        );
        topicQuery.setFieldWeights(fieldWeights);
        topicQuery.setFieldTexts(fieldTexts);
    }
}
