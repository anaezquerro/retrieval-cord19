package es.udc.fi.irdatos.c2122.cords;

import es.udc.fi.irdatos.c2122.schemas.TopDocument;
import es.udc.fi.irdatos.c2122.schemas.TopDocumentOrder;
import es.udc.fi.irdatos.c2122.schemas.Topics;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.*;
import java.util.stream.Collectors;

import static es.udc.fi.irdatos.c2122.cords.AuxiliarFunctions.floatArray2RealVector;
import static es.udc.fi.irdatos.c2122.cords.AuxiliarFunctions.realVector2floatArray;
import static es.udc.fi.irdatos.c2122.cords.CollectionReader.*;

/**
 * Multiple query implementations for TREC-COVID topics.
 */
public class QueryComputation {
    private static Topics.Topic[] topics;
    private static int n;
    private static IndexSearcher isearcher;
    private static IndexReader ireader;
    private static float titleBoost = 1F;
    private static float abstractBoost = 1F;
    private static float bodyBoost = 1.1F;
    private static Map<Integer, float[]> queryEmbeddings = readQueryEmbeddingsFloating();
    private static Map<String, ArrayRealVector> docEmbeddings;


    public QueryComputation(IndexReader ireader, IndexSearcher isearcher, Topics.Topic[] topics, int n) {
        this.ireader = ireader;
        this.isearcher = isearcher;
        this.topics = topics;
        this.n = n;
        if (n == 100) {titleBoost = 1F; abstractBoost = 1F; bodyBoost = 1.1F; }
        else if (n == 1000) {titleBoost = 1F; abstractBoost = 1F; bodyBoost = 1.3F;}
    }

    public Map<Integer, List<TopDocument>> query(int typeQuery) {
        if (typeQuery == 0) {
            return probabilisticQuery();
        } else if (typeQuery == 1) {
            docEmbeddings = readDocEmbeddings();
            return knnRocchioQuery(0.5F, 0.4F, 0.1F);
        } else if (typeQuery == 2) {
            return queryPageRank(0.0F, 1000);
        }
        else {
            System.out.println("No queries have been applied");
        }
        return null;

    }

    private static Map<Integer, List<TopDocument>> probabilisticQuery() {
        Map<Integer, List<TopDocument>> initialResults = new HashMap<>();

        // To compute the initial results, we use the same query text for all fields (title, abstract and body)
        for (Topics.Topic topic : topics) {
            String[] initialTextQueries = new String[] {topic.query(), topic.query(), topic.query()};
            List<Query> queries = parseQueries(new String[] {"title", "abstract", "body"}, initialTextQueries);
            TopDocs topDocs = booleanQueries(queries);
            List<TopDocument> topDocuments = coerce(topDocs, topic.number());
            initialResults.put(topic.number(), topDocuments);
        }
        initialResults = obtainTop(initialResults, 100);

        ProbabilityFeedback probs = new ProbabilityFeedback(ireader, initialResults, 1);
        Map<Integer, List<String>> newTitleTerms = probs.getProbabilities("title");
        Map<Integer, List<String>> newAbstractTerms = probs.getProbabilities("abstract");

        Map<Integer, List<TopDocument>> topicsTopDocs = new HashMap<>();
        for (Topics.Topic topic : topics) {
            String[] newTextQueries = new String[] {
                    topic.query() + " " + String.join(" ", newTitleTerms.get(topic.number())),
                    topic.query()  + " " + String.join(" ", newAbstractTerms.get(topic.number())),
                    topic.query()
            };
            System.out.println("New query for topic " + topic.number() + ": " + String.join("/", newTextQueries));
            List<Query> queries = parseQueries(new String[] {"title", "abstract", "body"}, newTextQueries);

            // Create boolean query
            TopDocs topDocs = booleanQueries(queries);
            List<TopDocument> topDocuments = coerce(topDocs, topic.number());
            topicsTopDocs.put(topic.number(), topDocuments);
        }
        return topicsTopDocs;
    }

    private Map<Integer, List<TopDocument>> knnQuery(Map<Integer, float[]> queryEmbeddings) {
        QueryParser parser = new MultiFieldQueryParser(new String[] {"title", "abstract", "body"}, new StandardAnalyzer());
        Map<Integer, List<TopDocument>> topicsTopDocs = new HashMap<>();

        // Loop for each topic
        for (Topics.Topic topic : topics) {
            BooleanQuery.Builder booleanQueryBuilder = new BooleanQuery.Builder();

            Query query;
            try {
                query = parser.parse(topic.query());
            } catch (ParseException e) {
                System.out.println("ParseException while constructing the query for the topic " + topic.number());
                e.printStackTrace();
                return null;
            }
            booleanQueryBuilder.add(query, BooleanClause.Occur.MUST);

            float[] queryEmbedding = queryEmbeddings.get(topic.number());                   // add knn query
            Query knnQuery = new KnnVectorQuery("embedding", queryEmbedding, n);
            booleanQueryBuilder.add(knnQuery, BooleanClause.Occur.SHOULD);

            BooleanQuery booleanQuery = booleanQueryBuilder.build();
            TopDocs topDocs;
            try {
                topDocs = isearcher.search(booleanQuery, n);
            } catch (IOException e) {
                System.out.println("IOException while searching documents of the topic ");
                e.printStackTrace();
                return null;
            }

            // Finally, add the top documents to the map object
            System.out.println(topDocs.totalHits + " results for the KNN query [topic=" + topic.number() + "]");
            List<TopDocument> topDocuments = coerce(topDocs, topic.number());
            topicsTopDocs.put(topic.number(), topDocuments);
        }
        return topicsTopDocs;
    }

    /**
     * Computes KNN-algorithm (where k=n) using documents and query embeddings of the TREC-COVID collection and re-ranks
     * computing again the query embeddings using Rocchio-Algorithm.
     * @returns Top Documents list for each topic.
     */
    private Map<Integer, List<TopDocument>> knnRocchioQuery(float alpha, float beta, float gamma) {
        // Compute first results
        Map<Integer, List<TopDocument>> initialResults = knnQuery(queryEmbeddings);

        Map<Integer, ArrayRealVector> initialQueryEmbeddings = queryEmbeddings.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, x -> floatArray2RealVector(x.getValue())));


        Map<Integer, ArrayRealVector> newQueryEmbeddings = new HashMap<>();
        for (Topics.Topic topic : topics) {
            newQueryEmbeddings.put(topic.number(),
                    computeRocchio(
                            initialQueryEmbeddings.get(topic.number()),
                            initialResults.get(topic.number()),
                            alpha, beta, gamma));
        }
        Map<Integer, float[]> newQueryEmbeddingsFloat = newQueryEmbeddings.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, x -> realVector2floatArray(x.getValue())));

        // Recompute query
        return knnQuery(newQueryEmbeddingsFloat);
    }

    private ArrayRealVector computeRocchio(ArrayRealVector queryEmbedding, List<TopDocument> relevant,
                                           float alpha, float beta, float gamma) {
        // Obtain only docIDs of relevant documents
        List<String> relevantDocs = relevant.stream().map(doc -> doc.cordID()).toList();

        // Read document embeddings file to compute the new query
        // relevantSum = vector sum of relevant documents, nonRelevantSum = vector sum of non relevant documents
        // relevantCount = number of relevant documents, nonRelevantCount = number of non relevant documents
        ArrayRealVector relevantSum = new ArrayRealVector(CollectionReader.EMBEDDINGS_DIMENSIONALITY);
        ArrayRealVector nonRelevantSum = new ArrayRealVector(CollectionReader.EMBEDDINGS_DIMENSIONALITY);
        int relevantCount = 0;
        int nonRelevantCount = 0;

        // Iterate over the embeddings document (is preferable storing similarities, not embeddings vectors)
        for (String docID : docEmbeddings.keySet()) {
            ArrayRealVector docEmbedding = docEmbeddings.get(docID);
            if (relevantDocs.contains(docID)) {
                relevantSum = relevantSum.add(docEmbedding);
                relevantCount++;
            } else {
                nonRelevantSum = nonRelevantSum.add(docEmbedding);
                nonRelevantCount++;
            }
        }

        // Compute the new query
        ArrayRealVector newQueryEmbedding = (ArrayRealVector) queryEmbedding.mapMultiply((double)alpha);
        newQueryEmbedding = newQueryEmbedding.add(relevantSum.mapMultiply((double)(beta/relevantCount)));
        newQueryEmbedding = newQueryEmbedding.subtract(nonRelevantSum.mapMultiply((double)(gamma/nonRelevantCount)));

        return newQueryEmbedding;
    }

    private Map<Integer, List<TopDocument>> queryPageRank(float alpha, int iterations) {
        Map<Integer, List<TopDocument>> initialResults = new HashMap<>();

        // To compute the initial results, we use the same query text for all fields (title, abstract and body)
        for (Topics.Topic topic : topics) {
            String[] initialTextQueries = new String[] {topic.query(), topic.query(), topic.query()};
            List<Query> queries = parseQueries(new String[] {"title", "abstract", "body"}, initialTextQueries);
            TopDocs topDocs = booleanQueries(queries);
            List<TopDocument> topDocuments = coerce(topDocs, topic.number());
            initialResults.put(topic.number(), topDocuments);
        }
        initialResults = obtainTopN(initialResults);

        // Compute again using page rank
        ObtainTransitionMatrix poolPageRank = new ObtainTransitionMatrix(isearcher, ireader, initialResults, alpha, iterations);
        Map<Integer, List<TopDocument>> newResults = poolPageRank.launch();
        return newResults;
    }

    private static Map<Integer, List<TopDocument>> obtainTopN(Map<Integer, List<TopDocument>> topicsTopDocs) {
        topicsTopDocs = topicsTopDocs.entrySet().stream().peek(
                result ->  {
                    result.setValue(result.getValue().subList(0, n));
                }
        ).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        return topicsTopDocs;
    }

    private static Map<Integer, List<TopDocument>> obtainTop(Map<Integer, List<TopDocument>> topicsTopDocs, int topN) {
        topicsTopDocs = topicsTopDocs.entrySet().stream().peek(
                result ->  {
                    result.setValue(result.getValue().subList(0, topN));
                }
        ).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        return topicsTopDocs;
    }

    private static List<TopDocument> coerce(TopDocs topDocs, int topicID) {
        List<TopDocument> topDocuments = Arrays.stream(topDocs.scoreDocs).map(x -> {
            try {
                Document doc = ireader.document(x.doc);
                TopDocument topDocument = new TopDocument(doc.get("docID"), x.score, topicID,
                        doc.get("title"), doc.get("authors"));
                topDocument.setDocID(x.doc);
                return topDocument;
            } catch (IOException e) {
                e.printStackTrace();
                return null;
            }
        }).toList();
        return topDocuments;
    }

    private static List<Query> parseQueries(String[] fields, String[] textQueries) {
        List<Query> queries = new ArrayList<>();
        for (int i = 0; i < fields.length; i++) {
            QueryParser parser = new QueryParser(fields[i], new StandardAnalyzer());
            Query query;
            try {
                query = parser.parse(textQueries[i]);
            } catch (ParseException e) {e.printStackTrace(); return null; }
            queries.add(query);
        }
        return queries;
    }


    private static TopDocs booleanQueries(List<Query> queries) {
        float[] boosts = new float[] {titleBoost, abstractBoost, bodyBoost};

        BooleanQuery.Builder booleanQueryBuilder = new BooleanQuery.Builder();

        for (int i = 0; i < queries.size(); i++) {
            booleanQueryBuilder.add(new BoostQuery(queries.get(i), boosts[i]), BooleanClause.Occur.MUST);
        }
        BooleanQuery booleanQuery = booleanQueryBuilder.build();

        TopDocs topDocs;
        try {
            topDocs = isearcher.search(booleanQuery, n);
        } catch (IOException e) {
            System.out.println("IOException while searching documents of the topic ");
            e.printStackTrace();
            return null;
        }
        return topDocs;
    }




}