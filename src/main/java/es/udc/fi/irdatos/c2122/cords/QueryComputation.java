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
    private static float bodyBoost = 1F;
    private static Map<Integer, float[]> queryEmbeddings = readQueryEmbeddingsFloating();

    public QueryComputation(IndexReader ireader, IndexSearcher isearcher, Topics.Topic[] topics, int n) {
        this.ireader = ireader;
        this.isearcher = isearcher;
        this.topics = topics;
        this.n = n;
        if (n > 10) {
            bodyBoost = 1F;
        }
    }

    public Map<Integer, List<TopDocument>> query(int typeQuery) {
        if (typeQuery == 0) {
            return probabilisticQuery();
        } else if (typeQuery == 1) {
            return knnRocchioQuery(0.5F, 0.4F, 0.1F);
        } else if (typeQuery == 2) {
            return cosineSimilarityPageRank(0.0F, 1000);
        }
        else {
            System.out.println("No queries have been applied");
        }
        return null;

    }

    private static Map<Integer, List<TopDocument>> multifieldQuery() {
        Map<Integer, List<TopDocument>> topicsTopDocs = new HashMap<>();

        // Create field weights (initially they are set to constant values but in future approaches it might be
        // useful to configure them as a function of the number of documents to be returned)
        Map<String, Float> fields = Map.of("title", titleBoost, "abstract", abstractBoost, "body", bodyBoost);

        // Create QueryParser with StandardAnalyzer
        QueryParser parser = new MultiFieldQueryParser(fields.keySet().toArray(new String[0]), new StandardAnalyzer(), fields);

        // Loop for each topic to extract topicID and query
        for (Topics.Topic topic : topics) {

            // parse the query text
            Query query;
            try {
                query = parser.parse(topic.query());
            } catch (ParseException e) {
                System.out.println("ParseException while constructing the query for the topic " + topic.number());
                e.printStackTrace();
                return null;
            }

            // search in the index
            TopDocs topDocs;
            try {
                topDocs = isearcher.search(query, n);
            } catch (IOException e) {
                System.out.println("IOException while searching documents of the topic ");
                e.printStackTrace();
                return null;
            }

            // add the top documents to the map object
            System.out.println(topDocs.totalHits + " results for the query: " + topic.query() + " [topic=" + topic.number() + "]");
            List<TopDocument> topDocuments = coerce(topDocs, topic.number());
            topicsTopDocs.put(topic.number(), topDocuments);
        }
        return obtainTopN(topicsTopDocs);
    }

    private static Map<Integer, List<TopDocument>> probabilisticQuery() {
        Map<Integer, List<TopDocument>> initialResults = new HashMap<>();
        float[] boosts = new float[] {titleBoost, abstractBoost, bodyBoost};

        // To compute the initial results, we use the same query text for all fields (title, abstract and body)
        for (Topics.Topic topic : topics) {
            String[] initialTextQueries = new String[] {topic.query(), topic.query(), topic.query()};
            List<Query> queries = parseQueries(new String[] {"title", "abstract", "body"}, initialTextQueries);
            TopDocs topDocs = booleanQueries(queries, queryEmbeddings.get(topic.number()));
            List<TopDocument> topDocuments = coerce(topDocs, topic.number());
            initialResults.put(topic.number(), topDocuments);
        }

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
            TopDocs topDocs = booleanQueries(queries, queryEmbeddings.get(topic.number()));
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
        PoolRocchio poolRocchio = new PoolRocchio(initialResults, initialQueryEmbeddings, alpha, beta, gamma);
        poolRocchio.launch();

        Map<Integer, ArrayRealVector> newQueryEmbeddings = poolRocchio.getNewQueryEmbeddings();
        Map<Integer, float[]> newQueryEmbeddingsFloat = newQueryEmbeddings.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, x -> realVector2floatArray(x.getValue())));

        // Recompute query
        return knnQuery(newQueryEmbeddingsFloat);
    }

    private Map<Integer, List<TopDocument>> cosineSimilarityPageRank(float alpha, int iterations) {

        // Obtain initial results
        Map<Integer, List<TopDocument>> initialResults = readCosineSimilarities("cosineSimilarity", true);
        initialResults = obtainTopN(initialResults);

        // Obtain results of simpleQuery
        Map<Integer, List<TopDocument>> simpleQueryResults = multifieldQuery();

        // Merge both results
        Map<Integer, List<TopDocument>> mergedResults = new HashMap<>();
        for (Integer topic : initialResults.keySet()) {
            Map<String, TopDocument> mergedresultsTopic = new HashMap<>();
            for (TopDocument topDocument : initialResults.get(topic)) {
                mergedresultsTopic.put(topDocument.cordID(), topDocument);
            }
            for (TopDocument topDocument : simpleQueryResults.get(topic)) {
                if (mergedresultsTopic.containsKey(topDocument.cordID())) {
                    double oldScore = mergedresultsTopic.get(topDocument.cordID()).score();
                    mergedresultsTopic.get(topDocument.cordID()).setScore(oldScore + topDocument.score());
                } else {
                    mergedresultsTopic.put(topDocument.cordID(), topDocument);
                }
            }
            List<TopDocument> topDocuments = new ArrayList<>();
            topDocuments.addAll(mergedresultsTopic.values());
            Collections.sort(topDocuments, new TopDocumentOrder());
            mergedResults.put(topic, topDocuments);
        }

        mergedResults = obtainTopN(mergedResults);
        // Compute again using page rank
        ObtainTransitionMatrix poolPageRank = new ObtainTransitionMatrix(isearcher, ireader, mergedResults,
                alpha, iterations);
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


    private static TopDocs booleanQueries(List<Query> queries, float[] queryEmbedding) {
        BooleanQuery.Builder booleanQueryBuilder = new BooleanQuery.Builder();

        for (Query query : queries ) {
            booleanQueryBuilder.add(query, BooleanClause.Occur.MUST);
        }
        booleanQueryBuilder.add(new KnnVectorQuery("embedding", queryEmbedding, n), BooleanClause.Occur.SHOULD);
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