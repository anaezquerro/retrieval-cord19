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
import org.apache.lucene.queryparser.xml.builders.BooleanQueryBuilder;
import org.apache.lucene.search.*;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static es.udc.fi.irdatos.c2122.cords.AuxiliarFunctions.floatArray2RealVector;
import static es.udc.fi.irdatos.c2122.cords.AuxiliarFunctions.realVector2floatArray;
import static es.udc.fi.irdatos.c2122.cords.CollectionReader.*;

public class QueryTopics {
    private static Topics.Topic[] topics;
    private static int n;
    private static IndexSearcher isearcher;
    private static IndexReader ireader;


    public QueryTopics(IndexReader ireader, IndexSearcher isearcher, Topics.Topic[] topics, int n) {
        this.ireader = ireader;
        this.isearcher = isearcher;
        this.topics = topics;
        this.n = n;
    }

    public Map<Integer, List<TopDocument>> query(int typeQuery) {

        // Make the Query basd on the typeQuery
        if (typeQuery == 0) {
            return simpleQuery();
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

    /**
     * Coerces the top documents provided by Lucene to a list of Top Documents in which we store
     * the docID, the score obtained in the topic query, the title and the authors.
     * @param topDocs Top documents provided by Lucene with a specific query.
     * @param topicID Topic number from which the query was obtained.
     * @returns List of Top Document class.
     */
    private static List<TopDocument> coerce(TopDocs topDocs, int topicID) {
        List<TopDocument> topDocuments = Arrays.stream(topDocs.scoreDocs).map(x -> {
            try {
                Document doc = ireader.document(x.doc);
                TopDocument topDocument = new TopDocument(doc.get("docID"), x.score, topicID,
                        doc.get("title"), doc.get("authors"));
                return topDocument;
            } catch (IOException e) {
                e.printStackTrace();
                return null;
            }
        }).toList();
        return topDocuments;
    }

    /**
     * Performs a multifield weighted query in the main fields: title, abstract and body.
     * @returns Map object where topic IDs are the keys with their corresponding set of relevant documents.
     */
    private static Map<Integer, List<TopDocument>> simpleQuery() {
        Map<Integer, List<TopDocument>> topicsTopDocs = new HashMap<>();


        // Create MultiField parser with per field weights
        Map<String, Float> fields = Map.of("title", (float) 0.3, "abstract", (float) 0.5, "body", (float) 0.3);
        QueryParser parser = new MultiFieldQueryParser(fields.keySet().toArray(new String[0]), new StandardAnalyzer(), fields);

        // Loop for each topic to extract the topicID and make the query
        for (Topics.Topic topic : topics) {
            Query query;
            try {
                query = parser.parse(topic.query());
            } catch (ParseException e) {
                System.out.println("ParseException while constructing the query for the topic " + topic.number());
                e.printStackTrace();
                return null;
            }

            // Secondly, obtain the top N documents
            TopDocs topDocs;
            try {
                topDocs = isearcher.search(query, n);
            } catch (IOException e) {
                System.out.println("IOException while searching documents of the topic ");
                e.printStackTrace();
                return null;
            }

            // Finally, add the top documents to the map object
            System.out.println(topDocs.totalHits + " results for the query: " + topic.query() + " [topic=" + topic.number() + "]");
            List<TopDocument> topDocuments = coerce(topDocs, topic.number());
            topicsTopDocs.put(topic.number(), topDocuments);
        }

        return obtainTopN(topicsTopDocs);
    }


    private Map<Integer, List<TopDocument>> knnSimpleQuery(Map<Integer, float[]> queryEmbeddings) {
        // Create the MultiField Query
        Map<String, Float> fields = Map.of("title", (float) 0.7, "abstract", (float) 0.5, "body", (float) 0.3);
        QueryParser parser = new MultiFieldQueryParser(fields.keySet().toArray(new String[0]), new StandardAnalyzer(), fields);
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
            booleanQueryBuilder.add(query, BooleanClause.Occur.SHOULD);

            // Add KNN query
            float[] queryEmbedding = queryEmbeddings.get(topic.number());

            // Parse query
            Query knnQuery = new KnnVectorQuery("embedding", queryEmbedding, n);
            booleanQueryBuilder.add(knnQuery, BooleanClause.Occur.SHOULD);

            // build boolean query
            BooleanQuery booleanQuery = booleanQueryBuilder.build();

            // Obtain topDocs
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
        Map<Integer, float[]> initialQueryEmbeddings = readQueryEmbeddingsFloating();
        Map<Integer, List<TopDocument>> initialResults = knnSimpleQuery(initialQueryEmbeddings);

        // Recompute query embeddings
        Map<Integer, ArrayRealVector> initialQueryEmbeddingsVector = initialQueryEmbeddings.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, x -> floatArray2RealVector(x.getValue())));
        PoolRocchio poolRocchio = new PoolRocchio(initialResults, initialQueryEmbeddingsVector, alpha, beta, gamma);
        poolRocchio.launch();

        Map<Integer, ArrayRealVector> newQueryEmbeddings = poolRocchio.getNewQueryEmbeddings();
        Map<Integer, float[]> newQueryEmbeddingsFloat = newQueryEmbeddings.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, x -> realVector2floatArray(x.getValue())));

        // Recompute query
        return knnSimpleQuery(newQueryEmbeddingsFloat);
        }

    private Map<Integer, List<TopDocument>> cosineSimilarityPageRank(float alpha, int iterations) {
        Map<Integer, ArrayRealVector> queryEmbeddings = readQueryEmbeddings();

        // Obtain initial results
        PoolCosineSimilarity pool = new PoolCosineSimilarity(queryEmbeddings);
        Map<Integer, List<TopDocument>> initialResults = CollectionReader.readCosineSimilarities("cosineSimilarity", true);
        initialResults = obtainTopN(initialResults);

        // Compute again using page rank
        ObtainTransitionMatrix poolPageRank = new ObtainTransitionMatrix(isearcher, ireader, initialResults,
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

    private Map<Integer, List<TopDocument>> phraseQuery() {
        Map<Integer, List<TopDocument>> topicsTopDocs = new HashMap<>();
        Map<String, Float> fieldBoosts = Map.of("title", 0.4F, "abstract", 0.3F, "body", 0.3F);

        for (Topics.Topic topic : topics) {

            BooleanQuery.Builder booleanQueryBuilder = new BooleanQuery.Builder();

            PhraseQuery.Builder phraseQueryBuilder = new PhraseQuery.Builder();
            int pos = 0;
            List<String> bigram = new ArrayList<>();
            List<String> trigram = new ArrayList<>();
            for (String word : topic.query().split(" ")) {

                if (bigram.size() == 2) {
                    for (String field : new String[]{"title", "abstract", "body"}) {
                        PhraseQuery.Builder bigramQueryBuilder = new PhraseQuery.Builder();
                        bigramQueryBuilder.add(new Term(field, bigram.get(0)), 0);
                        bigramQueryBuilder.add(new Term(field, bigram.get(1)), 1);
                        bigramQueryBuilder.setSlop(4);
                        booleanQueryBuilder.add(new BoostQuery(bigramQueryBuilder.build(), fieldBoosts.get(field)),
                                BooleanClause.Occur.SHOULD);
                    }
                    bigram.remove(0);    // delete the first element
                }
                if (trigram.size() == 3) {
                    for (String field : new String[]{"title", "abstract", "body"}) {
                        PhraseQuery.Builder trigramQueryBuilder = new PhraseQuery.Builder();
                        trigramQueryBuilder.add(new Term(field, trigram.get(0)), 0);
                        trigramQueryBuilder.add(new Term(field, trigram.get(1)), 1);
                        trigramQueryBuilder.add(new Term(field, trigram.get(2)), 2);
                        trigramQueryBuilder.setSlop(10);
                        booleanQueryBuilder.add(new BoostQuery(trigramQueryBuilder.build(), fieldBoosts.get(field)),
                                BooleanClause.Occur.SHOULD);
                    }
                    trigram.remove(0);    // delete first element
                }

                trigram.add(word);
                bigram.add(word);
                phraseQueryBuilder.add(new Term("body", word), pos);

                for (String field : fieldBoosts.keySet()) {
                    booleanQueryBuilder.add(new BoostQuery(new TermQuery(new Term(field, word)), fieldBoosts.get(field)),
                            BooleanClause.Occur.SHOULD);
                }
                pos++;
            }
            phraseQueryBuilder.setSlop((int) Math.ceil(topic.query().split(" ").length * 2.5));
            PhraseQuery phraseQuery = phraseQueryBuilder.build();
            booleanQueryBuilder.add(new BoostQuery(phraseQuery, fieldBoosts.get("body")), BooleanClause.Occur.SHOULD);
            BooleanQuery booleanQuery = booleanQueryBuilder.build();

            // Make the query
            TopDocs topDocs;
            try {
                topDocs = isearcher.search(booleanQuery, n);
            } catch (IOException e) {
                System.out.println("IOException while searching documents of the topic ");
                e.printStackTrace();
                return null;
            }

            // Finally, add the top documents to the map object
            System.out.println(topDocs.totalHits + " results for the query: " + topic.query() + " [topic=" + topic.number() + "]");
            List<TopDocument> topDocuments = coerce(topDocs, topic.number());
            topicsTopDocs.put(topic.number(), topDocuments);
        }

        return topicsTopDocs;
    }
}