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

    /**
     * Initialize QueryComputation class by introducing the index reader/searcher, list of topics and number of
     * documents to be returned.
     * @param ireader: IndexReader (needed to compute reranking and obtain documents stored data).
     * @param isearcher: IndexSearcher.
     * @param topics: Array of topics
     * @param n: Number of documents to be returned for each query.
     */
    public QueryComputation(IndexReader ireader, IndexSearcher isearcher, Topics.Topic[] topics, int n) {
        this.ireader = ireader;
        this.isearcher = isearcher;
        this.topics = topics;
        this.n = n;
    }

    /**
     * Computes a specific query biy giving its integer identifier. From the list of topic (this.topics), it is possible
     * to access to topic queries and use them to compute a ranking of n documents per topic.
     * @param typeQuery: Integer used to identify the query to be computed (for more information, see README.md)
     * @returns Map object where the top n documents are stored for each topic number.
     */
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

    /**
     * Compute the base query that consists of a MultiField weighted query for title, abstract and body fields.
     * @returns Top n document for each topic.
     */
    private static Map<Integer, List<TopDocument>> multifieldQuery() {
        Map<Integer, List<TopDocument>> topicsTopDocs = new HashMap<>();

        // Create field weights (initially they are set to constant values but in future approaches it might be
        // useful to configure them as a function of the number of documents to be returned)
        Map<String, Float> fields = Map.of("title", (float) 0.3, "abstract", (float) 0.5, "body", (float) 0.3);

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

    /**
     * Computes an initial boolean query and takes the top n documents to expand the initial query
     * adding new terms and recompute the query.
     * @returns Top n documents based on the expanded query.
     */
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

        //
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
            List<Query> queries = parseQueries(new String[] {"title", "abstract", "body"}, newTextQueries);

            // Create boolean query
            TopDocs topDocs = booleanQueries(queries);
            List<TopDocument> topDocuments = coerce(topDocs, topic.number());
            topicsTopDocs.put(topic.number(), topDocuments);
        }
        return topicsTopDocs;
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
                topDocument.setDocID(x.doc);
                return topDocument;
            } catch (IOException e) {
                e.printStackTrace();
                return null;
            }
        }).toList();
        return topDocuments;
    }

    /**
     * For a given array of field names and array of texts (representing the query text for each field), use a
     * QueryParser to parse each text for its corresponding field and return the resulting query.
     * @param fields: String array of field names.
     * @param textQueries: String array of texts to be parsed as queries for each field.
     * @returns: An array of queries.
     */
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


    /**
     * Compute a boolean query with a given list of queries.
     * @param queries: List of queries.
     * @returns: Top n documents for the boolean query.
     */
    private static TopDocs booleanQueries(List<Query> queries) {
        BooleanQuery.Builder booleanQueryBuilder = new BooleanQuery.Builder();

        for (Query query : queries ) {
            booleanQueryBuilder.add(query, BooleanClause.Occur.SHOULD);
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