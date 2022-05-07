package es.udc.fi.irdatos.c2122.cords;

import es.udc.fi.irdatos.c2122.schemas.TopDocument;
import es.udc.fi.irdatos.c2122.schemas.TopDocumentOrder;
import es.udc.fi.irdatos.c2122.schemas.Topics;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
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
import java.util.stream.Stream;

import static es.udc.fi.irdatos.c2122.cords.CollectionReader.*;
import static es.udc.fi.irdatos.c2122.cords.ObtainTransitionMatrix.computePageRank;

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
            return phraseQuery();
        } else if (typeQuery == 2) {
            return embeddingsQuery();
        } else if (typeQuery == 3) {
            return embeddingsQueryRocchio(1F, 0.7F, 0.9F);
        } else if (typeQuery == 4) {
            return queryPageRank();
        } else {
            System.out.println("No queries have been applied");
        }
        return null;

    }

    private static List<TopDocument> coerce(TopDocs topDocs, int topicID) {
        List<TopDocument> topDocuments = Arrays.stream(topDocs.scoreDocs).map(x -> {
            try {
                return new TopDocument(ireader.document(x.doc).get("docID"), x.score, topicID);
            } catch (IOException e) {
                e.printStackTrace();
                return null;
            }
        }).toList();
        return topDocuments;
    }

    /**
     * Performs a multifield weighted query in the main fields: title, abstract and body.
     *
     * @returns Map object where topic IDs are the keys with they corresponding set of relevant documents.
     */
    private static Map<Integer, List<TopDocument>> simpleQuery() {
        // Create the MultiField Query
        Map<String, Float> fields = Map.of("title", (float) 0.3, "abstract", (float) 0.4, "body", (float) 0.3);
        QueryParser parser = new MultiFieldQueryParser(fields.keySet().toArray(new String[0]), new StandardAnalyzer(), fields);

        Query query;
        Map<Integer, List<TopDocument>> topicsTopDocs = new HashMap<>();

        // Loop for each topic to extract the topicID and make the query
        for (Topics.Topic topic : topics) {

            // Firstly make the query with the query topic
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


    private Map<Integer, List<TopDocument>> embeddingsQuery() {
        Map<Integer, List<TopDocument>> topicsTopDocs = readCosineSimilarities("cosineSimilarity", true);
        return obtainTopN(topicsTopDocs);
    }

    private Map<Integer, List<TopDocument>> embeddingsQueryRocchio(float alpha, float beta, float gamma) {
        // Obtain query embeddings
        Map<Integer, ArrayRealVector> queryEmbeddings = readQueryEmbeddings();

        // Obtain results by cosine similarity between embeddings
        Map<Integer, List<TopDocument>> initialResults = embeddingsQuery();

        // Compute new queries based on Rocchio Algorithm
        PoolRocchio poolRocchio = new PoolRocchio(initialResults, queryEmbeddings, alpha, beta, gamma);
        poolRocchio.launch();
        Map<Integer, List<TopDocument>> newResults = poolRocchio.getNewSimilarities();

        return obtainTopN(newResults);
    }

    private Map<Integer, List<TopDocument>> queryPageRank() {
        // Obtain initial results
        Map<Integer, List<TopDocument>> initialResults = simpleQuery();
        Map<Integer, List<TopDocument>> newResults = new HashMap<>();

        // Compute page rank for each topic
        for (Topics.Topic topic : topics) {
            List<TopDocument> initialDocuments = initialResults.get(topic.number());
            ArrayRealVector pageRank = computePageRank(initialDocuments, 0.5, 1000);

            // Multiply the initial score for pageRank value
            List<TopDocument> newDocuments = new ArrayList<>();
            for (int i=0; i < initialDocuments.size(); i++) {
                TopDocument initialDocument = initialDocuments.get(i);
                newDocuments.add(new TopDocument(initialDocument.docID(),
                        initialDocument.score() * pageRank.getEntry(i),
                        topic.number()));
            }
            Collections.sort(newDocuments, new TopDocumentOrder());
            newResults.put(topic.number(), newDocuments);
        }

        return newResults;
    }

    private Map<Integer, List<TopDocument>> obtainTopN(Map<Integer, List<TopDocument>> topicsTopDocs) {
        topicsTopDocs = topicsTopDocs.entrySet().stream().peek(
                result ->  {
                    result.setValue(result.getValue().subList(0, n));
                }
        ).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        return topicsTopDocs;
    }
}