package es.udc.fi.irdatos.c2122.cords;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.LMJelinekMercerSimilarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.lang.reflect.InaccessibleObjectException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class QueryTopics {
    private Path indexPath;
    private Topics.Topic[] topics;
    private int n;

    public QueryTopics(Path indexPath, Topics.Topic[] topics, int n) {
        this.indexPath = indexPath;
        this.topics = topics;
        this.n = n;
    }

    public Map<Integer, TopDocs> query(int typeQuery) {
        // Create IndexReader from indexPath
        IndexReader ireader = null;
        try {
            Directory directory = FSDirectory.open(indexPath);
            ireader = DirectoryReader.open(directory);
        } catch (CorruptIndexException e) {
            System.out.println("CorruptIndexEception while reading " + indexPath.toString());
            e.printStackTrace();
        } catch (IOException e) {
            System.out.println("IOException while reading " + indexPath.toString());
            e.printStackTrace();
        }

        // Create the IndexSearcher
        IndexSearcher isearcher = new IndexSearcher(ireader);

        // Make the Query basd on the typeQuery
        if (typeQuery == 0) {
            return simpleQuery(isearcher);
        } else if (typeQuery == 1) {
            return phraseQuery(isearcher);
        }
        return null;

    }

    /**
     * Performs a multifield weighted query in the main fields: title, abstract and body.
     * @param isearcher Lucene IndexSearcher to open the index path.
     * @returns Map object where topic IDs are the keys with they corresponding set of relevant documents.
     */
    private Map<Integer, TopDocs> simpleQuery(IndexSearcher isearcher) {
        // Create the MultiField Query
        Map<String, Float> fields = Map.of("title", (float)0.3, "abstract", (float)0.4, "body", (float)0.3);
        QueryParser parser = new MultiFieldQueryParser(fields.keySet().toArray(new String[0]), new StandardAnalyzer(), fields);

        Query query;
        Map<Integer, TopDocs> topicsTopDocs = new HashMap<>();

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
            System.out.println(topDocs.totalHits + " results for the query: " + topic.query() + " [topic=" + topic.number() + "]" );
            topicsTopDocs.put(topic.number(), topDocs);
        }
        return topicsTopDocs;
    }


    private Map<Integer, TopDocs> phraseQuery(IndexSearcher isearcher) {
        Map<Integer, TopDocs> topicsTopDocs = new HashMap<>();
        Map<String, Float> fieldBoosts = Map.of("title", 0.4F, "abstract", 0.3F, "body", 0.3F);

        for (Topics.Topic topic : topics) {

            BooleanQuery.Builder booleanQueryBuilder = new BooleanQuery.Builder();

            PhraseQuery.Builder phraseQueryBuilder = new PhraseQuery.Builder();
            int pos = 0;
            List<String> bigram = new ArrayList<>();
            List<String> trigram = new ArrayList<>();
            for (String word : topic.query().split(" ")) {

                if (bigram.size() == 2) {
                    for (String field : new String[] {"title", "abstract", "body"}) {
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
                    for (String field : new String[] {"title", "abstract", "body"}) {
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
            phraseQueryBuilder.setSlop((int) Math.ceil(topic.query().split(" ").length*2.5));
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
            System.out.println(topDocs.totalHits + " results for the query: " + topic.query() + " [topic=" + topic.number() + "]" );
            System.out.println(booleanQuery.toString());
            topicsTopDocs.put(topic.number(), topDocs);
        }

        return topicsTopDocs;
    }



}
