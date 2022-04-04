package es.udc.fi.irdatos.c2122.cords;

import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import es.udc.fi.irdatos.c2122.util.ObjectReaderUtils;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.search.BooleanClause.Occur;


import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static java.util.Map.entry;

public class ReadQueryTopics {
    private static final Path DEFAULT_COLLECTION_PATH = Paths.get("cord-19_2020-07-16",
            "2020-07-16");
    private static String TOPICS_FILENAME = "topics_set.xml";
    private static String RELEVANCE_JUDGEMENTS_FILENAME = "relevance_judgements.txt";

    private static final ObjectReader TOPICS_READER = XmlMapper.builder().findAndAddModules().build().readerFor(Topics.class);

    private static final Topics.Topic[] readTopicSet() {
        Path collectionPath = DEFAULT_COLLECTION_PATH;
        Path topicsPath = collectionPath.resolve(TOPICS_FILENAME);

        Topics.Topic[] topics;
        try {
            Topics topicsList = TOPICS_READER.readValue(topicsPath.toFile());
            topics = topicsList.topic();
        } catch (IOException e) {
            System.out.println("While reading a JSON file an error has occurred: " + e);
            return null;
        }
        return topics;
    }

    private static final HashMap<Integer, List<String>> readRelevanceJudgements() {
        Path collectionPath = DEFAULT_COLLECTION_PATH;
        Path relevanceJudgementsPath = collectionPath.resolve(RELEVANCE_JUDGEMENTS_FILENAME);

        CsvSchema schema = CsvSchema.builder().setColumnSeparator(' ').addColumn("topicID").addColumn("rank").addColumn("docID").addColumn("score").build();
        ObjectReader reader = new CsvMapper().readerFor(RelevanceJudgements.class).with(schema);

        List<RelevanceJudgements> docsRelevance;
        try {
            docsRelevance = ObjectReaderUtils.readAllValues(relevanceJudgementsPath, reader);
        } catch (IOException e) {
            System.out.println("IOException while reading metadata in " + relevanceJudgementsPath.toString());
            e.printStackTrace();
            return null;
        }

        HashMap<Integer, List<String>> topicRelevDocs = new HashMap<>();
        for (int i=1; i < 51; i++) {
            List<String> emptyList = new ArrayList<>();
            topicRelevDocs.put(i, emptyList) ;
        }

        for (RelevanceJudgements doc : docsRelevance) {
            if (doc.score() != 0) {
                topicRelevDocs.get(doc.topicID()).add(doc.docID());
            }
        }
        return topicRelevDocs;
    }

    private static final TopDocs[] searchTopics(Topics.Topic[] topics, int n) {
        // 1. Open Index Directory and create the IndexReader
        Directory directory = null;
        IndexReader reader = null;
        try {
            directory = FSDirectory.open(Paths.get("Index-StandardAnalyzer"));
            reader = DirectoryReader.open(directory);
        } catch (CorruptIndexException e1) {
            System.out.println("Graceful message: exception " + e1);
            e1.printStackTrace();
        } catch (IOException e1) {
            System.out.println("Graceful message: exception " + e1);
            e1.printStackTrace();;
        }
        TopDocs[] topicsTopDocs = new TopDocs[topics.length];
        IndexSearcher searcher = new IndexSearcher(reader);


        // 2. Make the query for each topic
        Query query;
        Map<String, Float> fields = Map.of("title", (float)0.4, "abstract", (float)0.35, "body", (float)0.25);
        QueryParser parser = new MultiFieldQueryParser(fields.keySet().toArray(new String[0]), new StandardAnalyzer(), fields);
        for (Topics.Topic topic : topics) {
            try {
                query = parser.parse(topic.query());
            } catch (ParseException e) {
                e.printStackTrace();
                return null;
            }

            // Obtain top n documents
            TopDocs topDocs;
            try {
                topDocs = searcher.search(query, n);
            } catch (IOException e) {
                System.out.println("IOException while searching documents" + e);
                e.printStackTrace();
                return null;
            }
            System.out.println(query.toString());
            System.out.println(topDocs.totalHits + " results for the query of topic: " + topic.query());
            topicsTopDocs[topic.number()-1] = topDocs;
        }

        return topicsTopDocs;
    }


    public static void main(String[] args) {
        Topics.Topic[] topics = readTopicSet();
        HashMap<Integer, List<String>> topicRelevDocs = readRelevanceJudgements();
        TopDocs[] topicsTopDocs = searchTopics(topics, Integer.parseInt(args[0]));

        IndexReader reader = null;
        try {
            reader = DirectoryReader.open(FSDirectory.open(Paths.get("Index-StandardAnalyzer")));
        } catch (CorruptIndexException e1) {
            System.out.println("Graceful message: exception " + e1);
            e1.printStackTrace();
        } catch (IOException e1) {
            System.out.println("Graceful message: exception " + e1);
            e1.printStackTrace();;
        }

        // Compute MAP@k metric
        float mAPk = 0;
        for (int topic_index = 0; topic_index < 50; topic_index ++) {
            TopDocs topicTopDocs = topicsTopDocs[topic_index];
            float APk = 0;
            int TPtotal = topicRelevDocs.get(topic_index+1).size();
            int TPseen = 0;
            for (int k = 1; k <= Math.min(Integer.parseInt(args[0]), topicTopDocs.totalHits.value); k++) {
                String docID;
                try {
                    docID = reader.document(topicTopDocs.scoreDocs[k-1].doc).get("docID");
                } catch (CorruptIndexException e) {
                    System.out.println("Graceful message: exception " + e);
                    e.printStackTrace();
                    return;
                } catch (IOException e) {
                    System.out.println("Graceful message: exception " + e);
                    e.printStackTrace();
                    return;
                }
                if (topicRelevDocs.get(topic_index + 1).contains(docID)) {
                    TPseen = TPseen + 1;
                    APk = APk + (TPseen / k);
                }
            }
            APk = APk / TPtotal;
            if (Double.isNaN(APk)) {
                APk = 0;
            }
            mAPk = mAPk + APk;
            System.out.println("AP@k metric in topic " + (topic_index+1) + ": " + APk);
        }
        mAPk = mAPk / topics.length;
        System.out.println("Average mAP@k metric: " + mAPk);

    }
}


