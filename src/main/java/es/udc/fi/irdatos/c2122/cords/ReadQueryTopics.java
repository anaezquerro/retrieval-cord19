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
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;


import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

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

        HashMap<Integer, List<String>> topicDocs = new HashMap<>();
        for (int i=1; i < 51; i++) {
            List<String> emptyList = new ArrayList<>();
            topicDocs.put(i, emptyList) ;
        }

        for (RelevanceJudgements doc : docsRelevance) {
            if (doc.score() != 0) {
                topicDocs.get(doc.topicID()).add(doc.docID());
            }
        }
        for (int i=1; i < 51; i++) {
            System.out.println("Topic " + i + ": " + topicDocs.get(i).size() + " relevant documents");
        }

        return topicDocs;
    }

    private static final TopDocs searchTopics(Topics.Topic[] topics, int n) {
        // Open Index folder
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

        // Create query and make search
        String[] fieldsParse = new String[] {"title", "abstract", "body"};
        IndexSearcher searcher = new IndexSearcher(reader);
        QueryParser parser = new MultiFieldQueryParser(fieldsParse, new StandardAnalyzer());

        Query query = null;
        try {
            query = parser.parse(topics[0].query());
        } catch (ParseException e) {
            e.printStackTrace();
        }

        TopDocs topDocs;
        try {
            topDocs = searcher.search(query, n);
        } catch (IOException e) {
            System.out.println("IOException while searching documents" + e);
            e.printStackTrace();
            return null;
        }
        System.out.println(topDocs.totalHits + " results for the query: " + query.toString());
        return topDocs;
    }


    public static void main(String[] args) {
        Topics.Topic[] topics = readTopicSet();
        TopDocs topDocs = searchTopics(topics, Integer.parseInt(args[0]));

//        HashMap<Integer, List<String>> topicDocs = readRelevanceJudgements();
    }
}


