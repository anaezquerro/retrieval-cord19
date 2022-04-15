package es.udc.fi.irdatos.c2122.cords;

import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import es.udc.fi.irdatos.c2122.util.ObjectReaderUtils;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.*;
import org.apache.lucene.store.FSDirectory;


import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;


/**
 * Class for reading and parsing TREC-COVID topics set and relevance judgments and make queries using the query field
 * of each topic.
 */
public class ReadQueryTopics {
    private static final Path COLLECTION_PATH = Paths.get("2020-07-16");
    private static Path INDEX_PATH = Paths.get("Index-StandardAnalyzer");
    private static String TOPICS_FILENAME = "topics_set.xml";
    private static String RELEVANCE_JUDGEMENTS_FILENAME = "relevance_judgements.txt";
    private static final ObjectReader TOPICS_READER = XmlMapper.builder().findAndAddModules().build().readerFor(Topics.class);

    /**
     * Reads and parses topics set using the defined structure in Topics.java file.
     * @returns A 50-length array with information about each topic, stored with the Topics.Topic structure.
     */
    private static final Topics.Topic[] readTopicSet() {
        // Define topics path
        Path collectionPath = COLLECTION_PATH;
        Path topicsPath = collectionPath.resolve(TOPICS_FILENAME);

        // Use Topics and Topics.Topic structure to parse the topic set information
        Topics.Topic[] topics;
        try {
            Topics topicsList = TOPICS_READER.readValue(topicsPath.toFile());
            topics = topicsList.topic();
        } catch (IOException e) {
            System.out.println("IOException while reading topics in: " + topicsPath.toString());
            return null;
        }

        // Returns an array consisted of each topic information (number, query, question and narrative)
        return topics;
    }

    /**
     * Reads and parses relevance judgements.
     * @returns Map object where each key is a topic ID with its corresponding list of relevant documents identificers.
     */
    private static final Map<Integer, List<String>> readRelevanceJudgements() {
        // Define relevance judgments path
        Path collectionPath = COLLECTION_PATH;
        Path relevanceJudgementsPath = collectionPath.resolve(RELEVANCE_JUDGEMENTS_FILENAME);

        // Read an parse relevance judgments file
        CsvSchema schema = CsvSchema.builder().setColumnSeparator(' ').addColumn("topicID").addColumn("rank").addColumn("docID").addColumn("score").build();
        ObjectReader reader = new CsvMapper().readerFor(RelevanceJudgements.class).with(schema);

        // Creating a list with each relevance judgments using the defined structure in RelevanceJudgements.java
        // (topicID, docID, score)
        List<RelevanceJudgements> docsRelevance;
        try {
            docsRelevance = ObjectReaderUtils.readAllValues(relevanceJudgementsPath, reader);
        } catch (IOException e) {
            System.out.println("IOException while reading relevance judgments in " + relevanceJudgementsPath.toString());
            e.printStackTrace();
            return null;
        }

        // Create the Map object where each topic ID is stored with its corresponding list of relevant documents identifiers
        Map<Integer, List<String>> topicRelevDocs = new HashMap<>();
        for (int i=1; i < 51; i++) {
            List<String> emptyList = new ArrayList<>();    // firstly create an empty list
            topicRelevDocs.put(i, emptyList);                    // add to the map object the index i with the empty list
        }

        // Read the relevance judgments list and add in the list of each topicID the corresponding document identifier
        for (RelevanceJudgements doc : docsRelevance) {
            // We do not care if the score is 1 or 2 to assess its relevance
            if (doc.score() != 0) {
                topicRelevDocs.get(doc.topicID()).add(doc.docID());
            }
        }
        return topicRelevDocs;
    }

    /**
     * Computes the average precision metric with the top documents returned by a query and the real relevant documents.
     * @param reader IndexReader to obtain the fields of the documents returned.
     * @param predictedRelevant Top documents returned by the query.
     * @param realRelevant Real relevant documents obtained from the relevance judgements file.
     * @param k Threshold for calculating the precision in each document.
     * @returns Average precision at k.
     */
    public static final Float averagePrecision(IndexReader reader, TopDocs predictedRelevant, List<String> realRelevant, int k) {
        float APk = 0;
        int TPseen = 0;

        // Loop for each document returned by the query
        for (int i = 0; i < Math.min(predictedRelevant.totalHits.value, k); i++) {

            // Read the docID of the document
            String docID;
            try {
                docID = reader.document(predictedRelevant.scoreDocs[i].doc).get("docID");
            } catch (CorruptIndexException e) {
                System.out.println("CorruptIndexException while reading a docID");
                return null;
            } catch (IOException e) {
                System.out.println("IOException while reading a docID " + e);
                e.printStackTrace();
                return null;
            }

            // If the docID is in the real relevant documents list
            if (realRelevant.contains(docID)) {
                TPseen = TPseen + 1;          // add +1 to the TP seen
                APk = APk + (TPseen / (i+1));     // add TPseen/i to the APk summary
            }
        }

        // Once the loop is finished, normalize the APk summary with the min( number of real relevant document, k)
        APk = APk / Math.min(realRelevant.size(), k);

        return APk;
    }

    public static void main(String[] args) {
        // Read the topics set
        Topics.Topic[] topics = readTopicSet();

        // Read the relevance judgements
        Map<Integer, List<String>> topicRelevDocs = readRelevanceJudgements();

        // Make the queries for each topic query
        QueryTopics queryTopics = new QueryTopics(INDEX_PATH, topics, Integer.parseInt(args[0]));
        Map<Integer, TopDocs> topicsTopDocs = queryTopics.query(1);

        // Create the IndexReader instance to read the indexed documents ID
        IndexReader reader = null;
        try {
            reader = DirectoryReader.open(FSDirectory.open(INDEX_PATH));
        } catch (CorruptIndexException e1) {
            System.out.println("CorruptIndexException while reading the index " + INDEX_PATH.toString());
        } catch (IOException e1) {
            System.out.println("IOException while reading the index " + INDEX_PATH.toString());
        }

        // Compute MAP@k metric
        float mAPk = 0;
        int k = Integer.parseInt(args[0]);

        // Loop for each topic
        for (Map.Entry<Integer, TopDocs> topic : topicsTopDocs.entrySet()) {
            float APk = averagePrecision(reader, topic.getValue(), topicRelevDocs.get(topic.getKey()), k);
            System.out.println("AveragePrecision at k=" + k + " in topic " + topic.getKey() + ": " + APk);
            mAPk = mAPk + APk;
        }

        // Normalize the mean average precision
        mAPk = mAPk / topics.length;
        System.out.println("Average mAP@" + k + " metric: " + mAPk);

    }
}


