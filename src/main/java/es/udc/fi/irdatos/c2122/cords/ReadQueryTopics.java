package es.udc.fi.irdatos.c2122.cords;

import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import es.udc.fi.irdatos.c2122.util.ObjectReaderUtils;
import org.apache.commons.io.FileUtils;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.*;
import org.apache.lucene.store.FSDirectory;


import java.io.File;
import java.io.FileWriter;
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
    private static String RESULTS_FILENAME = "round5-submission.txt";

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

    /**
     * Computes the mean average precision metric with the top documents returned by all topic queries and the real
     * relevant documents of each topic.
     * @param reader IndexReader to obtain the fields of the documents returned.
     * @param predictedRelevants Map object with the predicted relevant documents of each topic.
     * @param realRelevants Map object with the real relevant documents of each topic obtained form the relevance_judgments.txt file.
     * @param k Threshold for calculating the precision in each document.
     * @returns Mean average precision at k over all topics.
     */
    public static Float meanAveragePrecision(IndexReader reader, Map<Integer, TopDocs> predictedRelevants, Map<Integer, List<String>> realRelevants, int k) {
        float mAPk = 0;

        // Loop for each topic
        for (Map.Entry<Integer, TopDocs> topic : predictedRelevants.entrySet()) {
            float APk = averagePrecision(reader, topic.getValue(), realRelevants.get(topic.getKey()), k);
            System.out.println("AveragePrecision at k=" + k + " in topic " + topic.getKey() + ": " + APk);
            mAPk = mAPk + APk;
        }

        // Normalize the mean average precision
        mAPk = mAPk / predictedRelevants.size();
        System.out.println("mAP@" + k + " metric: " + mAPk);

        return mAPk;
    }

    /**
     * Generated the txt file with the submission format specified in the TREC-COVID Challenge once the top documents
     * of each topic have been obtained.
     * @param reader IndexReader to access to the document ID.
     * @param topicsTopDocs Map object with the top documents of each topic.
     * @param filename File name which results text file will be stored with.
     * @param cut Number of top documents to submit in the results list.
     */
    public static final void generateResults(IndexReader reader, Map<Integer, TopDocs> topicsTopDocs, String filename, int cut) {
        // Create the new file (delete previously if it already exists)
        File file;
        try {
            file = new File(filename);
            if (file.exists()) {
                file.delete();
            }
        } catch (Exception e) {
            System.out.println("IOException while removing " + filename + " folder");
        }

        FileWriter writer = null;
        try {
            writer = new FileWriter(filename);
        } catch (Exception e) {
            System.out.println("Exception occurred while creating the new file: " + filename);
            e.printStackTrace();
        }

        // loop for each topic to submit the results
        String runtag = "ir-ppaa";
        for (int topic : topicsTopDocs.keySet()) {
            TopDocs topDocs = topicsTopDocs.get(topic);    // obtain the top documents

            // add each document
            for (int i=0; i < cut; i++) {
                String docID = null;
                try {
                    docID = reader.document(topDocs.scoreDocs[i].doc).get("docID");
                } catch (IOException e) {
                    System.out.println("IOException while saving the document " + i + " of the topic " + topic);
                    e.printStackTrace();
                }

                String rank = Integer.toString(i);
                String score = Float.toString(topDocs.scoreDocs[i].score);

                try {
                    writer.write(String.join(" ", Integer.toString(topic), "Q0", docID, rank, score, runtag, "\n"));
                } catch (IOException e) {
                    System.out.println("IOException while saving the results of the document " + i + " of the topic " + topic);
                    e.printStackTrace();
                }
            }
        }

        // Close the writer
        try {
            writer.close();
        } catch (IOException e) {
            System.out.println("IOException while closing the txt writer");
            e.printStackTrace();
        }
    }

    /**
     * Calls the whole process of parsing topics set (XML file) and relevance judgements (TXT file), searching in the
     * inverted index created in the first iteration, creating the results list and evaluating the queries performance.
     * @param args At least two arguments are needed to be provided. The first argument is the cut-off of the documents
     *             to submit in the results list. The second argument is the k value to compute the MAP@k metric.
     * Optionally, the typeQuery ID can be passed. If it is 0, the simpleQuery (see README-Iteration2) will be computed.
     *             If it is 1, the phraseQuery will be computed instead.
     */
    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("At least the cut-off of the documents results and k value of the MAP@k metric must be provided");
            return;
        }
        int n = Integer.parseInt(args[0]);
        int k = Integer.parseInt(args[1]);
        int typeQuery = 0;
        if (args.length > 2) {
            typeQuery = Integer.parseInt(args[2]);
            if (typeQuery > 2) {
                System.out.println("typeQuery parameter must be 0 [simpleQuery] or 1 [phraseQuery]");
                return;
            }
        }

        // Read the topics set
        Topics.Topic[] topics = readTopicSet();

        // Read the relevance judgements
        Map<Integer, List<String>> topicRelevDocs = readRelevanceJudgements();

        // Make the queries for each topic query
        QueryTopics queryTopics = new QueryTopics(INDEX_PATH, topics, n);
        Map<Integer, TopDocs> topicsTopDocs = queryTopics.query(typeQuery);

        // Create the IndexReader instance to read the indexed documents ID
        IndexReader reader = null;
        try {
            reader = DirectoryReader.open(FSDirectory.open(INDEX_PATH));
        } catch (CorruptIndexException e1) {
            System.out.println("CorruptIndexException while reading the index " + INDEX_PATH.toString());
        } catch (IOException e1) {
            System.out.println("IOException while reading the index " + INDEX_PATH.toString());
        }

        // Generate the results
        generateResults(reader, topicsTopDocs, RESULTS_FILENAME, n);

        // Compute MAP@k metric
        float mAPk = meanAveragePrecision(reader, topicsTopDocs, topicRelevDocs, k);
    }


}


