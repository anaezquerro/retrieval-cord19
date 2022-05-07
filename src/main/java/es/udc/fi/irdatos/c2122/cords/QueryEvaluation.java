package es.udc.fi.irdatos.c2122.cords;

import es.udc.fi.irdatos.c2122.schemas.TopDocument;
import es.udc.fi.irdatos.c2122.schemas.Topics;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.*;
import org.apache.lucene.search.similarities.LMJelinekMercerSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;


import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static es.udc.fi.irdatos.c2122.cords.CollectionReader.readRelevanceJudgements;
import static es.udc.fi.irdatos.c2122.cords.CollectionReader.readTopicSet;


/**
 * Class for reading and parsing TREC-COVID topics set and relevance judgments and make queries using the query field
 * of each topic.
 */
public class QueryEvaluation {
    private static Path INDEX_PATH = Paths.get("Index-StandardAnalyzer-LM");
    private static Similarity similarity = new LMJelinekMercerSimilarity(0.1F);

    /**
     * Computes the average precision metric with the top documents returned by a query and the real relevant documents.
     * @param predictedRelevant Top documents returned by the query.
     * @param realRelevant Real relevant documents obtained from the relevance judgements file.
     * @param k Threshold for calculating the precision in each document.
     * @returns Average precision at k.
     */
    public static final Float averagePrecision(List<TopDocument> predictedRelevant, List<String> realRelevant, int k) {
        float APk = 0;
        int TPseen = 0;

        // Loop for each document returned by the query
        for (int i = 0; i < Math.min(predictedRelevant.size(), k); i++) {

            String docID = predictedRelevant.get(i).docID();

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
     * @param predictedRelevants Map object with the predicted relevant documents of each topic.
     * @param realRelevants Map object with the real relevant documents of each topic obtained form the relevance_judgments.txt file.
     * @param k Threshold for calculating the precision in each document.
     * @returns Mean average precision at k over all topics.
     */
    public static Float meanAveragePrecision(Map<Integer, List<TopDocument>> predictedRelevants, Map<Integer, List<String>> realRelevants, int k) {
        float mAPk = 0;

        // Loop for each topic
        for (Map.Entry<Integer, List<TopDocument>> topic : predictedRelevants.entrySet()) {
            float APk = averagePrecision(topic.getValue(), realRelevants.get(topic.getKey()), k);
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
     * @param topicsTopDocs Map object with the top documents of each topic.
     * @param filename File name which results text file will be stored with.
     * @param cut Number of top documents to submit in the results list.
     */
    public static final void generateResults(Map<Integer, List<TopDocument>> topicsTopDocs, String filename, int cut) {
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
            List<TopDocument> topDocuments = topicsTopDocs.get(topic);    // obtain the top documents

            // add each document
            for (int i=0; i < cut; i++) {
                String docID = topDocuments.get(i).docID();
                String rank = Integer.toString(i);
                String score = Double.toString(topDocuments.get(i).score());

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
        typeQuery = Integer.parseInt(args[2]);


        // Read the topics set
        Topics.Topic[] topics = readTopicSet();

        // Read the relevance judgements
        Map<Integer, List<String>> topicRelevDocs = readRelevanceJudgements();

        // Create IndexReader and IndexSearcher
        ReaderSearcher creation = new ReaderSearcher(INDEX_PATH, similarity);
        IndexReader ireader = creation.reader();
        IndexSearcher isearcher = creation.searcher();

        // Make the queries for each topic query
        QueryTopics queryTopics = new QueryTopics(ireader, isearcher, topics, n);
        Map<Integer, List<TopDocument>> topicsTopDocs = queryTopics.query(typeQuery);

        // Generate the results
        String filenameResults = "round5-submission.txt";
        generateResults(topicsTopDocs, filenameResults, n);

        // Compute MAP@k metric
        float mAPk = meanAveragePrecision(topicsTopDocs, topicRelevDocs, k);
    }




}


