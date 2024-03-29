package cords;

import lucene.IdxReader;
import lucene.IdxSearcher;
import schemas.TopDocument;
import schemas.TopicQuery;


import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

import static cords.CollectionReader.*;


/**
 * Class for reading and parsing TREC-COVID topics set and relevance judgments and make queries using the query field
 * of each topic.
 */
public class QueryEvaluation {
    private static String INDEX_FOLDERNAME = PoolIndexing.INDEX_FOLDERNAME;


    // --------------------------------------------- compute metrics ---------------------------------------------
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

            String cordID = predictedRelevant.get(i).cordUID();

            if (realRelevant.contains(cordID)) {
                TPseen = TPseen + 1;              // add +1 to the TP seen
                APk = APk + (TPseen / (i+1));     // add TPseen/i to the APk summary
            }
        }

        // Once the loop is finished, normalize the APk summary with the min( number of real relevant document, k)
        APk = APk / Math.min(realRelevant.size(), k);

        return APk;
    }

    /**
     * Computes the mean AP metric with the top documents returned by all topic queries and the real relevant documents of each topic.
     * @param predictedRelevants Map object with the predicted relevant documents of each topic.
     * @param realRelevants Map object with the real relevant documents of each topic obtained form the relevance_judgments.txt file.
     * @param k Threshold for calculating the precision in each document.
     * @returns Mean AP at k over all topics.
     */
    public static Float meanAveragePrecision(Map<Integer, List<TopDocument>> predictedRelevants, Map<Integer, List<String>> realRelevants, int k) {
        float mAPk = 0;

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


    // ------------------------------------------------- save results --------------------------------------------------
    /**
     * Generated the TXT file with the submission format specified in the TREC-COVID Challenge once the top documents
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
                String cordID = topDocuments.get(i).cordUID();
                String rank = Integer.toString(i);
                String score = Double.toString(topDocuments.get(i).score());

                try {
                    writer.write(String.join(" ", Integer.toString(topic), "Q0", cordID, rank, score, runtag, "\n"));
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

    public static void main(String[] args) {
        int n = Integer.parseInt(args[0]);
        int k = Integer.parseInt(args[1]);
        int typeQuery = Integer.parseInt(args[2]);

        // read topics set and relevance judgements
        List<TopicQuery> topics = readTopics();
        Map<Integer, List<String>> topicRelevDocs = readRelevanceJudgements();


        // create the reader and the searcher
        IdxReader ireader = new IdxReader(INDEX_FOLDERNAME);
        IdxSearcher isearcher = new IdxSearcher(ireader);

        // Make the queries for each topic query
        QueryComputation queryTopics = new QueryComputation(ireader, isearcher, topics, n);
        long start = System.currentTimeMillis();
        Map<Integer, List<TopDocument>> topicsTopDocs = queryTopics.query(typeQuery);
        long end = System.currentTimeMillis();

        // Generate the results
        generateResults(topicsTopDocs, COLLECTION_PATH.toString() + "/round5-submission.txt", n);

        // Compute MAP@k metric
        float mAPk = meanAveragePrecision(topicsTopDocs, topicRelevDocs, k);
        System.out.println("Final result: " + mAPk);
        System.out.println("Execution time (seconds): " + (end-start)*0.001);
    }




}


