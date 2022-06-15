package es.udc.fi.irdatos.c2122.cords;

import es.udc.fi.irdatos.c2122.schemas.TopDocument;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.lucene.index.*;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static java.util.stream.Collectors.toMap;

/**
 * Implements query expansion by adding new terms based on their odds-ratio (probability of occurrence on a relevant
 * document divided by probability of occurrence in a non-relevant document)
 */
public class ProbabilityFeedback {
    private IndexReader ireader;
    private Map<Integer, List<TopDocument>> initialResults;
    private int numTerms;

    /**
     * Class constructor.
     * @param ireader: IndexReader to access to term vectors.
     * @param initialResults: Initial top n documents (that will be considered as true relevant).
     * @param numTerms: Number of terms to expand the initial query.
     */
    public ProbabilityFeedback(IndexReader ireader, Map<Integer, List<TopDocument>> initialResults, int numTerms) {
        this.ireader = ireader;
        this.initialResults = initialResults;
        this.numTerms = numTerms;
    }

    /**
     *
     * @param topDocuments
     * @param fieldname
     * @return
     */
    public List<String> getProbability(List<TopDocument> topDocuments, String fieldname) {
        Double numDocs = (double) ireader.numDocs();
        Double numRelDocs = (double) topDocuments.size();

        Map<String, Double> relDocFrequencies = new HashMap<>();   // store relevant document frequency per term
        Map<String, Double> docFrequencies = new HashMap<>();      // store document frequency per term

        for (TopDocument doc : topDocuments) {
            // Read terms of the document
            int docID = doc.docID();
            Terms vector;
            try {
                vector = ireader.getTermVector(docID, fieldname);
            } catch (IOException e) {
                System.out.println("IOException while reading term vector of document " + docID);
                e.printStackTrace();
                return null;
            }

            if (Objects.isNull(vector)) {
                continue;
            }

            // Update relDocFrequencies and docFrequencies dictionaries
            TermsEnum termsEnum;
            BytesRef text;
            try {
                termsEnum = vector.iterator();
                while ((text = termsEnum.next()) != null) {
                    String term = text.utf8ToString();
                    if (relDocFrequencies.containsKey(term)) {
                        relDocFrequencies.put(term, relDocFrequencies.get(term) + 1.0);
                    } {
                        relDocFrequencies.put(term, 1.0);
                        docFrequencies.put(term, (double) ireader.docFreq(new Term(fieldname, term)));
                    }
                }
            } catch (IOException e) {
                e.printStackTrace(); return null;
            }
        }

        Map<String, Double> scores = new HashMap<>();
        for (String term : relDocFrequencies.keySet()) {
            Double VRt = relDocFrequencies.get(term);     // Number of relevant documents where the term appears
            Double dft = docFrequencies.get(term);        // Number of documents (in the complete collection) where the term appears
            scores.put(term, Math.log((VRt + 0.5) / (numRelDocs - VRt + 1)) + Math.log(numDocs/dft));
        }

        // Sort scores by value
        List<String> sortedTerms = scores
                .entrySet()
                .stream()
                .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
                .map(x -> x.getKey()).toList();

        // Return the list of numTerms terms with the highest score
        return sortedTerms.subList(0, Math.min(numTerms, sortedTerms.size()));
    }

    /**
     * Compute the probability score for each topic.
     * @param fieldname: Name of the field in which the query expansion is computed.
     * @returns: Map object with new query terms for each topic.
     */
    public Map<Integer, List<String>> getProbabilities(String fieldname) {
        Map<Integer, List<String>> newTermsTopic = new HashMap<>();
        for (int topicID : initialResults.keySet()) {
            List<String> topTerms = getProbability(initialResults.get(topicID), fieldname);
            newTermsTopic.put(topicID, topTerms);
        }
        return newTermsTopic;
    }

}
