package es.udc.fi.irdatos.c2122.cords;

import es.udc.fi.irdatos.c2122.schemas.TopDocument;
import org.apache.lucene.index.*;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.similarities.LMJelinekMercerSimilarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static java.util.stream.Collectors.toMap;

public class ProbabilityFeedback {
    private static Path INDEX_PATH = Paths.get("Index-StandardAnalyzer-LM");
    private IndexReader ireader;
    private Map<Integer, List<TopDocument>> initialResults;
    private int numTerms;

    public ProbabilityFeedback(IndexReader ireader, Map<Integer, List<TopDocument>> initialResults, int numTerms) {
        this.ireader = ireader;
        this.initialResults = initialResults;
        this.numTerms = numTerms;
    }

    public List<String> getProbability(List<TopDocument> topDocuments, String fieldname) {
        Map<String, Double> probs = new HashMap<>();
        Map<String, Double> probsComp = new HashMap<>();
        Double N = (double) ireader.numDocs();
        Double VR = (double) topDocuments.size();

        for (TopDocument doc : topDocuments) {
            int docCID = doc.docCID();
            Terms vector;
            try {
                vector = ireader.getTermVector(docCID, fieldname);
            } catch (IOException e) {
                e.printStackTrace();
                return null;
            }
            if (Objects.isNull(vector)) {
                continue;
            }

            TermsEnum termsEnum;
            BytesRef text;
            try {
                termsEnum = vector.iterator();
                while ((text = termsEnum.next()) != null) {
                    String term = text.utf8ToString();
                    if (probs.containsKey(term)) {
                        probs.put(term, probs.get(term) + 1.0);
                    } else {
                        probs.put(term, 1.0);
                        probsComp.put(term, (double) ireader.docFreq(new Term(fieldname, term)));
                    }
                }
            } catch (IOException e) { e.printStackTrace(); return null;}
        }


        Map<String, Double> scores = new HashMap<>();
        for (String key : probs.keySet()) {
            Double VRt = probs.get(key);
            Double dft = probsComp.get(key);
            scores.put(key, Math.log10((VRt+1.0/2.0)/(VR-VRt+1))+Math.log10(N/dft));
        }

        List<String> sortedTerms = scores
                .entrySet()
                .stream()
                .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
                .map(x -> x.getKey()).toList();

        return sortedTerms.subList(0, Math.min(numTerms, sortedTerms.size()));
    }

    public Map<Integer, List<String>> getProbabilities(String fieldname) {
        Map<Integer, List<String>> newTermsTopic = new HashMap<>();
        for (int topicID : initialResults.keySet()) {
            List<String> topTerms = getProbability(initialResults.get(topicID), fieldname);
            newTermsTopic.put(topicID, topTerms);
        }
        return newTermsTopic;
    }

}
