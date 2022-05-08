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

    public ProbabilityFeedback(IndexReader ireader, Map<Integer, List<TopDocument>> initialResults) {
        this.ireader = ireader;
        this.initialResults = initialResults;
    }

    public Map<String, Double> getProbabilities(List<TopDocument> topicsTopDocs) {
        Map<String, Double> probs = new HashMap<>();
        Map<String, Double> probsComp = new HashMap<>();
        Double N = (double) ireader.numDocs();
        Double VR = (double) topicsTopDocs.size();

        for (TopDocument doc : topicsTopDocs) {
            int docCID = doc.docCID();
            Terms vector;
            try {
                vector = ireader.getTermVector(docCID, "abstract");
            } catch (IOException e) {
                e.printStackTrace();
                return null;
            }

            if (!Objects.isNull(vector)) {
                TermsEnum termsEnum;
                BytesRef text;
                try {
                    termsEnum = vector.iterator();

                    while (!Objects.isNull(text = termsEnum.next())) {
                        String term = text.utf8ToString();
                        if (probs.containsKey(term)) {
                            probs.put(term, probs.get(term) + 1.0);
                        } else {
                            probs.put(term, 1.0);
                            probsComp.put(term, (double) ireader.docFreq(new Term("abstract", term)));
                        }
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                    return null;
                }
            }
        }
        Map<String, Double> scores = new HashMap<>();
        for (String key : probs.keySet()) {
            Double VRt = probs.get(key);
            Double dft = probsComp.get(key);

            probs.put(key, VRt/VR);
            probsComp.put(key, (dft-VRt)/(N-VR));
            scores.put(key, Math.log10((VRt+1.0/2.0)/(VR-VRt+1))+Math.log10(N/dft));
        }
        Map<String, Double> sortedScores = scores
                .entrySet()
                .stream()
                .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
                .collect(
                        toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e2,
                                LinkedHashMap::new));

        return sortedScores;
    }

    public static void main(String[] args)  {

        ReaderSearcher creation = new ReaderSearcher(Paths.get(PoolIndexing.INDEX_FOLDERNAME), PoolIndexing.similarity);
        IndexReader reader = creation.reader();


        Map<Integer, List<TopDocument>> temporal = new HashMap<>();
        for (int i = 0; i < 20; i++) {
            temporal.put(1, new ArrayList<>());
            TopDocument newDoc = new TopDocument("aa", 5.0, 1);
            newDoc.setCID(i);
            temporal.get(1).add(newDoc);
        }

        ProbabilityFeedback prob = new ProbabilityFeedback(reader, temporal);
        Map<String, Double> fin = prob.getProbabilities(temporal.get(1));
        System.out.println(fin);
    }

}
