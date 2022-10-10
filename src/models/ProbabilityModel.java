package models;

import lucene.IdxReader;
import lucene.IdxSearcher;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import schemas.TopDocument;
import schemas.TopicQuery;

import java.io.IOException;
import java.util.*;

public class ProbabilityModel extends RetrievalModel {
    private IdxReader reader;
    private IdxSearcher searcher;
    private int numTerms;
    private BooleanModel baseModel;
    private String[] expandFields;


    public ProbabilityModel(IdxReader reader, IdxSearcher searcher, int numTerms, String[] expandFields) {
        super(reader, searcher);
        this.reader = reader;
        this.searcher = searcher;
        baseModel = new BooleanModel(reader, searcher);
        this.numTerms = numTerms;
        this.expandFields = expandFields;
    }

    public List<TopDocument> query(TopicQuery topicQuery, int topN) {
        TopicQuery tempQuery = topicQuery.copy();
        List<TopDocument> initialResults = baseModel.query(tempQuery, topN);

        for (Map.Entry<String, String> entry : tempQuery.fieldTexts().entrySet()) {
            String expanded = entry.getValue() + " " +
                    String.join(" ", expand(initialResults, entry.getKey()));
            tempQuery.putField(entry.getKey(), expanded);
        }

        List<TopDocument> finalResults = baseModel.query(tempQuery, topN);
        return finalResults;
    }

    private List<String> expand(List<TopDocument> topDocuments, String fieldname) {
        Double numDocs = (double) reader.numDocs();
        Double numRelDocs = (double) topDocuments.size();

        Map<String, Double> relDocFrequencies = new HashMap<>();   // store relevant document frequency per term
        Map<String, Double> docFrequencies = new HashMap<>();      // store document frequency per term

        for (TopDocument topDoc : topDocuments) {
            // Read terms of the document
            int docID = topDoc.docID();
            Terms vector = reader.getTermVector(docID, fieldname);

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
                    } else {
                        relDocFrequencies.put(term, 1.0);
                        docFrequencies.put(term, reader.docFreq(new Term(fieldname, term)));
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
                return null;
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
                .sorted(Map.Entry.comparingByValue())
                .map(x -> x.getKey()).toList();
        // Return the list of numTerms terms with the highest score
        return sortedTerms.subList(0, Math.min(numTerms, sortedTerms.size()));
    }



}
