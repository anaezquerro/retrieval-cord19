package models;

import lucene.IdxReader;
import lucene.IdxSearcher;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.lucene.document.Document;
import org.apache.lucene.search.KnnVectorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import schemas.Embedding;
import schemas.TopDocument;
import schemas.TopicQuery;

import java.util.List;

import static util.AuxiliarFunctions.string2vector;

public class VectorModel extends RetrievalModel {
    private IdxReader reader;
    private IdxSearcher searcher;
    private double alpha;
    private double beta;
    private double gamma;
    private int iterations;


    public VectorModel(IdxReader reader, IdxSearcher searcher, double alpha, double beta, double gamma, int iterations) {
        super(reader, searcher);
        this.alpha = alpha;
        this.beta = beta;
        this.gamma = gamma;
        this.iterations = iterations;
    }

    public List<TopDocument> query(TopicQuery topicQuery, int topN) {
        List<TopDocument> results;
        Embedding embedding = topicQuery.embedding();
        for (int iter=0; iter < (iterations-1); iter++) {
            results = vectorQuery(embedding, topN);
            embedding = rocchio(embedding, results);
        }
        return vectorQuery(embedding, topN);
    }

    private List<TopDocument> vectorQuery(Embedding embedding, int topN) {
        Query knnQuery = new KnnVectorQuery("embedding", embedding.getFloat(), topN);
        TopDocs topDocs = searcher.search(knnQuery, topN);
        return super.coerce(topDocs, topN);
    }

    private Embedding rocchio(Embedding queryEmbedding, List<TopDocument> relevantSet) {
        ArrayRealVector sumRelevant = new ArrayRealVector(queryEmbedding.size());
        ArrayRealVector sumNonRelevant = new ArrayRealVector(queryEmbedding.size());
        int countRelevant = 0;
        int countNonRelevant = 0;

        List<String> cordUIDrel = relevantSet.stream().map(doc -> doc.cordUID()).toList();

        for (int docID = 0; docID < reader.numDocs(); docID++) {
            Document doc = reader.document(docID);
            if (cordUIDrel.contains(doc.get("cordUID"))) {
                sumRelevant.add(string2vector(doc.get("embedding"), " "));
                countRelevant++;
            } else {
                sumNonRelevant.add(string2vector(doc.get("embedding"), " "));
                countNonRelevant++;
            }
        }

        // compute the new query

        ArrayRealVector newQueryEmbedding = (ArrayRealVector) queryEmbedding.getArray().mapMultiply(alpha);
        newQueryEmbedding = newQueryEmbedding.add(
                sumRelevant.mapMultiply(beta/countRelevant)
        );
        newQueryEmbedding = newQueryEmbedding.subtract(
                sumNonRelevant.mapMultiply(gamma/countNonRelevant)
        );
        return new Embedding(newQueryEmbedding);
    }
}
