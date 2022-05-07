package es.udc.fi.irdatos.c2122.cords;


import com.ctc.wstx.exc.WstxOutputException;
import es.udc.fi.irdatos.c2122.schemas.Article;
import es.udc.fi.irdatos.c2122.schemas.Metadata;
import es.udc.fi.irdatos.c2122.schemas.TopDocument;
import org.apache.commons.io.FileUtils;
import org.apache.commons.math3.exception.OutOfRangeException;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.util.MathUtils;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.*;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.LockObtainFailedException;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Array;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static es.udc.fi.irdatos.c2122.cords.AuxiliarFunctions.coalesce;
import static es.udc.fi.irdatos.c2122.cords.CollectionReader.readMetadata;
import static es.udc.fi.irdatos.c2122.cords.CollectionReader.streamDocEmbeddings;

public class ObtainTransitionMatrix {
    public static ArrayRealVector computePageRank(List<TopDocument> topDocuments, double alpha, int iterations) {
        Map<String, Integer> docs2index = new HashMap<>();
        for (int i = 0; i < topDocuments.size(); i++) {
            TopDocument topDocument = topDocuments.get(i);
            docs2index.put(topDocument.docID(), i);
        }

        RealMatrix transitionMatrix = MatrixUtils.createRealMatrix(topDocuments.size(), topDocuments.size());

        // Read references file
        Stream<String> stream;
        try { stream = Files.lines(CollectionReader.DEFAULT_COLLECTION_PATH.resolve("references.txt")); }
        catch (IOException e) {e.printStackTrace();return null;}

        for (Iterator<String> it = stream.iterator(); it.hasNext(); ) {
            String line = it.next();
            String[] lineContent = line.split(", ");
            String docID = lineContent[0];
            String[] references = Arrays.copyOfRange(lineContent, 1, lineContent.length);
            for (int j=0; j < references.length; j++) {
                String reference = references[j];
                if (docs2index.containsKey(docID) && docs2index.containsKey(reference)) {
                    try {
                        int indexDocID = docs2index.get(docID);
                        int indexRef = docs2index.get(reference);
                        transitionMatrix.setEntry(indexDocID, indexRef, 1);
                    } catch (OutOfRangeException e) {e.printStackTrace();}
                }
            }
        }
        System.out.println(transitionMatrix);

//        transitionMatrix
        for (int i=0; i < transitionMatrix.getRowDimension(); i++) {
            double normValue = transitionMatrix.getRowVector(i).getL1Norm();
            transitionMatrix.setRowVector(i, transitionMatrix.getRowVector(i).mapMultiply(1/normValue));

        }

        transitionMatrix = transitionMatrix.scalarMultiply(1-alpha);
        transitionMatrix = transitionMatrix.scalarAdd(alpha/transitionMatrix.getRowDimension());


        ArrayRealVector pageRank = new ArrayRealVector(topDocuments.size(), (double) 1/ topDocuments.size());
        ArrayRealVector transition;
        for (int i=0; i< iterations; i++) {
            transition = (ArrayRealVector) transitionMatrix.preMultiply(pageRank);
            if (pageRank.equals(transition)) {
                break;
            } else {
                pageRank = transition;
            }
        }
        return pageRank;
    }

}
