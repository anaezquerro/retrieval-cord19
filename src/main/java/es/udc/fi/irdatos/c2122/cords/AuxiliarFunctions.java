package es.udc.fi.irdatos.c2122.cords;

import org.apache.commons.io.FileUtils;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.LockObtainFailedException;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class AuxiliarFunctions {

    public static Path deleteFolder(String foldername) {
        if (new File(foldername).exists()) {
            try {
                FileUtils.deleteDirectory(new File(foldername));
            } catch (IOException e) {
                System.out.println("IOException while removing " + foldername + " folder");
            }
        }
        return Paths.get(foldername);
    }

    public static FileWriter createFileWriter(String filename) {
        try {
            if (new File(filename).exists()) {
                Files.delete(Paths.get(filename));
            }
            FileWriter fwriter = new FileWriter(Paths.get(filename).toFile());
            return fwriter;
        } catch (IOException e){e.printStackTrace();return null;}
    }

    public static void createFolder(String foldername) {
        deleteFolder(foldername);
        try {
            Files.createDirectory(Paths.get(foldername));
        } catch (IOException e) {e.printStackTrace();return;}
    }

    public static Integer[] coalesce(int numWorkers, int N) {
        int futuresPerWorker = (int) Math.floor((double) N / (double) numWorkers);
        int surplus = Math.floorMod(N, numWorkers);

        Integer[] indexes = new Integer[numWorkers + 1];
        indexes[0] = 0;
        for (int i = 1; i <= numWorkers; i++) {
            if (i <= surplus) {
                indexes[i] = indexes[i - 1] + futuresPerWorker + 1;
            } else {
                indexes[i] = indexes[i - 1] + futuresPerWorker;
            }
        }
        return indexes;
    }


    public static ArrayRealVector floatArray2RealVector(float[] arr) {
        ArrayRealVector v = new ArrayRealVector(arr.length);
        for (int i=0; i < arr.length; i++) {
            v.setEntry(i, arr[i]);
        }
        return v;
    }

    public static float[] realVector2floatArray(ArrayRealVector v) {
        float[] arr = new float[v.getDimension()];
        for (int i = 0; i < v.getDimension(); i++) {
            arr[i] = (float) v.getEntry(i);
        }
        return arr;
    }

    public static IndexWriter createIndexWriter(String foldername) {
        IndexWriterConfig config = new IndexWriterConfig(new StandardAnalyzer());
        config.setSimilarity(PoolIndexing.similarity);

        IndexWriter writer = null;
        try {
            writer = new IndexWriter(FSDirectory.open(Paths.get(foldername)), config);
        } catch (CorruptIndexException e) {
            System.out.println("CorruptIndexException while creating IndexWriter at " + foldername);
            e.printStackTrace();
        } catch (LockObtainFailedException e) {
            System.out.println("LockObtainFailedException while creating IndexWriter at " + foldername);
            e.printStackTrace();
        } catch (IOException e) {
            System.out.println("IOException while creating IndexWriter at " + foldername);
            e.printStackTrace();
        }
        return writer;
    }

}
