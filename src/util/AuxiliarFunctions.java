package util;

import org.apache.commons.io.FileUtils;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.LockObtainFailedException;
import schemas.CompressedRefsVector;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.stream.IntStream;

public class AuxiliarFunctions {

    /**
     * Deletes folder if exists.
     * @param foldername Name of the folder.
     */
    public static Path deleteFolder(String foldername) {
        if (new File(foldername).exists()) {
            System.out.println("The path " + foldername + " does exist. Deleting it...");
            try {
                FileUtils.deleteDirectory(new File(foldername));
            } catch (IOException e) {
                System.out.println("IOException while removing " + foldername + " folder");
            }
        }

        return Paths.get(foldername);
    }

    /**
     * Deletes all files in a folder.
     * @param foldername Name of the folder.
     */
    public static void deleteFiles(String foldername) {
        String[] files = new File(foldername).list();
        for (String file : files) {
            new File(file).delete();
        }
    }

    public static boolean exists(String path) {
        return (new File(path).exists());
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


    public static String vector2string(ArrayRealVector vector, String sep) {
        String[] stringVector = new String[vector.getDimension()];
        IntStream.range(0, vector.getDimension()).forEach(
                i -> {stringVector[i] = String.valueOf(vector.getEntry(i));}
        );
        return String.join(sep, stringVector);
    }

    public static ArrayRealVector string2vector(String content, String sep) {
        ArrayRealVector vector = new ArrayRealVector(
                Arrays.stream(content.split(sep)).mapToDouble(x -> Double.parseDouble(x)).toArray()
        );
        return vector;
    }

    public static void renameFolder(String oldName, String newName) {
        File oldFolder = new File(oldName);
        File newFolder = new File(newName);

        if (oldFolder.renameTo(newFolder)) {
            System.out.println(oldName + " has been renamed to " + newName);
        } else {
            System.out.println("Failed to rename directory");
        }
    }

    public static void duplicateFolder(String sourcePath, String targetPath) {
        File sourceDirectory = new File(sourcePath);
        File targetDirectory = new File(targetPath);
        try {
            FileUtils.copyDirectory(sourceDirectory, targetDirectory);
        } catch (IOException e) {
            System.out.println("IOException while copying " + sourcePath + " -> " + targetPath);
            e.printStackTrace();
        }
        System.out.println(sourcePath + " has been duplicated to " + targetPath);
    }
}
