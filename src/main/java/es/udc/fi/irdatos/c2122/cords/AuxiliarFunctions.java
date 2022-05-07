package es.udc.fi.irdatos.c2122.cords;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

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

}
