package es.udc.fi.irdatos.c2122.cords;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import static es.udc.fi.irdatos.c2122.cords.CollectionReader.readQueryEmbeddings;
import static es.udc.fi.irdatos.c2122.cords.CollectionReader.readDocEmbeddings;



public class ReadCosineSimilarity {
    private static final Map<String, Integer> docEmbeddings = readDocEmbeddings();


    public static void main(String[] args) throws IOException {
        Map<String, List<Double>> queryEmbeddings = readQueryEmbeddings();
        for (Map.Entry<String, List<Double>> queryEmbedding : queryEmbeddings.entrySet()) {
            System.out.println(queryEmbedding.getKey() + ": " + queryEmbedding.getValue().size());
        }

    public static float cosineSimilarity(String doc, Double[] query) {
            
        }




    }
}

