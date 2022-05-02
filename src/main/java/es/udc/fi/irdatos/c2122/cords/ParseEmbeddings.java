package es.udc.fi.irdatos.c2122.cords;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.ObjectMapper;

public class ParseEmbeddings {

    public static void main(String[] args) throws IOException {
        Path DEFAULT_COLLECTION_PATH = Paths.get("2020-07-16");
        Path queryEmbeddingsFile = DEFAULT_COLLECTION_PATH.resolve("query_embeddings.json");
        Map<String, Double[]> queryEmbeddings = new ObjectMapper().readValue(queryEmbeddingsFile.toFile(), Map.class);

        for (Map.Entry<String, Double[]> queryEmbedding : queryEmbeddings.entrySet()) {
            System.out.println("Query embedding detected for query " + queryEmbedding.getKey());
        }

        Path docEmbeddingsFile = DEFAULT_COLLECTION_PATH.resolve("cord_19_embeddings_2020-07-16.csv");
        Map<String, String[]> docEmbeddings = new HashMap<>();
        Stream<String> stream = null;
        try {
            stream = Files.lines(docEmbeddingsFile, StandardCharsets.UTF_8);
        } catch (IOException e) {
            System.out.println("Error");
        }

        stream.forEach(line -> {
            if (line.contains(",")) {
                try {

                    docEmbeddings.put(line.substring(0, line.indexOf(",")),
                            line.substring(line.indexOf(",")).split(","));
                } catch (Exception e) {
                    System.out.println(line);
                }
            }
        });

//                Arrays.stream(line.substring(line.indexOf(",")).split(",")).map(x -> Double.parseDouble(x)).toArray(Double[]::new)));

        System.out.println(docEmbeddings.toString().substring(0, 10000));



    }
}

