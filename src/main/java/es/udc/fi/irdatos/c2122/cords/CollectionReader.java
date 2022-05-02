package es.udc.fi.irdatos.c2122.cords;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import es.udc.fi.irdatos.c2122.util.ObjectReaderUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Implements reading and parsing methods for TREC-COVID Collection.
 */
public class CollectionReader {
    private static final ObjectReader articleReader = JsonMapper.builder().findAndAddModules().build().readerFor(Article.class);
    private static final Path DEFAULT_COLLECTION_PATH = Paths.get("2020-07-16");
    private static String METADATA_FILE_NAME = "metadata.csv";
    private static String QUERY_EMBEDDINGS_FILENAME = "query_embeddings.json";
    private static String DOC_EMBEDDINGS_FILENAME = "cord_19_embeddings_2020-07-16.csv";



    /**
     * Parses the content of the JSON files with the given structure in the class Article.java
     * @param articlePath Path of the JSON file.
     * @returns String array with the body text of the JSON file, references notes of the article and figure/table notes.
     */
    public static final String[] readArticle(Path articlePath) {
        Article article;
        try {
            article = articleReader.readValue(articlePath.toFile());
        } catch (IOException e) {
            System.out.println("While reading a JSON file an error has occurred: " + articlePath);
            return null;
        }

        StringBuilder articleText = new StringBuilder();

        // add article text
        for (Article.Content content : article.body_text()) {
            articleText.append(content.section());
            articleText.append('\n');
            articleText.append(content.text());
            articleText.append('\n');
        }

        // add article bibliography
        StringBuilder bibliography = new StringBuilder();
        for (Map.Entry<String, Article.Reference> reference : article.bib_entries().entrySet()) {
            if (reference.getValue().title().length() == 0) {
                continue;
            }
            bibliography.append(reference.getValue().title());
            bibliography.append('\n');
        }

        // add article figures
        StringBuilder figures = new StringBuilder();
        for (Map.Entry<String, Article.Figure> figure : article.ref_entries().entrySet()) {
            if (figure.getValue().text().length() == 0) {
                continue;
            }
            figures.append(figure.getValue().text());
            figures.append('\n');
        }

        String[] articleContent = new String[]{articleText.toString(), bibliography.toString(), figures.toString()};
        return articleContent;
    }

    /**
     * Using the readArticle method, parses a set of articles and returns their joined content.
     * @param articlesPath Array of paths to the set of articles which need to be parsed.
     * @returns String array where each element is the concatenation of one specific field of all articles specified in
     * articlesPath.
     */
    public static final String[] readArticles(List<Path> articlesPath) {
        StringBuilder[] articlesContentBuilder = new StringBuilder[3];
        for (int i = 0; i < 3; i++) {
            articlesContentBuilder[i] = new StringBuilder();
        }
        for (Path articlePath : articlesPath) {
            String[] articlesContent = readArticle(articlePath);
            for (int i = 0; i < 3; i++) {
                articlesContentBuilder[i].append(articlesContent[i]);
                articlesContentBuilder[i].append('\n');
            }
        }

        String[] articlesContent = Arrays.stream(articlesContentBuilder).map(builder -> builder.toString()).toArray(String[]::new);
        return articlesContent;
    }

    /**
     * Parses the metadata.csv file with the given structure in Metadata.java
     * @returns List of Metadata objects representing each row of the CSV.
     */
    public static final List<Metadata> readMetadata() {
        Path metadataPath = DEFAULT_COLLECTION_PATH.resolve(METADATA_FILE_NAME);
        CsvSchema schema = CsvSchema.emptySchema().withHeader().withArrayElementSeparator("; ");
        ObjectReader reader = new CsvMapper().readerFor(Metadata.class).with(schema);

        List<Metadata> metadata;
        try {
            metadata = ObjectReaderUtils.readAllValues(metadataPath, reader);
        } catch (IOException e) {
            System.out.println("IOException while reading metadata in " + metadataPath.toString());
            e.printStackTrace();
            return null;
        }
        return metadata;
    }


    public static final Map<String, List<Double>> readQueryEmbeddings() {
        Map<String, List<Double>> queryEmbeddings = null;
        try {
            queryEmbeddings = new ObjectMapper().readValue(
                    DEFAULT_COLLECTION_PATH.resolve(QUERY_EMBEDDINGS_FILENAME).toFile(), Map.class);
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("IOException while reading query embeddings JSON file");
        }
        return queryEmbeddings;
    }

    public static final Map<String, Integer> readDocEmbeddings() {
        Map<String, Integer> docEmbeddings = new HashMap<>();
        Stream<String> stream = null;
        try {
            stream = Files.lines(DEFAULT_COLLECTION_PATH.resolve(DOC_EMBEDDINGS_FILENAME), StandardCharsets.UTF_8);
        } catch (IOException e) {
            System.out.println("IOException while reading document embeddings CSV file");
        }

        int[] index = { 0 };
        stream.forEach(line -> {
            docEmbeddings.put(line.substring(0, line.indexOf(",")), index[0]++);
        });

        return docEmbeddings;
    }

}
