package es.udc.fi.irdatos.c2122.cords;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import es.udc.fi.irdatos.c2122.schemas.Article;
import es.udc.fi.irdatos.c2122.schemas.Metadata;
import es.udc.fi.irdatos.c2122.schemas.RelevanceJudgements;
import es.udc.fi.irdatos.c2122.schemas.Topics;
import es.udc.fi.irdatos.c2122.util.ObjectReaderUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Stream;
import org.apache.commons.math3.linear.ArrayRealVector;


/**
 * Implements reading and parsing methods for TREC-COVID Collection.
 */
public class CollectionReader {
    // Path global variables
    private static final Path DEFAULT_COLLECTION_PATH = Paths.get("2020-07-16");
    private static String METADATA_FILENAME = "metadata.csv";
    private static String QUERY_EMBEDDINGS_FILENAME = "query_embeddings.json";
    private static String DOC_EMBEDDINGS_FILENAME = "cord_19_embeddings_2020-07-16.csv";
    private static String TOPICS_FILENAME = "topics_set.xml";
    private static String RELEVANCE_JUDGEMENTS_FILENAME = "relevance_judgements.txt";

    // Readers
    private static final ObjectReader ARTICLE_READER = JsonMapper.builder().findAndAddModules().build().readerFor(Article.class);
    private static final ObjectReader TOPICS_READER = XmlMapper.builder().findAndAddModules().build().readerFor(Topics.class);



    /**
     * Parses the content of the JSON files with the given structure in the class Article.java
     * @param articlePath Path of the JSON file.
     * @returns String array with the body text of the JSON file, references notes of the article and figure/table notes.
     */
    public static final String[] readArticle(Path articlePath) {
        Article article;
        try {
            article = ARTICLE_READER.readValue(articlePath.toFile());
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
        Path metadataPath = DEFAULT_COLLECTION_PATH.resolve(METADATA_FILENAME);
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


    /**
     * Parses query embeddings file and returns for each topic query identifier the embedding vector.
     * @returns Map object formed by (topicID, queryEmbedding) pairs.
     */
    public static final Map<Integer, ArrayRealVector> readQueryEmbeddings() {
        Map<String, List<Double>> queryEmbeddingsString = null;
        try {
            queryEmbeddingsString = new ObjectMapper().readValue(
                    DEFAULT_COLLECTION_PATH.resolve(QUERY_EMBEDDINGS_FILENAME).toFile(), Map.class);
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("IOException while reading query embeddings JSON file");
        }
        Map<Integer, ArrayRealVector> queryEmbeddings = new HashMap<>();
        for (Map.Entry<String, List<Double>> query : queryEmbeddingsString.entrySet()) {
            Integer topicID = Integer.parseInt(query.getKey());
            Double[] embeddings = query.getValue().toArray(new Double[query.getValue().size()]);
            queryEmbeddings.put(topicID, new ArrayRealVector(embeddings));
        }
        return queryEmbeddings;
    }

    public static final Stream<String> streamDocEmbeddings() {
        Stream<String> docEmbeddingsStream = null;
        try {
            docEmbeddingsStream = Files.lines(DEFAULT_COLLECTION_PATH.resolve(DOC_EMBEDDINGS_FILENAME));
        } catch (IOException e) {
            System.out.println("IOException while creating document embeddings stream");
            e.printStackTrace();
        }
        return docEmbeddingsStream;
    }


    /**
     * Reads and parses topics set using the defined structure in Topics.java file.
     * @returns A 50-length array with information about each topic, stored with the Topics.Topic structure.
     */
    public static final Topics.Topic[] readTopicSet() {
        // Define topics path
        Path topicsPath = DEFAULT_COLLECTION_PATH.resolve(TOPICS_FILENAME);

        // Use Topics and Topics.Topic structure to parse the topic set information
        Topics.Topic[] topics;
        try {
            Topics topicsList = TOPICS_READER.readValue(topicsPath.toFile());
            topics = topicsList.topic();
        } catch (IOException e) {
            System.out.println("IOException while reading topics in: " + topicsPath.toString());
            return null;
        }

        // Returns an array consisted of each topic information (number, query, question and narrative)
        return topics;
    }

    /**
     * Reads and parses relevance judgements.
     * @returns Map object where each key is a topic ID with its corresponding list of relevant documents identificers.
     */
    public static final Map<Integer, List<String>> readRelevanceJudgements() {
        // Define relevance judgments path
        Path collectionPath = DEFAULT_COLLECTION_PATH;
        Path relevanceJudgementsPath = collectionPath.resolve(RELEVANCE_JUDGEMENTS_FILENAME);

        // Read an parse relevance judgments file
        CsvSchema schema = CsvSchema.builder().setColumnSeparator(' ').addColumn("topicID").addColumn("rank").addColumn("docID").addColumn("score").build();
        ObjectReader reader = new CsvMapper().readerFor(RelevanceJudgements.class).with(schema);

        // Creating a list with each relevance judgments using the defined structure in RelevanceJudgements.java
        // (topicID, docID, score)
        List<RelevanceJudgements> docsRelevance;
        try {
            docsRelevance = ObjectReaderUtils.readAllValues(relevanceJudgementsPath, reader);
        } catch (IOException e) {
            System.out.println("IOException while reading relevance judgments in " + relevanceJudgementsPath.toString());
            e.printStackTrace();
            return null;
        }

        // Create the Map object where each topic ID is stored with its corresponding list of relevant documents identifiers
        Map<Integer, List<String>> topicRelevDocs = new HashMap<>();
        for (int i=1; i < 51; i++) {
            List<String> emptyList = new ArrayList<>();    // firstly create an empty list
            topicRelevDocs.put(i, emptyList);                    // add to the map object the index i with the empty list
        }

        // Read the relevance judgments list and add in the list of each topicID the corresponding document identifier
        for (RelevanceJudgements doc : docsRelevance) {
            // We do not care if the score is 1 or 2 to assess its relevance
            if (doc.score() != 0) {
                topicRelevDocs.get(doc.topicID()).add(doc.docID());
            }
        }
        return topicRelevDocs;
    }


}
