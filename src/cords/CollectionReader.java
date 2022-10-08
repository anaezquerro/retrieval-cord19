package cords;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import formats.Article;
import formats.Metadata;
import formats.RelevanceJudgements;
import formats.Topics;
import schemas.*;
import util.ObjectReaderUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static schemas.ParsedArticle.AUTHORS_SEPARATOR;

/**
 * Implements parsing of the TREC-COVID Collection files adapting their content to the desired structure.
 */
public class CollectionReader {
    /* Global variables (paths):
    COLLECTION_PATH         [Path]    : Relative path to the TREC-COVID Collection.
    METADATA_FILENAME       [String]  : Relative path from COLLECTION_PATH to the metadata.csv file.
    QEMBEDDINGS_FILENAME    [String]  : Relative path from COLLECTION_PATH to the query embeddings.json file.
    DOCEMBEDDINGS_FILENAME  [String]  : Relative path from COLLECTION_PATH to the document embeddings CSV file.
    TOPICS_FILENAME         [String]  : Relative path from COLLECTION_PATH to the topics set XML file.
    RELJUDGS_FILENAME       [String]  : Relative path form COLLECTION_PATH to the relevance judgements TXT file.
     */

    public static final Path COLLECTION_PATH = Paths.get("2020-07-16");
    public static final String METADATA_FILENAME = "metadata.csv";
    public static final String QUERY_EMBEDDINGS_FILENAME = "topics-embeddings.json";
    public static final String DOC_EMBEDDINGS_FILENAME = "embeddings.csv";
    public static final String TOPICS_FILENAME = "topics-set.xml";
    public static final String RELEVANCE_JUDGEMENTS_FILENAME = "relevance-judgements.txt";

    /* Global variables (readers):
    ARTICLE_READER     [ObjectReader]  : JSON reader for the files in `2020-07-16/document_parses/`
    TOPICS_READER      [ObjectReader]  : XML reader for `2020-07-16/topics-set.xml`
    METADATA_SCHEMA    [CsvSchema]     : CSV schema for 2020-07-16/metadata.csv.
    METADATA_READER    [ObjectReader]  : Relative path from COLLECTION_PATH to the document embeddings CSV file.
     */
    public static final ObjectReader ARTICLE_READER = JsonMapper.builder().findAndAddModules().build().readerFor(Article.class);
    public static final ObjectReader TOPICS_READER = XmlMapper.builder().findAndAddModules().build().readerFor(Topics.class);
    public static final CsvSchema METADATA_SCHEMA = CsvSchema.emptySchema().withHeader().withArrayElementSeparator("; ");
    public static final ObjectReader METADATA_READER = new CsvMapper().readerFor(Metadata.class).with(METADATA_SCHEMA);

    // ------------------------------------------------ document_parses ------------------------------------------------

    /**
     * Deletes invalid string sequences in order to allow the function parseQuery() to parse it.
     *
     * @param text Text to validate.
     * @return Parsed text.
     */
    public static String parse(String text) {
        String parsedText = text.replaceAll("\\[|\\]|\\(|\\)|/|-|\\'|\\:|\\\\|\"|\\}|\\{|\\*|\\?|\\!|\\^|\\~|\\+|\\;", " ");
        parsedText = " " + parsedText;
        parsedText = parsedText.replaceAll(" and| or| the| at| of| a| in| OR| AND", " ");
        parsedText = String.join(" ", parsedText.strip().split("\\s+"));
        return parsedText;
    }

    /**
     * Given a list of [Author] authors (obtained from reading a JSON file with Article schema), concatenates last names
     * of the first n authors of the list.
     *
     * @param authors    List of Author.
     * @param numAuthors Number of authors that must be saved in a single string.
     * @returns String of the first `numAuthors` last names.
     */
    private static String parseAuthors(List<Article.Author> authors, int numAuthors) {
        StringBuilder builderAuthors = new StringBuilder();
        for (int i = 0; i < Math.min(numAuthors, authors.size()); i++) {
            Article.Author author = authors.get(i);
            if (author.last().length() == 0) {
                continue;
            }
            builderAuthors.append(author.last() + AUTHORS_SEPARATOR);
        }
        String parsedAuthors = parse(builderAuthors.toString());
        return parsedAuthors;
    }


    /**
     * Given an Article scheme, parses its content to obtain the body, authors and parsed references.
     *
     * @param articlePath : Path to the JSON file.
     * @return ParsedArticle.
     */
    public static ParsedArticle parseArticle(Path articlePath) {
        // -------------------------- READING --------------------------
        Article article;
        try {
            article = ARTICLE_READER.readValue(articlePath.toFile());
        } catch (IOException e) {
            System.out.println("IOException while reading JSON file " + articlePath.toString());
            e.printStackTrace();
            return null;
        }

        // -------------------------- PARSING --------------------------
        StringBuilder bodyBuilder = new StringBuilder();
        Map<String, ParsedArticle.ParsedReference> parsedReferences = new HashMap<>();

        // add bibliography entries to parsedReferences
        for (Map.Entry<String, Article.Reference> bibEntry : article.bib_entries().entrySet()) {
            String refTitle = parse(bibEntry.getValue().title());
            if (refTitle.length() == 0) {
                continue;
            }
            String refAuthors = parseAuthors(bibEntry.getValue().authors(), ParsedArticle.NUM_AUTHORS_PARSED);
            if (refAuthors.length() == 0) {
                continue;
            }
            parsedReferences.put(bibEntry.getKey(), new ParsedArticle.ParsedReference(refTitle, refAuthors));
        }

        // add article text and count references to update the counts
        for (Article.Content paragraph : article.body_text()) {
            bodyBuilder.append(paragraph.section() + "\n" + paragraph.text() + "\n");
            for (Article.Content.Cite cite : paragraph.cite_spans()) {
                if (parsedReferences.containsKey(cite.ref_id())) {
                    parsedReferences.get(cite.ref_id()).increaseCount();
                }
            }
        }
        List<ParsedArticle.ParsedReference> references = parsedReferences.values().stream().toList();
        String body = bodyBuilder.toString();

        // add authors last name
        String authors;
        if (!Objects.isNull(article.metadata().authors())) {
            authors = parseAuthors(article.metadata().authors(), article.metadata().authors().size());
        } else {
            authors = "";
        }

        return new ParsedArticle(null, null, body, authors, references);
    }

    /**
     * Given multiple Article instances, parses and merges their contents to obtain an unique body,
     * set of authors and set of references.
     *
     * @param articlePaths List of paths to JSON files.
     * @return ParsedArticle.
     */
    public static ParsedArticle parseArticles(List<Path> articlePaths) {
        ParsedArticle completeArticle = new ParsedArticle(null, null, "", "", new ArrayList<>());
        ParsedArticle partialArticle;
        for (Path article : articlePaths) {
            partialArticle = parseArticle(article);
            completeArticle.addBody("\n" + partialArticle.body());
            String partialAuthors = String.join(AUTHORS_SEPARATOR,
                    Arrays.stream(partialArticle.authors().split(AUTHORS_SEPARATOR))
                            .filter(x -> !completeArticle.authors().contains(x))
                            .toList());
            completeArticle.addAuthors(AUTHORS_SEPARATOR + partialAuthors);
            completeArticle.addReferences(partialArticle.references());
        }
        return completeArticle;
    }

    // ------------------------------------------------- metadata.csv --------------------------------------------------

    /**
     * From a Metadata instance (row information in metadata.csv), reads the corresponding JSON file or files to return
     * its content.
     *
     * @param rowMetadata Metadata object.
     * @returns Parsed article with all the content.
     */
    public static final ParsedArticle parseRowMetadata(Metadata rowMetadata) {
        ParsedArticle parsedArticle;
        if (rowMetadata.pmcFile().length() != 0) {
            parsedArticle = parseArticle(COLLECTION_PATH.resolve(rowMetadata.pmcFile()));
        } else if (rowMetadata.pdfFiles().size() >= 1) {
            parsedArticle = parseArticles(
                    rowMetadata.pdfFiles().stream().map(pdfPath -> COLLECTION_PATH.resolve(pdfPath)).toList()
            );
        } else {
            return null;
        }
        parsedArticle.setTitle(rowMetadata.title());
        parsedArticle.setAbstract(rowMetadata.abstrac());
        return parsedArticle;
    }


    /**
     * Parses the metadata.csv file with the given structure in Metadata.java
     *
     * @returns List of ParsedArticle objects representing each article specified in the collection.
     */
    public static final List<Metadata> readMetadata() {
        Path metadataPath = COLLECTION_PATH.resolve(METADATA_FILENAME);
        List<Metadata> metadata;
        try {
            metadata = ObjectReaderUtils.readAllValues(metadataPath, METADATA_READER);
        } catch (IOException e) {
            System.out.println("IOException while reading metadata in " + metadataPath.toString());
            e.printStackTrace();
            return null;
        }
        return metadata;
    }


    // ------------------------------------------------- topics-set ---------------------------------------------------

    /**
     * Reads and parses topics set using the defined structure in Topics.java file.
     *
     * @returns A 50-length array with information about each topic, stored with the Topics.Topic structure.
     */
    public static final List<TopicQuery> readTopics() {
        Map<Integer, Embedding> topicEmbeddings = parseTopicEmbeddings();

        // Define topics path
        Path topicsPath = COLLECTION_PATH.resolve(TOPICS_FILENAME);

        // Use Topics and Topics.Topic structure to parse the topic set information
        Topics.Topic[] topics;
        try {
            Topics topicsList = TOPICS_READER.readValue(topicsPath.toFile());
            topics = topicsList.topic();
        } catch (IOException e) {
            System.out.println("IOException while reading topics in: " + topicsPath.toString());
            return null;
        }

        List<TopicQuery> topicsQuery = new ArrayList<>();
        for (int topicID : topicEmbeddings.keySet()) {
            topicsQuery.add(
                    new TopicQuery(topicID, topics[topicID].query(), topicEmbeddings.get(topicID))
            );
        }
        return topicsQuery;
    }

    // ----------------------------------------------- topics-embeddings -----------------------------------------------

    /**
     * Parses topic embeddings JSON file and returns for each topic query identifier the embedding vector.
     *
     * @returns Map object formed by (topicID, topicEmbedding) pairs.
     */
    public static final Map<Integer, Embedding> parseTopicEmbeddings() {
        Map<String, String> content = null;
        try {
            content = new ObjectMapper().readValue(
                    COLLECTION_PATH.resolve(QUERY_EMBEDDINGS_FILENAME).toFile(), Map.class);
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("IOException while reading query embeddings JSON file");
        }
        Map<Integer, Embedding> topicEmbeddings = content.entrySet().stream().collect(
                Collectors.toMap(key -> Integer.parseInt(key.getKey()), value -> new Embedding(value.getValue().split(" ")))
        );

        return topicEmbeddings;
    }

    // ------------------------------------------------- doc-embeddings ------------------------------------------------

    public static final Stream<String> streamDocEmbeddings() {
        Stream<String> docEmbeddingsStream = null;
        try {
            docEmbeddingsStream = Files.lines(COLLECTION_PATH.resolve(DOC_EMBEDDINGS_FILENAME));
        } catch (IOException e) {
            System.out.println("IOException while creating document embeddings stream");
            e.printStackTrace();
        }
        return docEmbeddingsStream;
    }


    public static Map<String, Embedding> readDocEmbeddings() {
        Stream<String> stream = streamDocEmbeddings();
        Map<String, Embedding> docEmbeddings = new HashMap<>();
        for (Iterator<String> it = stream.iterator(); it.hasNext(); ) {
            String[] lineContent = it.next().split(",");
            Embedding embedding = new Embedding(Arrays.copyOfRange(lineContent, 1, lineContent.length));
            docEmbeddings.put(lineContent[0], embedding);
        }
        return docEmbeddings;
    }


    // --------------------------------------------- relevance-judgements ----------------------------------------------

    /**
     * Reads and parses relevance judgements TXT file.
     *
     * @returns Map object where each key is a topic ID with its corresponding list of relevant documents identifiers.
     */
    public static final Map<Integer, List<String>> readRelevanceJudgements() {
        // Define relevance judgments path
        Path relevanceJudgementsPath = COLLECTION_PATH.resolve(RELEVANCE_JUDGEMENTS_FILENAME);

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
        for (int i = 1; i < 51; i++) {
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

    public static void main(String[] args) {
        Map<Integer, Embedding> topicEmbeddings = parseTopicEmbeddings();
        System.out.println(topicEmbeddings.get(0).toString());
    }

}
