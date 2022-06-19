package es.udc.fi.irdatos.c2122.cords;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import es.udc.fi.irdatos.c2122.schemas.*;
import es.udc.fi.irdatos.c2122.util.ObjectReaderUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.math3.linear.ArrayRealVector;

import static es.udc.fi.irdatos.c2122.cords.AuxiliarFunctions.parse;


/**
 * Implements reading and parsing methods for TREC-COVID Collection.
 */
public class CollectionReader {
    public static final Path DEFAULT_COLLECTION_PATH = Paths.get("2020-07-16");
    public static String METADATA_FILENAME = "metadata.csv";
    public static String QUERY_EMBEDDINGS_FILENAME = "query_embeddings.json";
    public static String DOC_EMBEDDINGS_FILENAME = "cord_19_embeddings_2020-07-16.csv";
    public static String TOPICS_FILENAME = "topics_set.xml";
    public static String RELEVANCE_JUDGEMENTS_FILENAME = "relevance_judgements.txt";
    public static int EMBEDDINGS_DIMENSIONALITY = 768;
    private static int numRefAuthors = 5;

    // Readers
    public static final ObjectReader ARTICLE_READER = JsonMapper.builder().findAndAddModules().build().readerFor(Article.class);
    public static final ObjectReader TOPICS_READER = XmlMapper.builder().findAndAddModules().build().readerFor(Topics.class);

    public static class ParsedReference {
        private String title;
        private String authors;
        private int count;

        public ParsedReference(String title, String authors) {
            this.title = title;
            this.authors = authors;
            this.count = 1;
        }

        public void increaseCount(int by) {
            this.count += 1;
        }

        public String title() {
            return this.title;
        }

        public String authors() {
            return this.authors;
        }

        public int count() {
            return this.count;
        }
    }

    public static class ParsedArticle {
        public String body;
        public String authors;
        public List<ParsedReference> references;

        public ParsedArticle(String body, String authors, List<ParsedReference> references) {
            this.body = body;
            this.authors = authors;
            this.references = references;
        }

        private void setBody(String newBody) {
            body = newBody;
        }
        private void setAuthors(String newAuthors) {
            authors = newAuthors;
        }
        private void setReferences(List<ParsedReference> newReferences) {references = newReferences;}
        private void addReferences(List<ParsedReference> newReferences) {references.addAll(newReferences); }

        public String body() {return body;}
        public String authors() {return authors;}
        public List<ParsedReference> references() {return references;}
        public String textReferences() {
            StringBuilder refBuilder = new StringBuilder();
            for (ParsedReference reference : references) {
                refBuilder.append(reference.title() + "\t" + reference.authors() + "\t" + reference.count() + "\n");
            }
            return refBuilder.toString();
        }
    }

    private static String parseAuthors(List<Article.Author> authors) {
        StringBuilder builderAuthors = new StringBuilder();
        for (int i = 0; i < Math.min(numRefAuthors, authors.size()); i++) {
            Article.Author author = authors.get(i);
            if (author.last().length() == 0) {
                continue;
            }
            builderAuthors.append(author.last() + " ");
        }
        String parsedAuthors = parse(builderAuthors.toString());
        return parsedAuthors;
    }

    /**
     * Parses the content of the JSON file: authors (last name), body, references parsed title, references parsed
     * authors and number of citations of that reference.
     * @param articlePath Path of the JSON file.
     * @returns PorsedArticle instance.
     */
    private static final ParsedArticle parseArticleFile(Path articlePath) {
        // read article
        Article article;
        try {
            article = ARTICLE_READER.readValue(articlePath.toFile());
        } catch (IOException e) {
            System.out.println("While reading a JSON file an error has occurred: " + articlePath);
            return null;
        }

        // prepare string builders and map objects to store information of the article
        StringBuilder articleTextBuilder = new StringBuilder();
        Map<String, ParsedReference> parsedReferences = new HashMap<>();
        String articleAuthors = "";


        // add all bib entries to parsedReferences
        for (Map.Entry<String, Article.Reference> entry : article.bib_entries().entrySet()) {
            String parsedTitle = parse(entry.getValue().title());
            if (parsedTitle.length() == 0) { continue; }
            String parsedAuthors = parseAuthors(entry.getValue().authors());
            if (parsedAuthors.length() == 0) { continue; }
            parsedReferences.put(entry.getKey(), new ParsedReference(parsedTitle, parsedAuthors));
        }
        // add article text and count references to update the counts
        for (Article.Content paragraph : article.body_text()) {
            articleTextBuilder.append(paragraph.section() + "\n");
            articleTextBuilder.append(paragraph.text() + "\n");
            for (Article.Content.Cite cite : paragraph.cite_spans()) {
                if (parsedReferences.containsKey(cite.ref_id())) {
                    parsedReferences.get(cite.ref_id()).increaseCount(1);
                }
            }
        }
        String articleText = articleTextBuilder.toString();

        // add article authors (last name)
        if (!Objects.isNull(article.metadata().authors())) {
            List<Article.Author> authors = article.metadata().authors();
            StringBuilder articleAuthorsBuilder = new StringBuilder();
            for (int i=0; i < authors.size(); i++) {
                articleAuthorsBuilder.append(authors.get(i).last() + "; ");
            }
            articleAuthors = articleAuthorsBuilder.toString();
        }

        ParsedArticle parsedArticle = new ParsedArticle(articleText, articleAuthors, parsedReferences.values().stream().toList());
        return parsedArticle;
    }


    /**
     * Using the readArticleFile method, parses a set of articles and returns their joined content.
     * @param articlePaths Array of paths to the set of articles which need to be parsed.
     * @returns String array where each element is the concatenation of one specific field of all articles specified in
     * articlesPath.
     */
    private static final ParsedArticle parseArticleFiles(List<Path> articlePaths) {
        ParsedArticle completeArticle = new ParsedArticle("", "", new ArrayList<>());
        for (Path articlePath : articlePaths) {
            ParsedArticle parsedArticle = parseArticleFile(articlePath);
            completeArticle.setBody(completeArticle.body() + "\n" + parsedArticle.body());
            completeArticle.setAuthors(completeArticle.authors() + "\n" + parsedArticle.authors());
            completeArticle.addReferences(completeArticle.references());
        }
        return completeArticle;
    }

    /**
     * From a Metadata instance (row information in metadata.csv), reads the corresponding JSON file or files to return
     * its content.
     * @param rowMetadata Metadata object.
     * @returns Parsed article with all the content.
     */
    public static final ParsedArticle parseArticle(Metadata rowMetadata) {
        ParsedArticle parsedArticle;
        if (rowMetadata.pmcFile().length() != 0) {
            parsedArticle = parseArticleFile(DEFAULT_COLLECTION_PATH.resolve(rowMetadata.pmcFile()));
        } else if (rowMetadata.pdfFiles().size() >= 1) {
            List<Path> pdfPaths = rowMetadata.pdfFiles().stream().map(
                    pdfPath -> DEFAULT_COLLECTION_PATH.resolve(pdfPath)
            ).collect(Collectors.toList());
            parsedArticle = parseArticleFiles(pdfPaths);
        } else {
            return null;
        }
        return parsedArticle;
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

    public static final Map<Integer, float[]> readQueryEmbeddingsFloating() {
        Map<String, List<Double>> queryEmbeddingsString = null;
        try {
            queryEmbeddingsString = new ObjectMapper().readValue(
                    DEFAULT_COLLECTION_PATH.resolve(QUERY_EMBEDDINGS_FILENAME).toFile(), Map.class);
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("IOException while reading query embeddings JSON file");
        }
        Map<Integer, float[]> queryEmbeddings = new HashMap<>();
        for (Map.Entry<String, List<Double>> query : queryEmbeddingsString.entrySet()) {
            Integer topicID = Integer.parseInt(query.getKey());
            float[] embeddings = new float[query.getValue().size()];
            for (int i=0; i < EMBEDDINGS_DIMENSIONALITY; i++) {
                embeddings[i] = (float) (double) query.getValue().get(i);
            }
            queryEmbeddings.put(topicID, embeddings);
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

    public static Map<Integer, List<TopDocument>> readCosineSimilarities(String foldername, boolean order) {
        Map<Integer, List<TopDocument>> queryDocSimilarities = new HashMap<>();
        File folder = new File(foldername);
        for (int i = 1; i <= 50; i++) {
            List<TopDocument> docSimilarities = new ArrayList<>();
            try {
                Stream<String> stream = Files.lines(Paths.get(folder.toString() + "/" + i + ".txt"));
                stream.forEach(line -> {
                    String[] lineContent = line.split(" ");
                    docSimilarities.add(new TopDocument(lineContent[1],
                            Double.parseDouble(lineContent[2]), Integer.parseInt(lineContent[0])));
                });
            } catch (IOException e) {
                System.out.println("IOException while reading cosine similarities results in topic " + i);
                e.printStackTrace();
            }
            if (order) {
                Collections.sort(docSimilarities, new TopDocumentOrder());
            }

            queryDocSimilarities.put(i, docSimilarities);
        }
        return queryDocSimilarities;
    }

    public static Map<String, ArrayRealVector> readDocEmbeddings() {
        Stream<String> stream = streamDocEmbeddings();
        Map<String, ArrayRealVector> docEmbeddings = new HashMap<>();
        for (Iterator<String> it = stream.iterator(); it.hasNext(); ) {
            String line = it.next();
            String[] lineContent = line.split(",");
            String docID = lineContent[0];
            lineContent = Arrays.copyOfRange(lineContent, 1, lineContent.length);
            ArrayRealVector embedding = new ArrayRealVector(Arrays.stream(lineContent)
                    .mapToDouble(Double::parseDouble).toArray());
            docEmbeddings.put(docID, embedding);
        }
        return docEmbeddings;
    }

    public static Map<String, float[]> readDocEmbeddingsFloating() {
        Stream<String> stream = streamDocEmbeddings();
        Map<String, float[]> docEmbeddings = new HashMap<>();
        for (Iterator<String> it = stream.iterator(); it.hasNext(); ) {
            String line = it.next();
            String[] lineContent = line.split(",");
            String docID = lineContent[0];
            lineContent = Arrays.copyOfRange(lineContent, 1, lineContent.length);
            float[] embedding = new float[lineContent.length];
            for (int i = 0; i < lineContent.length; i++) {
                embedding[i] = (float) Float.parseFloat(lineContent[i]);
            }
            docEmbeddings.put(docID, embedding);
        }
        return docEmbeddings;
    }

}
