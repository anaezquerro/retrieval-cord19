package es.udc.fi.irdatos.c2122.cords;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;

import es.udc.fi.irdatos.c2122.util.ObjectReaderUtils;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.commons.io.FileUtils;


public class ReadIndexMetadata {
    private static final Path DEFAULT_COLLECTION_PATH = Paths.get("cord-19_2020-07-16", "2020-07-16");
    private static String METADATA_FILE_NAME = "metadata.csv";
    private static final ObjectReader ARTICLE_READER = JsonMapper.builder().findAndAddModules().build()
            .readerFor(Article.class);

    private static final String[] readArticle(Path articlePath) {
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

    private static final String[] readArticles(List<Path> articlesPath) {
        StringBuilder[] articlesContentBuilder = new StringBuilder[3];
        for (int i = 0; i < 3; i++) {
            articlesContentBuilder[i] = new StringBuilder();
        }
        for (Path articlePath : articlesPath) {
            String[] articlesContent = readArticle(articlePath);
            for (int i = 0; i<3; i++) {
                articlesContentBuilder[i].append(articlesContent[i]);
                articlesContentBuilder[i].append('\n');
            }
        }

        String[] articlesContent = Arrays.stream(articlesContentBuilder).map(builder -> builder.toString()).toArray(String[]::new);
        return articlesContent;
    }


    private static final List<MetadataArticle> readMetadata() {
        Path collectionPath = DEFAULT_COLLECTION_PATH;
        Path metadataPath = collectionPath.resolve(METADATA_FILE_NAME);
        CsvSchema schema = CsvSchema.emptySchema().withHeader().withArrayElementSeparator("; ");
        ObjectReader reader = new CsvMapper().readerFor(MetadataArticle.class).with(schema);

        List<MetadataArticle> metadata;
        try {
            metadata = ObjectReaderUtils.readAllValues(metadataPath, reader);
        } catch (IOException e) {
            System.out.println("IOException while reading metadata in " + metadataPath.toString());
            e.printStackTrace();
            return null;
        }
        return metadata;
    }

    private static void indexMetadata(List<MetadataArticle> metadata, String indexFolder) {
        Path collectionPath = DEFAULT_COLLECTION_PATH;

        // Creation of IndexWriter
        IndexWriterConfig config = new IndexWriterConfig(new StandardAnalyzer());
        IndexWriter writer = null;
        try {
            writer = new IndexWriter(FSDirectory.open(Paths.get(indexFolder)), config);
        } catch (CorruptIndexException e) {
            System.out.println("CorruptIndexException while creating IndexWriter at " + indexFolder);
            e.printStackTrace();
        } catch (LockObtainFailedException e) {
            System.out.println("LockObtainFailedException while creating IndexWriter at " + indexFolder);
            e.printStackTrace();
        } catch (IOException e) {
            System.out.println("IOException while creating IndexWriter at " + indexFolder);
            e.printStackTrace();
        }


        // Index each metadata row as a new document
        for (MetadataArticle article : metadata) {
            Document doc = new Document();
            doc.add(new StoredField("docID", article.cordUid()));
            doc.add(new TextField("title", article.title(), Field.Store.YES));
            doc.add(new TextField("abstract", article.abstrac(), Field.Store.YES));

            // Remove comments to index authors (check README-iteration1)
            // String authors = String.join("; ", article.authors());
            // doc.add(new StoredField("authors", authors));

            // Read PMC and PDF paths (check README-iteration1 to understand this block of code)
            List<Path> pdfPaths = article.pdfFiles().stream().map(pdfPath -> collectionPath.resolve(pdfPath)).collect(Collectors.toList());
            String[] articleContent;
            if (article.pmcFile().length() != 0) {
                articleContent = readArticle(collectionPath.resolve(article.pmcFile()));
            } else if (article.pdfFiles().size() >= 1) {
                List<Path> pmcpdfPaths = new ArrayList<>();
                pmcpdfPaths.addAll(pdfPaths);
                articleContent = readArticles(pmcpdfPaths);
            } else {
                articleContent = new String[] {"", "", ""};
            }

            // Save body, references and figure notes as new fields in the document
            FieldType bodyFieldType = new FieldType();
            bodyFieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
            doc.add(new Field("body", articleContent[0], bodyFieldType));
            doc.add(new TextField("references", articleContent[1], Field.Store.YES));
            doc.add(new TextField("figures", articleContent[2], Field.Store.YES));

            try {
                writer.addDocument(doc);
            } catch (CorruptIndexException e) {
                System.out.println("CorruptIndexException while trying to write the document " + article.cordUid());
                e.printStackTrace();
            } catch (IOException e) {
                System.out.println("IOException while trying to write the document " + article.cordUid());
                e.printStackTrace();
            }
        }

        // Close the writer
        try {
            writer.commit();
            writer.close();
        } catch (CorruptIndexException e) {
            System.out.println("CorruptIndexException while closing the index writer");
            e.printStackTrace();
        } catch (IOException e) {
            System.out.println("IOException while closing the index writer");
            e.printStackTrace();
        }

    }

    public static void main(String[] args) {
        String indexFolder;
        if (args.length == 0) {
            indexFolder = "Index-StandardAnalyzer";
        } else {
            indexFolder = args[0];
        }

        if (new File(indexFolder).exists()) {
            try {
                FileUtils.deleteDirectory(new File(indexFolder));
            } catch (IOException e) {
                System.out.println("IOException while removing " + indexFolder + " folder");
            }
        }

        // Read metadata.csv
        List<MetadataArticle> metadata = readMetadata();

        // Create index
        indexMetadata(metadata, indexFolder);
    }
}
