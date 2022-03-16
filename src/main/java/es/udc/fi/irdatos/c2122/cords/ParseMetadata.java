package es.udc.fi.irdatos.c2122.cords;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;

import es.udc.fi.irdatos.c2122.util.ObjectReaderUtils;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.queryparser.flexible.standard.config.FieldDateResolutionFCListener;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.util.packed.DirectMonotonicReader;

public class ParseMetadata {
    private static final Path DEFAULT_COLLECTION_PATH = Paths.get("cord-19_2020-07-16",
            "2020-07-16");
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
        StringBuilder[] articleContentBuilder = new StringBuilder[3];
        for (int i = 0; i < 3; i++) {
            articleContentBuilder[i] = new StringBuilder();
        }
        for (Path articlePath : articlesPath) {
            String[] articleContent = readArticle(articlePath);
            for (int i = 0; i<3; i++) {
                articleContentBuilder[i].append(articleContent[i]);
                articleContentBuilder[i].append('\n');
            }
        }

        String[] articleContent = Arrays.stream(articleContentBuilder).map(builder -> builder.toString()).toArray(String[]::new);
        return articleContent;
    }


    public static void main(String[] args) {

        Path collectionPath = DEFAULT_COLLECTION_PATH;
        Path metadataPath = collectionPath.resolve(METADATA_FILE_NAME);

        CsvSchema schema = CsvSchema.emptySchema().withHeader().withArrayElementSeparator("; ");
        ObjectReader reader = new CsvMapper().readerFor(MetadataArticle.class).with(schema);

        List<MetadataArticle> metadata;
        try {
            metadata = ObjectReaderUtils.readAllValues(metadataPath, reader);
        } catch (IOException ex) {
            System.out.println("An IOException occurred while reading " + metadataPath.toString());
            return;
        }


        // Creating index folder
        String indexFolder = "Index-StandardAnalyzer";
        IndexWriterConfig config = new IndexWriterConfig(new StandardAnalyzer());
        IndexWriter writer = null;
        try {
            writer = new IndexWriter(FSDirectory.open(Paths.get(indexFolder)), config);
        } catch (CorruptIndexException e1) {
            System.out.println("CorruptIndexException: Creating " + indexFolder);
            e1.printStackTrace();
        } catch (LockObtainFailedException e1) {
            System.out.println("LockObtainFailedException: Creating " + indexFolder);
            e1.printStackTrace();
        } catch (IOException e1) {
            System.out.println("IOException: Creating " + indexFolder);
            e1.printStackTrace();
        }


        // Indexing articles
        for (MetadataArticle article : metadata) {

            Document doc = new Document();
            doc.add(new StoredField("docID", article.cordUid()));
            doc.add(new TextField("title", article.title(), Field.Store.YES));
            doc.add(new TextField("abstract", article.abstrac(), Field.Store.YES));

            String authors = String.join("; ", article.authors());
            doc.add(new StoredField("authors", authors));

            // Read PMC and PDF paths
            List<Path> pdfPaths = article.pdfFiles().stream().map(pdfPath -> collectionPath.resolve(pdfPath)).collect(Collectors.toList());
            String[] articleContent;
            if (article.pmcFile().length() != 0 && article.pdfFiles().size() <= 1) {
                articleContent = readArticle(collectionPath.resolve(article.pmcFile()));
            } else if (article.pdfFiles().size() >= 1){
                articleContent = readArticles(pdfPaths);
            } else {
                articleContent = new String[] {"", "", ""};
            }

            // Save body of articles indexed and
            FieldType bodyFieldType = new FieldType();
            bodyFieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
            doc.add(new Field("body", articleContent[0], bodyFieldType));
            doc.add(new TextField("references", articleContent[1], Field.Store.YES));
            doc.add(new TextField("figures", articleContent[2], Field.Store.YES));

            try {
                writer.addDocument(doc);
            } catch (CorruptIndexException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        try {
            writer.commit();
            writer.close();
        } catch (CorruptIndexException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
