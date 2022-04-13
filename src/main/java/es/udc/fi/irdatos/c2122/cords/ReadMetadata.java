package es.udc.fi.irdatos.c2122.cords;

import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import es.udc.fi.irdatos.c2122.util.ObjectReaderUtils;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class ReadMetadata {
    private static Path metadataPath;
    private static ObjectReader articleReader;

    public ReadMetadata(Path metadataPath, ObjectReader articleReader) {
        this.metadataPath = metadataPath;
        this.articleReader = articleReader;
    }

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

    public static final List<MetadataArticle> readMetadata() {
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
}
