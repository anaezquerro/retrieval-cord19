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
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.commons.io.FileUtils;

import static es.udc.fi.irdatos.c2122.cords.IndexMetadataPool.indexMetadataPool;


public class ReadIndexMetadata {
    private static final Path DEFAULT_COLLECTION_PATH = Paths.get("2020-07-16");
    private static String METADATA_FILE_NAME = "metadata.csv";
    private static final ObjectReader ARTICLE_READER = JsonMapper.builder().findAndAddModules().build()
            .readerFor(Article.class);

    public static void main(String[] args) {
        // Get index folder
        String indexFolder;
        if (args.length == 0) {
            indexFolder = "Index-EnglishAnalyzer";
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

        Path indexPath = Paths.get(indexFolder);

        // Read metadata.csv
        ReadMetadata metadataReader = new ReadMetadata(DEFAULT_COLLECTION_PATH.resolve(METADATA_FILE_NAME), ARTICLE_READER);
        List<MetadataArticle> metadata = metadataReader.readMetadata();

        // Index articles from parsed metadata
        indexMetadataPool(metadata, indexPath, DEFAULT_COLLECTION_PATH);


    }
}
