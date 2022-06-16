package es.udc.fi.irdatos.c2122.cords;

import es.udc.fi.irdatos.c2122.schemas.Article;
import es.udc.fi.irdatos.c2122.schemas.Metadata;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.output.FileWriterWithEncoding;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.KnnVectorField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.*;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLOutput;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static es.udc.fi.irdatos.c2122.cords.AuxiliarFunctions.*;

public class ReferencesIndexing {
    IndexWriter iwriter;
    IndexSearcher isearcher;
    IndexReader ireader;
    int m = 2;
    int numAuthors = 5;
    String storingFolder = "references";
    boolean search;

    /**
     * Deletes stopwords from text (and, or the at of a in, OR, AND and other characters that cannot be parsed).
     * @param text Text to parse.
     * @returns Text parsed.
     */
    public String parse(String text) {
        String parsedText = text.replaceAll("\\[|\\]|\\(|\\)|/|-|\\'|\\:|\\\\|\"|\\}|\\{|\\*|\\?|\\!|\\^|\\~|\\+|\\;", " ");
        parsedText = parsedText.replaceAll("and|or|the|at|of|a|in|OR|AND", "");
        return parsedText.strip();
    }

    private String parseAuthors(List<Article.Author> authors) {
        StringBuilder builderAuthors = new StringBuilder();
        for (int i = 0; i < Math.min(numAuthors, authors.size()); i++) {
            Article.Author author = authors.get(i);
            if (author.last().length() == 0) {
                continue;
            }
            builderAuthors.append(author.last() + " ");
        }
        String parsedAuthors = parse(builderAuthors.toString());
        return parsedAuthors;
    }

    public ReferencesIndexing(IndexWriter iwriter, IndexReader ireader, IndexSearcher isearcher, boolean search) {
        this.iwriter = iwriter;
        this.isearcher = isearcher;
        this.ireader = ireader;
        this.search = search;
        if (search) {
            deleteFolder(storingFolder);
            createFolder(storingFolder);
        }
    }


    private class ParsedReference {
        private String title;
        private String authors;
        private int count;

        public ParsedReference(String title, String authors) {
            this.title = title;
            this.authors = authors;
            this.count = 0;
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

    private Map<String, ParsedReference> parseReferences(Article article) {
        Map<String, ParsedReference> parsedReferences = new HashMap<>();
        Map<String, Article.Reference> bib_entries = article.bib_entries();

        // add all bib entries to parsedReferences
        for (Map.Entry<String, Article.Reference>  entry : bib_entries.entrySet()) {
            // get parsed title
            String parsedTitle = parse(entry.getValue().title());
            if (parsedTitle.length() == 0) { continue; }
            String parsedAuthors = parseAuthors(entry.getValue().authors());
            if (parsedAuthors.length() == 0) { continue; }
            else {
                parsedReferences.put(entry.getKey(), new ParsedReference(parsedTitle, parsedAuthors));
            }
        }

        // update based on body text the references count
        List<Article.Content> body_text = article.body_text();
        for (Article.Content paragraph : body_text) {
            List<Article.Content.Cite> cites = paragraph.cite_spans();
            for (Article.Content.Cite cite : cites) {
                if (parsedReferences.containsKey(cite.ref_id())) {
                    parsedReferences.get(cite.ref_id()).increaseCount(1);
                }
            }
        }
        return parsedReferences;
    }



    private class WorkerReferences implements Runnable {
        private int start;
        private int end;
        private int workerID;

        public WorkerReferences(int start, int end, int workerID) {
            this.start = start;
            this.end = end;
            this.workerID = workerID;
        }

        private String searchReferences(int docID) {
            // 1. Read document with IndexReader and obtain the Article file read
            Document doc;
            Article article;
            try {
                doc = ireader.document(docID);
                article = CollectionReader.ARTICLE_READER.readValue(
                        CollectionReader.DEFAULT_COLLECTION_PATH.resolve(doc.get("file")).toFile());
            } catch (IOException e) {
                e.printStackTrace();
                return null;
            }

            StringBuilder referencesBuilder = new StringBuilder();

            // 2. Obtain article references and search them in the index
            Map<String, ParsedReference> references = parseReferences(article);
            for (ParsedReference reference : references.values()) {

                // construct boolean query
                BooleanQuery.Builder booleanQueryBuilder = new BooleanQuery.Builder();

                // construct query parser for the title of the reference
                QueryParser queryParser = new QueryParser("title", new StandardAnalyzer());
                Query query;

                try {
                    query = queryParser.parse(reference.title());
                } catch (ParseException e) {
                    System.out.println("ParseException while construction the query for reference in article " +
                            doc.get("docID") + " [" + doc.get("file") + "]: " + reference.title());
                    e.printStackTrace();
                    return null;
                }

                // add to the query
                booleanQueryBuilder.add(query, BooleanClause.Occur.SHOULD);

                // query for authors of the reference
                QueryParser parserAuthors = new QueryParser("authors", new StandardAnalyzer());
                Query queryAuthor;
                try {
                    queryAuthor = parserAuthors.parse(reference.authors());
                } catch (ParseException e) {
                    System.out.println("ParseException while constructing the query for reference author " +
                            "in article " + doc.get("docID") + ": " + reference.authors());
                    e.printStackTrace();
                    return null;
                }

                booleanQueryBuilder.add(queryAuthor, BooleanClause.Occur.SHOULD);

                // build the query and execute
                BooleanQuery booleanQuery = booleanQueryBuilder.build();

                TopDocs topDocs;
                try {
                    topDocs = isearcher.search(booleanQuery, 100);
                } catch (IOException e) {
                    e.printStackTrace();
                    return null;
                }

                // save results with the index writer
                try {
                    int j = 0;
                    for (int i = 0;
                         (i < Math.min(topDocs.scoreDocs.length, topDocs.totalHits.value)) && (j<m);
                         i++) {
                        int docRefID = topDocs.scoreDocs[i].doc;
                        List<String> docRefTitleWords = Arrays.stream(parse(ireader.document(docRefID).get("title")).split(" "))
                                .distinct().toList();
                        int mismatches = (int) docRefTitleWords.stream()
                                .filter(x -> reference.title().contains(x)).count();
                        if (mismatches > 0.15*docRefTitleWords.size()) {
                            continue;
                        }
                        referencesBuilder.append(
                                ireader.document(docRefID).get("docID") + " " + reference.count() + "\n");
                        j++;
                    }
                } catch (IOException e) {e.printStackTrace();return null;}
            }
            return referencesBuilder.toString();
        }

        /**
         * For each article in the metadata slice (group of rows of metadata.csv), parse its references
         * from the PMC file and use them to search in the index (with ireader and isearcher). Obtain the top
         * m results (where m is small) and consider that those m documents match with the references, so
         * add to the article their identifiers in the index.
         */
        @Override
        public void run() {
            for (int docID = start; docID < end; docID++) {
                if (Math.floorMod(docID, 500) == 0) {
                    System.out.println(workerID + ": is in docID=" + docID);
                }

                Document doc;
                String references = null;
                try {
                    doc = ireader.document(docID);
                    if (search) {
                        references = searchReferences(docID);
                        FileWriter fwriter = createFileWriter(storingFolder + "/" + doc.get("docID"));
                        fwriter.write(references);
                        fwriter.close();
                    } else {
                        references = new String(Files.readAllBytes(Paths.get(storingFolder, doc.get("docID"))));
                    }
                } catch (IOException e) {e.printStackTrace();return;
                } catch(Exception e) {e.printStackTrace();return;}

                // add to the document the referencesVector
                doc.add(new StoredField("references", references));
                try {
                    iwriter.updateDocument(new Term("docID", doc.get("docID")), doc);
                } catch (IOException e) {
                    System.out.println("IOException occurred while updating document " + doc.get("docID"));
                    e.printStackTrace();
                }
            }
        }
    }



    public void launch() {
        final int numCores = Runtime.getRuntime().availableProcessors();
        System.out.println("Indexing references with " + numCores + " cores");
        ExecutorService executor = Executors.newFixedThreadPool(numCores);
        System.out.println("A total of " + ireader.numDocs() + " articles will be indexed");
        Integer[] workersDivision = coalesce(numCores, ireader.numDocs());

        for (int i = 0; i < numCores; i++) {
            int start = workersDivision[i];
            int end = workersDivision[i+1];
            System.out.println("Thread " + i + " is indexing articles from " + start + " to " + end);
            WorkerReferences worker = new WorkerReferences(start, end, i);
            executor.execute(worker);
        }

        // end the executor
        executor.shutdown();
        try {
            executor.awaitTermination(1, TimeUnit.HOURS);
        } catch (final InterruptedException e) {
            e.printStackTrace();
            System.exit(-2);
        }

        // close the writer
        try {
            iwriter.commit();
            iwriter.close();
        } catch (CorruptIndexException e) {
            System.out.println("CorruptIndexException while closing the index writer");
            e.printStackTrace();
        } catch (IOException e) {
            System.out.println("IOException while closing the index writer");
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        IndexWriter writer = createIndexWriter(PoolIndexing.INDEX_FOLDERNAME);
        ReaderSearcher objReaderSearcher = new ReaderSearcher(
                Paths.get(PoolIndexing.INDEX_FOLDERNAME),
                PoolIndexing.similarity);
        IndexSearcher searcher = objReaderSearcher.searcher();
        IndexReader reader = objReaderSearcher.reader();

        ReferencesIndexing pool = new ReferencesIndexing(writer, reader, searcher, true);
        pool.launch();

    }
}
