package es.udc.fi.irdatos.c2122.cords;

import es.udc.fi.irdatos.c2122.schemas.Article;
import es.udc.fi.irdatos.c2122.schemas.Metadata;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnVectorField;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;

import java.io.IOException;
import java.nio.file.Paths;
import java.sql.SQLOutput;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static es.udc.fi.irdatos.c2122.cords.AuxiliarFunctions.coalesce;

public class ReferencesIndexing {
    IndexWriter iwriter;
    IndexSearcher isearcher;
    IndexReader ireader;
    int m;
    int numAuthors = 5;

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


    public ReferencesIndexing(IndexWriter iwriter, IndexReader ireader, IndexSearcher isearcher) {
        this.iwriter = iwriter;
        this.isearcher = isearcher;
        this.ireader = ireader;
        this.m = 2;
    }

    public ReferencesIndexing(IndexWriter iwriter, IndexReader ireader, IndexSearcher isearcher, int m) {
        this.iwriter = iwriter;
        this.isearcher = isearcher;
        this.ireader = ireader;
        this.m = m;
    }

    private class ParsedReference {
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
                parsedReferences.get(cite.ref_id()).increaseCount(1);
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

        /**
         * For each article in the metadata slice (group of rows of metadata.csv), parse its references
         * from the PMC file and use them to search in the index (with ireader and isearcher). Obtain the top
         * m results (where m is small) and consider that those m documents match with the references, so
         * add to the article their identifiers in the index.
         */
        @Override
        public void run() {
            for (int docID = start; docID < end; docID++) {
                // 1. Read document with IndexReader and obtain the Article file read
                Document doc;
                Article article;
                try {
                    doc = ireader.document(docID);
                    article = CollectionReader.ARTICLE_READER.readValue(
                            CollectionReader.DEFAULT_COLLECTION_PATH.resolve(doc.get("file")).toFile());
                } catch (IOException e) {e.printStackTrace(); return;}

                // prepare the vector where count of references will be stored
                float[] referencesVector = new float[ireader.numDocs()];
                Arrays.fill(referencesVector, 0F);

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
                        return;
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
                        return;
                    }

                    booleanQueryBuilder.add(queryAuthor, BooleanClause.Occur.SHOULD);

                    // build the query and execute
                    BooleanQuery booleanQuery = booleanQueryBuilder.build();

                    TopDocs topDocs;
                    try {
                        topDocs = isearcher.search(booleanQuery, m);
                    } catch (IOException e) {
                        e.printStackTrace();
                        return;
                    }

                    // save results with the index writer
                    for (int i = 0; i < Math.min(topDocs.scoreDocs.length, topDocs.totalHits.value); i++) {
                        int docRefID = topDocs.scoreDocs[i].doc;
                        referencesVector[docRefID] = Math.max(reference.count(), referencesVector[docRefID]);
                    }
                }
                // add to the document the referencesVector
                doc.add(new KnnVectorField("referencesVector", referencesVector));
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
            executor.awaitTermination(40, TimeUnit.MINUTES);
        } catch (final InterruptedException e) {
            e.printStackTrace();
            System.exit(-2);
        }
    }
}
