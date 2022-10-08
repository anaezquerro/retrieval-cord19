package schemas;


import java.util.List;

/**
 * Parsed article object to store body (as string), authors (as string) and list of parsed references.
 * body [String]                     : Body of the article.
 * authors [String]                  : Authors of the article (only last name and separated with commas).
 * references List<ParsedReferences> : List of ParsedReference of the article.
 */
public class ParsedArticle {
    public static int NUM_AUTHORS_PARSED = 5;
    public static String AUTHORS_SEPARATOR = ",";
    public static String REFERENCES_SEPARATOOR = "\n";

    /**
     * ParsedReference object to store parsed title, authors and count of the citations.
     * Note: In this context, "parsed" means that title and authors string expression has been modified in order to not
     * raising the ParseException in the QueryParser.parse() method.
     */
    public static class ParsedReference {
        private String title;
        private String authors;
        private int count;
        public static String ITEM_REFS_SEPARATOR = "\t";

        public ParsedReference(String title, String authors) {
            this.title = title;
            this.authors = authors;
            this.count = 1;
        }

        public void increaseCount() {
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

        public String toString() {
            return title + ITEM_REFS_SEPARATOR + authors + ITEM_REFS_SEPARATOR + count;
        }
    }

    private String title;
    private String abstract_;
    private String body;
    private String authors;
    private List<ParsedReference> references;


    public ParsedArticle(String title, String abstract_, String body, String authors, List<ParsedReference> references) {
        this.body = body;
        this.authors = authors;
        this.references = references;
        this.title = title;
        this.abstract_ = abstract_;
    }

    public void setTitle(String newTitle) {
        this.title = title;
    }

    public void setAbstract(String newAbstract) {
        this.abstract_ = newAbstract;
    }

    public void setBody(String newBody) {
        body = newBody;
    }
    public void setAuthors(String newAuthors) {
        authors = newAuthors;
    }
    public void setReferences(List<ParsedReference> newReferences) {
        references = newReferences;
    }

    public void addBody(String moreBody) {
        body = body + moreBody;
    }

    public void addAuthors(String moreAuthors) {
        authors = authors + moreAuthors;
    }

    public void addReferences(List<ParsedReference> newReferences) {
        references.addAll(newReferences);
    }


    public String title() {
        return title;
    }

    public String abstract_() {
        return abstract_;
    }

    public String body() {
        return body;
    }

    public String authors() {
        return authors;

    }
    public List<ParsedReference> references() {
        return references;
    }

    public String textReferences() {
        StringBuilder refBuilder = new StringBuilder();
        for (ParsedReference reference : references) {
            refBuilder.append(reference.toString() + REFERENCES_SEPARATOOR);
        }
        return refBuilder.toString();
    }
}