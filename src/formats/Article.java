package formats;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Defines the JSON structure of the PMC and PDF files stored in the TREC-COVID Collection.
 * For indexing and searching only the following fields are considered:
 * - body_text of the article, interpreted as the raw content of it.
 * - bib_entries of the article, i.e. notes of the article bibliography.
 * - ref_entries of the article, i.e. notes of the tables and figures.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record Article(Metadata metadata, List<Content> body_text, Map<String, Reference> bib_entries,
                      Map<String, Figure> ref_entries) {
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static record Metadata(List<Author> authors) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static record Content(String text, List<Cite> cite_spans, String section) {

        @JsonIgnoreProperties(ignoreUnknown = true)
        public static record Cite(String ref_id) {}
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static record Author(String first, List<String> middle, String last) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static record Reference(String title, List<Author> authors) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static record Figure(String text) {}
}

