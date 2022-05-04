package es.udc.fi.irdatos.c2122.schemas;
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
public record Article(List<Content> body_text, Map<String, Reference> bib_entries, Map<String, Figure> ref_entries) {
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static record Content(String text, String section) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static record Reference(String title) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static record Figure(String text) {}
}

