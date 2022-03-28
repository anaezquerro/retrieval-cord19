package es.udc.fi.irdatos.c2122.cords;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public record Article(List<Content> body_text, Map<String, Reference> bib_entries, Map<String, Figure> ref_entries) {
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static record Content(String text, String section) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static record Reference(String title) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static record Figure(String text) {}
}

