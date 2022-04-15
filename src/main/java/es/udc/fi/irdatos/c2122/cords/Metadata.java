package es.udc.fi.irdatos.c2122.cords;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Defines the metadata.csv structure stored in the TREC-COVID Collection.
 * For indexing and searching only the following fields are considered:
 * - cord_uid of the article, i.e. its identifier.
 * - title of the article.
 * - abstract of the article.
 * - pmc_json_files and pdf_json_files, i.e. the path to the JSON files.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record Metadata(
    @JsonProperty("cord_uid") String cordUid,
    String title,
    List<String> authors,
    @JsonProperty("publish_time") String publishDate,
    @JsonProperty("abstract") String abstrac,
    String journal,
    @JsonProperty("pmc_json_files") String pmcFile,
    @JsonProperty("pdf_json_files") List<String> pdfFiles
) {}
