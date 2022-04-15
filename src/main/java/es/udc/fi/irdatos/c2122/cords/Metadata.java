package es.udc.fi.irdatos.c2122.cords;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

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
