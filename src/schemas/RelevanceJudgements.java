package es.udc.fi.irdatos.c2122.schemas;

import java.util.List;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonPropertyOrder({"topicID", "docID", "score"})
@JsonIgnoreProperties(ignoreUnknown = true)
public record RelevanceJudgements(
        Integer topicID,
        String docID,
        Integer score
) {}
