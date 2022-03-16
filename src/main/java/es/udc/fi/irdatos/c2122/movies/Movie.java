package es.udc.fi.irdatos.c2122.movies;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/*
 * Define a record class with the attributes we are going to use and set
 * the @JsonIgnoreProperties annotation. In this example we are ignoring the
 * rating column in the CSV
 *
 * Without the annotation we need to declare all attributes or parsing fails
 * because it finds unknown attributes.
 *
 * Use the annotation @JsonProperty to define the name of the field in the
 * source file if we want to have a different name for the attribute. This can
 * be because we want, for example, to use a different naming convention, but it
 * is necessary in case of field names that conflict with Java keywords, such as
 * class or abstract
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record Movie(
    @JsonProperty("imdb_id") String imdbId,
    String title,
    String director,
    List<String> cast,
    String synopsis,
    int year,
    String script
) {}
