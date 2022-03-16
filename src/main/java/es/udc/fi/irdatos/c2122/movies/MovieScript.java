package es.udc.fi.irdatos.c2122.movies;

import java.time.LocalDate;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public record MovieScript(String title, LocalDate date, List<Scene> scenes) {
    public static record Scene(String transition, String header, List<SceneContent> contents) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static record SceneContent(String type, String text) {}
}
