package es.udc.fi.irdatos.c2122.cords;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
@JacksonXmlRootElement(localName="topics")
public record Topics(
        @JacksonXmlElementWrapper(localName = "topic", useWrapping = false)
        Topic[] topic
        ) {

    public record Topic(
            @JacksonXmlProperty(localName="number", isAttribute = true)
            int number,

            @JacksonXmlProperty(localName = "query")
            String query,

            @JacksonXmlProperty(localName = "question")
            String question,

            @JacksonXmlProperty(localName = "narrative")
            String narrative

    ) {}
}
