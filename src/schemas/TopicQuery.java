package schemas;

import java.util.Map;

public class TopicQuery {
    private int topicID;
    private String text;
    private Embedding embedding;
    private Map<String, String> fieldTexts;
    private Map<String, Float> fieldWeights;


    public TopicQuery(int topicID, String text, Embedding embedding) {
        this.topicID = topicID;
        this.text = text;
        this.embedding = embedding;

    }

    public void setFieldTexts(Map<String, String> fieldTexts) {
        this.fieldTexts = fieldTexts;
    }

    public void setFieldWeights(Map<String, Float> fieldWeights) {
        this.fieldWeights = fieldWeights;
    }

    public Map<String, String> fieldTexts() {
        return fieldTexts;
    }

    public Map<String, Float> fieldWeights() {
        return fieldWeights;
    }

    public String text() {
        return text;
    }

    public Embedding embedding() {
        return embedding;
    }

    public void putField(String fieldname, String text) {
        fieldTexts.put(fieldname, text);
    }

    public int topicID() {
        return topicID;
    }

    public TopicQuery copy() {
        TopicQuery topicQuerycopy = new TopicQuery(topicID, text, embedding);
        topicQuerycopy.setFieldTexts(fieldTexts);
        topicQuerycopy.setFieldWeights(fieldWeights);
        return topicQuerycopy;
    }
}
