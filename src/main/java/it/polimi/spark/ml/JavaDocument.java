package it.polimi.spark.ml;

import java.io.Serializable;

public class JavaDocument implements Serializable {
    private final long id;
    private final String text;

    public JavaDocument(long id, String text) {
        this.id = id;
        this.text = text;
    }

    public long getId() {
        return this.id;
    }

    public String getText() {
        return this.text;
    }
}
