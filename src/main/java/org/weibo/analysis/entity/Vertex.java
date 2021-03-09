package org.weibo.analysis.entity;


import java.io.Serializable;
import java.util.Objects;

public class Vertex implements Serializable {
    private static final long serialVersionUID = 1L;
    public String label;
    public String id;
    public Integer count = 1;

    public Vertex() {
    }

    public Vertex(String id, String label) {
        this.id = id;
        if (Objects.isNull(label)) {
            this.label = this.getClass().getSimpleName();
        } else {
            this.label = label;
        }
    }

    public String getLabel() {
        return this.label;
    }

    public String getId() {
        return this.id;
    }

    public int getCount() {
        return count;
    }

    public void addCount(int count) {
        this.count += count;
    }

    public String toString() {
        return String.format("{" +
                        "\"id\":\"%s\"," +
                        "\"label\":\"%s\"," +
                        "\"count\":%d" +
                        "}",
                getId(), getLabel(), getCount());
    }
}
