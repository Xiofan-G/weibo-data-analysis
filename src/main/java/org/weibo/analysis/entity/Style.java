package org.weibo.analysis.entity;


import java.io.Serializable;
//label name and color
public class Style implements Serializable {
    public String name;
    public String symbolSize;
    public String color;

    public Style() {
    }

    public Style(String name, String color) {
        this.name = name;
        this.color = color;
    }

    @Override
    public String toString() {
        return String.
                format("{\"name\":\"%s\", \"style\":{\"color\": \"%s\"}}"
                        , name, color);
    }
}
