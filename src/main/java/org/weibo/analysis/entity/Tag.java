package org.weibo.analysis.entity;

import java.io.Serializable;

public class Tag extends Vertex implements Serializable {
    public String name;
    public String desc;

    public Tag() {
    }

    public Tag(String id, String name, String desc) {
        super(id, "");
        this.name = name;
        this.desc = desc;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }

    @Override
    public String toString() {
        String properties = String.format(
                "\"properties\":" +
                        "{" +
                        "\"name\":\"%s\"," +
                        "\"des\":\"%s\"" +
                        "}",
                name, desc);
        return String.format("%s,%s}", super.toString().replace("}", ""), properties);
    }
}
