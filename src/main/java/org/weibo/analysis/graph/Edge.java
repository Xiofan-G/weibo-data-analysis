package org.weibo.analysis.graph;

import com.alibaba.fastjson.JSON;
import org.weibo.analysis.entity.*;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Edge<S, T> implements Serializable {

    private static final long serialVersionUID = 1L;
    public String id;
    public String label;
    public S source;
    public T target;
    public String remark;
    public Long timestamp;
    public Integer count;

    public Edge() {
    }

    public Edge(S source, T target, String label, String id, String remark, Long timestamp) {
        this.id = id;
        this.label = label;
        this.source = source;
        this.target = target;
        this.remark = remark;
        this.timestamp = timestamp;
        // count
        this.count = 1;
    }
//<Vertex, Edge, Vertex>
// user, author, weibo
// user, author, comment
// /user, fans or follow, user
// comment, replyOf, weibo
//comment, mentioned, weibo
//comment, @, user
//weibo, mentioned, weibo
//weibo, @, user
//tag, belongOf,  weibo

    public static Edge<Vertex, Vertex> build(Edge<String, String> edge) {
        RelationLabel label = RelationLabel.valueOf(edge.getLabel());

        String source = edge.getSource();
        String target = edge.getTarget();
        String remark = edge.getRemark();
        Vertex sourceVertex;
        Vertex targetVertex;
        switch (label) {
            case Fans:
                sourceVertex = JSON.parseObject(source, User.class);
                targetVertex = JSON.parseObject(target, User.class);
                break;
            case At:

                targetVertex = JSON.parseObject(target, User.class);
                if (VertexLabel.Weibo.name().equals(remark)) {
                    sourceVertex = JSON.parseObject(source, Weibo.class);

                } else {
                    sourceVertex = JSON.parseObject(source, Comment.class);
                }

                break;
            case Author:
                sourceVertex = JSON.parseObject(source, User.class);
                if (VertexLabel.Weibo.name().equals(remark)) {
                    targetVertex = JSON.parseObject(target, Weibo.class);

                } else {
                    targetVertex = JSON.parseObject(target, Comment.class);
                }
                break;
            case ReplyOf:
                sourceVertex = JSON.parseObject(source, Comment.class);
                if (VertexLabel.Weibo.name().equals(remark)) {
                    targetVertex = JSON.parseObject(target, Weibo.class);

                } else {
                    targetVertex = JSON.parseObject(target, Comment.class);
                }
                break;
            case BelongTo:
                sourceVertex = JSON.parseObject(source, Weibo.class);
                targetVertex = JSON.parseObject(target, Tag.class);
                break;
            case Mentioned:
                targetVertex = JSON.parseObject(target, Weibo.class);
                if (VertexLabel.Weibo.name().equals(remark)) {
                    sourceVertex = JSON.parseObject(source, Weibo.class);

                } else {
                    sourceVertex = JSON.parseObject(source, Comment.class);
                }
                break;
            default:
                sourceVertex = JSON.parseObject(source, Vertex.class);
                targetVertex = JSON.parseObject(target, Vertex.class);

        }
        return new Edge<>(sourceVertex, targetVertex, label.name(), edge.getId(), edge.getRemark(), edge.getTimestamp());
    }

    @Override
    public String toString() {
        String source = "";
        String target = "";
        if (this.source instanceof Vertex) {
            source = ((Vertex) this.source).getId();
            target = ((Vertex) this.target).getId();
        } else {
            source = this.source.toString();
            target = this.target.toString();
        }
        return String.format("{" +
                        "\"id\":\"%s\"," +
                        "\"label\":\"%s\"," +
                        "\"count\":%d," +
                        "\"properties\":{" +
                        "\"remark\":\"%s\"" +
                        "}," +
                        "\"source\":\"%s\"," +
                        "\"target\":\"%s\"," +
                        "\"timestamp\":%d" +
                        "}",
                id, label, count, String.format("%s, Since %s", remark, getFormatTime(timestamp)), source, target, timestamp);
    }

    private String getFormatTime(Long timestamp) {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = new Date(timestamp);
        return formatter.format(date);
    }

    public Long getTimestamp() {
        return this.timestamp;
    }

    public String getLabel() {
        return this.label;
    }

    public void setLabel(String label) {
        this.label = label;

    }

    public String getId() {
        return this.id;
    }

    public S getSource() {
        return this.source;
    }

    public T getTarget() {
        return target;
    }

    public String getRemark() {
        return this.remark;
    }

    public void addCount(int count) {
        this.count += count;
    }
}