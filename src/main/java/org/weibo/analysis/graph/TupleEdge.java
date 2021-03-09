package org.weibo.analysis.graph;

import org.apache.flink.api.java.tuple.Tuple7;
import org.weibo.analysis.entity.Vertex;

public class TupleEdge extends Tuple7<String, String, String, String, String, String, Long> {

    public TupleEdge() {
    }

    public TupleEdge(Edge<Vertex, Vertex> edge) {


        Vertex source = edge.getSource();
        Vertex target = edge.getTarget();

        this.f0 = edge.getId();
        this.f1 = edge.getLabel();
        this.f2 = source.getId();
        this.f3 = source.getLabel();
        this.f4 = target.getId();
        this.f5 = target.getLabel();
        this.f6 = edge.getTimestamp();
    }

    public String getId() {
        return this.f0;
    }

    public String getLabel() {
        return this.f1;
    }

    public String getSourceId() {
        return this.f2;
    }

    public String getSourceLabel() {
        return this.f3;
    }

    public String getTargetId() {
        return this.f4;
    }

    public String getTargetLabel() {
        return this.f5;
    }

    public Long getTimestamp() {
        return this.f6;
    }
}
