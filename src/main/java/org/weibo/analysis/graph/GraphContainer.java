package org.weibo.analysis.graph;

import org.weibo.analysis.entity.RelationLabel;
import org.weibo.analysis.entity.Style;
import org.weibo.analysis.entity.Vertex;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class GraphContainer implements Serializable {
    private static final long serialVersionUID = 1L;
    private HashMap<String, Edge<Vertex, Vertex>> edges;
    private HashMap<String, Vertex> vertices;
    private String version;
    private ArrayList<Style> styles = new ArrayList<>();


    public GraphContainer() {
        edges = new HashMap<>();
        vertices = new HashMap<>();
        styles.add(new Style("User", "#CD5C5C"));
        styles.add(new Style("Tag", "#666699"));
        styles.add(new Style("Weibo", "#B0E0E6"));
        styles.add(new Style("Comment", "#DAA520"));
        styles.add(new Style("Author", "#669966"));
        styles.add(new Style("Fans", "#669999"));
        styles.add(new Style("At", "#66CCCC"));
        styles.add(new Style("Mentioned", "#66CC99"));
        styles.add(new Style("ReplyOf", "#66CC66"));


    }

    public void addEdge(Edge<Vertex, Vertex> edge) {
        edges.put(edge.getId(), edge);
        vertices.put(edge.getSource().getId(), edge.getSource());
        vertices.put(edge.getTarget().getId(), edge.getTarget());
    }

    public int size() {
        int size = 0;
        for (Map.Entry<String, Edge<Vertex, Vertex>> entry : this.edges.entrySet()) {
            size += entry.getValue().count;
        }
        return size;
    }

    /**
     * Add grouped edge and vertex
     *
     * @param sourceLabel
     * @param targetLabel
     * @param edgeLabel
     * @param count
     */
    public void addEdge(Object sourceLabel, Object targetLabel, Object edgeLabel, Object count) {
        Vertex source = this.addVertex(sourceLabel, sourceLabel, (int) count);
        Vertex target = this.addVertex(targetLabel, targetLabel, (int) count);

        Edge<Vertex, Vertex> edge = new Edge<>(source, target, edgeLabel.toString(), edgeLabel.toString(), String.format("%s->%s", sourceLabel, targetLabel), new Timestamp(System.currentTimeMillis()).getTime());
        edge.addCount((int) count);
        edges.put(edgeLabel.toString(), edge);

    }

    public Vertex addVertex(Object id, Object type, int count) {
        Vertex vertex = this.vertices.get(id.toString());
        if (this.vertices.get(id.toString()) == null) {
            vertex = new Vertex(id.toString(), type.toString());
            this.vertices.put(id.toString(), vertex);
        }
        vertex.addCount(count);
        return vertex;
    }

    public void clear() {
        this.vertices.clear();
        this.edges.clear();
    }

    public String getVersion() {
        return this.version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public void mergeLabel() {
        String newLabel = RelationLabel.At.getLabel() + " | " + RelationLabel.Author.getLabel();
        if (this.edges.size() > 0) {
            HashMap<String, Edge<Vertex, Vertex>> newEdges = new HashMap<>();
            for (Map.Entry<String, Edge<Vertex, Vertex>> entry : this.edges.entrySet()) {
                Edge<Vertex, Vertex> value = entry.getValue();
                String k = entry.getKey();
                if (value.getLabel().equals(RelationLabel.At.getLabel()) || value.getLabel().equals(RelationLabel.Author.getLabel())) {
                    if (value.getLabel().equals(RelationLabel.At.getLabel())) {
                        Vertex source = value.getSource();
                        Vertex target = value.getTarget();
                        value.target = source;
                        value.source = target;
                    }
                    value.setLabel(newLabel);
                    value.id = newLabel;
                }

                newEdges.put(k, value);
            }
            this.edges = newEdges;
        }
    }

    @Override
    public String toString() {
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
//        if (edges.size() <= 0) {
//            return String.format("{\"timestamp\":%d}", timestamp.getTime());
//        }

        StringBuilder vertices = new StringBuilder("[");
        if (this.vertices.size() > 0) {
            for (Map.Entry<String, Vertex> entry : this.vertices.entrySet()) {
                Vertex v = entry.getValue();
                vertices.append(v.toString());
                vertices.append(",");
            }
            vertices = new StringBuilder(vertices.substring(0, vertices.length() - 1));
        }
        vertices.append("]");

        StringBuilder edges = new StringBuilder("[");
        if (this.edges.size() > 0) {
            for (Map.Entry<String, Edge<Vertex, Vertex>> entry : this.edges.entrySet()) {
                String k = entry.getKey();
                Edge<Vertex, Vertex> v = entry.getValue();
                edges.append(v.toString());
                edges.append(",");
            }
            edges = new StringBuilder(edges.substring(0, edges.length() - 1));
        }
        edges.append("]");

        StringBuilder styles = new StringBuilder("[");
        if (this.styles.size() > 0) {
            for (Style entry : this.styles) {
                styles.append(entry.toString());
                styles.append(",");
            }
            styles = new StringBuilder(styles.substring(0, styles.length() - 1));
        }
        styles.append("]");


        return String.format("{" +
                        "\"vertices\":%s, " +
                        "\"edges\":%s, " +
                        "\"version\":\"%s\", " +
                        "\"styles\":%s," +
                        "\"timestamp\":%d}",
                vertices.toString(), edges.toString(), version, styles.toString(), timestamp.getTime());

    }
}
