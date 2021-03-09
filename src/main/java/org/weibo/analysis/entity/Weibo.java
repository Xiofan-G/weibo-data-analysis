package org.weibo.analysis.entity;

import java.util.ArrayList;


public class Weibo extends Vertex {
    public String name;  // Weibo's content, max size is 140 unicode
    public String agent; // User's phone type, such as Iphone X
    public transient ArrayList<Tag> tags; // Weibo's Tag
    public User author;  // weibo's author
    public transient ArrayList<Weibo> mentionedWeibos;
    public transient ArrayList<User> atUsers;

    public Weibo() {
    }

    public Weibo(String id, String name, String agent, ArrayList<Tag> tags, User author) {
        super(id, "");
        this.name = name;
        this.agent = agent;
        this.tags = tags;
        this.author = author;
        this.mentionedWeibos = new ArrayList<>();
        this.atUsers = new ArrayList<>();
    }


    public String getName() {
        return name;
    }

    public String getAgent() {
        return agent;
    }

    public ArrayList<Tag> getTags() {
        return tags;
    }

    public User getAuthor() {
        return author;
    }

    public void addMentionedWeibo(Weibo weibo) {
        this.mentionedWeibos.add(weibo);
    }

    public void addAtUser(User atUser) {
        this.atUsers.add(atUser);
    }

    public ArrayList<Weibo> getMentionedWeibos() {
        return mentionedWeibos;
    }

    public ArrayList<User> getAtUsers() {
        return atUsers;
    }

    @Override
    public String toString() {
        String properties = String.format(
                "\"properties\":" +
                        "{" +
                        "\"name\":\"%s\"," +
                        "\"agent\":\"%s\"" +
                        "}",
                name, agent);
        return String.format("%s,%s}", super.toString().replace("}", ""), properties);
    }
}
