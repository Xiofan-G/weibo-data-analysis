package org.weibo.analysis.entity;


import java.util.ArrayList;


public class Comment extends Vertex {
    public String name;  // Weibo's content, max size is 140 unicode
    public String agent; // User's phone type, such as Iphone X
    public User author;
    public Weibo replyOf;
    public transient ArrayList<User> atUsers;
    public transient ArrayList<Weibo> mentionedWeibos;

    public Comment() {
    }

    public Comment(String id, String name, String agent, User author, Weibo replyOf) {
        super(id, "");
        this.name = name;
        this.agent = agent;
        this.author = author;
        this.replyOf = replyOf;
        this.atUsers = new ArrayList<>();
        this.mentionedWeibos = new ArrayList<>();
    }


    public String getName() {
        return name;
    }

    public String getAgent() {
        return agent;
    }

    public User getAuthor() {
        return author;
    }

    public Weibo getReplyOf() {
        return replyOf;
    }

    public ArrayList<User> getAtUsers() {
        return atUsers;
    }

    public ArrayList<Weibo> getMentionedWeibos() {
        return mentionedWeibos;
    }

    public void addAtUser(User atUser) {
        this.atUsers.add(atUser);
    }

    public void addMentionedWeibo(Weibo mentionedWeibo) {
        this.mentionedWeibos.add(mentionedWeibo);
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
