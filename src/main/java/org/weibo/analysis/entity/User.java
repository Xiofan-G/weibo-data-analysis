package org.weibo.analysis.entity;

import java.util.ArrayList;


public class User extends Vertex {
    public String name;
    public String city;
    public Integer age;
    public String gender;
    public transient ArrayList<User> fans;

    public User() {
    }

    public User(String name, String id, String city, int age, String gender) {
        super(id, "");
        this.name = name;
        this.city = city;
        this.age = age;
        this.gender = gender;
        this.fans = new ArrayList<>();
    }


    public String getName() {
        return name;
    }


    public String getCity() {
        return city;
    }

    public int getAge() {
        return age;
    }

    public String getGender() {
        return gender;
    }


    public ArrayList<User> getFans() {
        return fans;
    }

    public void followBy(User user) {
        this.fans.add(user);
    }


    @Override
    public String toString() {
        String properties = String.format(
                "\"properties\":" +
                        "{" +
                        "\"name\":\"%s\"," +
                        "\"city\":\"%s\"," +
                        "\"gender\":\"%s\"," +
                        "\"age\":%d" +
                        "}",
                name, city, gender, age);
        return String.format("%s,%s}", super.toString().replace("}",""), properties);
    }
}
