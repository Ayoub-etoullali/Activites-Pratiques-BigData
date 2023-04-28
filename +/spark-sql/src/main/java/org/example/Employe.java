package org.example;

import java.io.Serializable;

public class Employe implements Serializable {
    private long id;
    private String name;
    private long age;
    private double salary;
    private String departement;

    public Employe() {
    }

    public Employe(int id, String name, int age, double salary, String departement) {
        this.id = id;
        this.name = name;
        this.age = age;
        this.salary = salary;
        this.departement = departement;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getAge() {
        return age;
    }

    public void setAge(long age) {
        this.age = age;
    }

    public double getSalary() {
        return salary;
    }

    public void setSalary(double salary) {
        this.salary = salary;
    }

    public String getDepartement() {
        return departement;
    }

    public void setDepartement(String departement) {
        this.departement = departement;
    }
}
