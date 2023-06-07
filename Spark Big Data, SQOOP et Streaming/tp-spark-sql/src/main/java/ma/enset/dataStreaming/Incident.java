package ma.enset.dataStreaming;

import java.util.Date;

public class Incident {
    private int id;
    private String nom_avion, description;
    private Date date;

    public Incident() {
    }

    public Incident(int id, String nom_avion, String description, Date date) {
        this.id = id;
        this.nom_avion = nom_avion;
        this.description = description;
        this.date = date;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getNom_avion() {
        return nom_avion;
    }

    public void setNom_avion(String nom_avion) {
        this.nom_avion = nom_avion;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }
}
