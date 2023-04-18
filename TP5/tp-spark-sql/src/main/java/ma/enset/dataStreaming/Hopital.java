package ma.enset.dataStreaming;

import java.util.Date;

public class Hopital {
    private int Id;
    private String titre, description, service;
    private Date date;

    public Hopital() {
    }

    public Hopital(int id, String titre, String description, String service, Date date) {
        Id = id;
        this.titre = titre;
        this.description = description;
        this.service = service;
        this.date = date;
    }

    public int getId() {
        return Id;
    }

    public void setId(int id) {
        Id = id;
    }

    public String getTitre() {
        return titre;
    }

    public void setTitre(String titre) {
        this.titre = titre;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getService() {
        return service;
    }

    public void setService(String service) {
        this.service = service;
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }
}
