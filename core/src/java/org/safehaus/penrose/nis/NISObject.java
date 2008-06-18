package org.safehaus.penrose.nis;

/**
 * @author Endi Sukma Dewata
 */
public class NISObject {

    public String name;
    public String value;
    public String description;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }
}
