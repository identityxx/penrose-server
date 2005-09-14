/**
 * Copyright (c) 2000-2005, Identyx Corporation.
 * All rights reserved.
 */
package org.safehaus.penrose.mapping;

/**
 * @author Endi S. Dewata
 */
public class FieldDefinition implements Comparable, Cloneable {

	/**
	 * Name.
	 */
	private String name;

    private String originalName;

    private String type = "VARCHAR";

	/**
	 * This is a primary key.
	 */
	private boolean primaryKey;

    /**
     * Encryption method used to encrypt the value
     */
    private String encryption;

    /**
     * Encoding method used to encode the value
     */
    private String encoding;

	public FieldDefinition() {
	}

    public FieldDefinition(String name) {
        this.name = name;
    }

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public boolean isPrimaryKey() {
		return primaryKey;
	}

	public void setPrimaryKey(boolean primaryKey) {
		this.primaryKey = primaryKey;
	}
	
    public String getOriginalName() {
        return originalName == null ? name : originalName;
    }

    public void setOriginalName(String originalName) {
        this.originalName = originalName;
    }

    public String getEncryption() {
        return encryption;
    }

    public void setEncryption(String encryption) {
        this.encryption = encryption;
    }

    public String getEncoding() {
        return encoding;
    }

    public void setEncoding(String encoding) {
        this.encoding = encoding;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public int hashCode() {
        return name.hashCode();
    }

    boolean equals(Object o1, Object o2) {
        if (o1 == null && o2 == null) return true;
        if (o1 != null) return o1.equals(o2);
        return o2.equals(o1);
    }

    public boolean equals(Object object) {
        if (object == null) return false;
        if (!(object instanceof FieldDefinition)) return false;

        FieldDefinition fd = (FieldDefinition)object;
        if (!equals(name, fd.name)) return false;
        if (!equals(originalName, fd.originalName)) return false;
        if (!equals(type, fd.type)) return false;
        if (primaryKey != fd.primaryKey) return false;
        if (!equals(encryption, fd.encryption)) return false;
        if (!equals(encoding, fd.encoding)) return false;

        return true;
    }

    public int compareTo(Object object) {
        if (object == null) return 0;
        if (!(object instanceof FieldDefinition)) return 0;

        FieldDefinition fd = (FieldDefinition)object;
        return name.compareTo(fd.name);
    }

    public Object clone() {
        FieldDefinition fieldDefinition = new FieldDefinition();
        fieldDefinition.name = name;
        fieldDefinition.originalName = originalName;
        fieldDefinition.type = type;
        fieldDefinition.primaryKey = primaryKey;
        fieldDefinition.encryption = encryption;
        fieldDefinition.encoding = encoding;
        return fieldDefinition;
    }

}