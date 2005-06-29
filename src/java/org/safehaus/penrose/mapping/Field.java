/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.mapping;

/**
 * @author Endi S. Dewata
 */
public class Field {

    private FieldDefinition fieldDefinition;

	/**
	 * Name.
	 */
	private String name;

	/**
	 * Expression.
	 */
	private String expression;


    public Field() {
    }
    
    public Field(String name) {
        this.name = name;
    }

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getExpression() {
		return expression;
	}

	public void setExpression(String expression) {
		this.expression = expression;
	}

    public boolean isPrimaryKey() {
        return fieldDefinition.isPrimaryKey();
    }

    public String getOriginalName() {
        return fieldDefinition.getOriginalName();
    }

    public String getEncryption() {
        return fieldDefinition.getEncryption();
    }

    public String getEncoding() {
        return fieldDefinition.getEncoding();
    }

    public FieldDefinition getFieldDefinition() {
        return fieldDefinition;
    }

    public void setFieldDefinition(FieldDefinition fieldDefinition) {
        this.fieldDefinition = fieldDefinition;
    }

    public String getType() {
        return fieldDefinition.getType();
    }
}