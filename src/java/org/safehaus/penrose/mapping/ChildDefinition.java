/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.mapping;

import java.util.List;
import java.util.ArrayList;

/**
 * @author Endi S. Dewata
 */
public class ChildDefinition {

    private String file;

    private List entryDefinitions = new ArrayList();

    public String getFile() {
        return file;
    }

    public void setFile(String file) {
        this.file = file;
    }

    public List getEntryDefinitions() {
        return entryDefinitions;
    }

    public void setEntryDefinitions(List entryDefinitions) {
        this.entryDefinitions = entryDefinitions;
    }

    public void addEntryDefinition(EntryDefinition entryDefinition) {
        entryDefinitions.add(entryDefinition);
    }
}
