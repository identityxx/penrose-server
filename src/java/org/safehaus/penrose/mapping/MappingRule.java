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
public class MappingRule {

    private String file;

    private String baseDn;

    private List contents = new ArrayList();

    public String getFile() {
        return file;
    }

    public void setFile(String file) {
        this.file = file;
    }

    public List getContents() {
        return contents;
    }

    public void setContents(List contents) {
        this.contents = contents;
    }

    public void addContent(Object object) {
        contents.add(object);
    }

    public String getBaseDn() {
        return baseDn;
    }

    public void setBaseDn(String baseDn) {
        this.baseDn = baseDn;
    }
}
