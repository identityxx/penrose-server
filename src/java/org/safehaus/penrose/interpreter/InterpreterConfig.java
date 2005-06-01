package org.safehaus.penrose.interpreter;

import java.util.Properties;
import java.util.Collection;

/**
 * @author Endi S. Dewata
 */
public class InterpreterConfig {

    private String interpreterName;
    private String interpreterClass;
    private String description;

    private Properties parameters = new Properties();

    public String getInterpreterClass() {
        return interpreterClass;
    }

    public void setInterpreterClass(String interpreterClass) {
        this.interpreterClass = interpreterClass;
    }

    public Collection getParameterNames() {
        return parameters.keySet();
    }

    public void setParameter(String name, String value) {
       parameters.setProperty(name, value);
    }

    public String getParameter(String name) {
        return parameters.getProperty(name);
    }

    public String getInterpreterName() {
        return interpreterName;
    }

    public void setInterpreterName(String interpreterName) {
        this.interpreterName = interpreterName;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }
}
