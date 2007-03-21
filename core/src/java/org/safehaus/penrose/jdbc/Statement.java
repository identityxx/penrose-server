package org.safehaus.penrose.jdbc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.adapter.jdbc.Parameter;

import java.util.Collection;
import java.util.ArrayList;

/**
 * @author Endi S. Dewata
 */
public abstract class Statement {

    public Logger log = LoggerFactory.getLogger(getClass());

    protected Collection parameters = new ArrayList();

    public abstract String getSql() throws Exception;

    public Collection getParameters() {
        return parameters;
    }

    public void addParameter(Parameter parameter) {
        parameters.add(parameter);
    }

    public void addParameters(Collection parameters) {
        this.parameters.addAll(parameters);
    }

    public void setParameters(Collection parameters) {
        this.parameters = parameters;
    }

}
