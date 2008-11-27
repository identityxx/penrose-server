package org.safehaus.penrose.ldap;

import org.safehaus.penrose.control.Control;
import org.safehaus.penrose.util.TextUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.io.Serializable;

/**
 * @author Endi S. Dewata
 */
public class SearchResult implements Serializable, Cloneable {

    private DN dn;
    private Attributes attributes;
    private String entryId;

    private SourceAttributes sourceAttributes = new SourceAttributes();

    private Collection<Control> controls;

    public SearchResult() {
        attributes = new Attributes();
        controls = new ArrayList<Control>();
    }

    /**
     * Create a search result with the given DN and attributes.
     *
     * This object will take the ownership of the parameters.
     *
     * @param dn DN
     * @param attributes Attributes
     */
    public SearchResult(String dn, Attributes attributes) {
        this.dn = new DN(dn);
        this.attributes = attributes;
        controls = new ArrayList<Control>();
    }

    /**
     * Create a search result with the given DN and attributes.
     *
     * This object will take the ownership of the parameters.
     *
     * @param dn DN
     * @param attributes Attributes
     */
    public SearchResult(DN dn, Attributes attributes) {
        this.dn = dn;
        this.attributes = attributes;
        controls = new ArrayList<Control>();
    }

    /**
     * Create a search result with the given DN, attributes, and controls.
     *
     * This object will take the ownership of the parameters.
     *
     * @param dn DN
     * @param attributes Attributes
     * @param controls Controls
     */
    public SearchResult(DN dn, Attributes attributes, Collection<Control> controls) {
        this.dn = dn;
        this.attributes = attributes;
        this.controls = controls;
    }

    public DN getDn() {
        return dn;
    }

    public void setDn(DN dn) {
        this.dn = dn;
    }

    public Attributes getAttributes() {
        return attributes;
    }

    /**
     * Set the attributes of this search result.
     *
     * This object will take the ownership of the parameters.
     *
     * @param attributes Attributes
     */
    public void setAttributes(Attributes attributes) {
        this.attributes = attributes;
    }

    public void setAttribute(Attribute attribute) {
        attributes.set(attribute);
    }

    public void removeAttribute(String name) {
        attributes.remove(name);
    }
    
    public Attribute getAttribute(String name) {
        return attributes.get(name);
    }

    public Collection<String> getAttributeNames() {
        return attributes.getNames();
    }
    
    public Collection<Control> getControls() {
        return controls;
    }

    /**
     * Set the controls of this search result.
     *
     * This object will take the ownership of the parameters.
     *
     * @param controls Controls
     */
    public void setControls(Collection<Control> controls) {
        this.controls = controls;
    }

    public String getEntryId() {
        return entryId;
    }

    public void setEntryId(String entryId) {
        this.entryId = entryId;
    }

    public SourceAttributes getSourceAttributes() {
        return sourceAttributes;
    }
    
    public void setSourceAttributes(SourceAttributes sourceAttributes) throws Exception {
        this.sourceAttributes.set(sourceAttributes);
    }

    public void print() throws Exception {

        Logger log = LoggerFactory.getLogger(getClass());
        log.debug(TextUtil.displaySeparator(70));
        log.debug(TextUtil.displayLine("Search Result: "+dn, 70));

        for (Attribute attribute : attributes.getAll()) {
            String name = attribute.getName();

            for (Object value : attribute.getValues()) {
                String className = value.getClass().getName();
                className = className.substring(className.lastIndexOf(".") + 1);

                log.debug(TextUtil.displayLine(" - " + name + ": " + value + " (" + className + ")", 70));
            }
        }

        boolean first = true;

        for (String sourceName : sourceAttributes.getNames()) {
            Attributes attrs = sourceAttributes.get(sourceName);

            for (Attribute attribute : attrs.getAll()) {
                String fieldName = sourceName + "." + attribute.getName();

                for (Object value : attribute.getValues()) {
                    String className = value.getClass().getName();
                    className = className.substring(className.lastIndexOf(".") + 1);

                    if (first) {
                        log.debug(TextUtil.displayLine("Source Values:", 70));
                        first = false;
                    }

                    log.debug(TextUtil.displayLine(" - " + fieldName + ": " + value + " (" + className + ")", 70));
                }
            }
        }

        log.debug(TextUtil.displaySeparator(70));
    }

    public Object clone() throws CloneNotSupportedException {
        SearchResult sr = (SearchResult)super.clone();

        sr.entryId = entryId;
        sr.dn = dn;
        sr.attributes = (Attributes)attributes.clone();
        sr.sourceAttributes = (SourceAttributes) sourceAttributes.clone();

        sr.controls = new ArrayList<Control>();
        for (Control control : controls) {
            sr.controls.add((Control)control.clone());
        }

        return sr;
    }
}
