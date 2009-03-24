package org.safehaus.penrose.nis;

import java.util.Map;
import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public interface NISSynchronizationModuleMBean {

    public Map<String,String> getNisMapRDNs() throws Exception;
    public Collection<String> getNisMaps() throws Exception;
    public String getNisMapRDN(String nisMap) throws Exception;
}