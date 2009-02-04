package org.safehaus.penrose.example.abandon;

import org.safehaus.penrose.directory.Entry;
import org.safehaus.penrose.operation.SearchOperation;
import org.safehaus.penrose.ldap.*;

/**
 * @author Endi Sukma Dewata
 */
public class AbandonEntry extends Entry {

    public boolean validateFilter(SearchOperation operation) throws Exception {
        return true;
    }

    public void expand(
            SearchOperation operation
    ) throws Exception {

        for (int i=0; i<10; i++) {

            if (operation.isAbandoned()) {
                if (debug) log.debug("Operation "+operation.getOperationName()+" has been abandoned.");
                return;
            }

            RDNBuilder rb = new RDNBuilder();
            rb.set("uid", "user"+i);
            RDN rdn = rb.toRdn();

            DN dn = rdn.append(getParentDn());
            log.debug("Returning "+dn+".");

            Attributes attributes = new Attributes();
            attributes.addValue("objectClass", "person");
            attributes.addValue("objectClass", "organizationalPerson");
            attributes.addValue("objectClass", "inetOrgPerson");
            attributes.addValue("uid", "user"+i);
            attributes.addValue("cn", "user"+i);
            attributes.addValue("sn", "user"+i);

            SearchResult result = new SearchResult(dn, attributes);
            result.setEntryName(getName());

            operation.add(result);

            Thread.sleep(5000);
        }
    }
}
