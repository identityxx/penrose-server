package org.safehaus.penrose.nis.module;

import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.ldap.module.SnapshotSyncModule;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.nis.NIS;

import java.util.ArrayList;
import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public class NISSynchronizationModule extends SnapshotSyncModule {

    public boolean checkSearchResult(SearchResult result) throws Exception {

        Attributes attributes = result.getAttributes();
        Attribute objectClass = attributes.get("objectClass");

        if (objectClass != null && objectClass.containsValue("nisNoSync")) {
            if (warn) log.warn("Don't synchronize "+result.getDn()+".");
            return false;
        }

        return true;
    }

    public Collection<Modification> createModifications(
            Attributes attributes1,
            Attributes attributes2
    ) throws Exception {

        convertAutomount(attributes2);

        return super.createModifications(
                attributes1,
                attributes2
        );
    }

    public void convertAutomount(Attributes attributes) throws Exception {

        Attribute attribute = attributes.get("nisMapEntry");
        if (attribute == null) return;

        Partition sourcePartition = getSourcePartition();
        Partition targetPartition = getTargetPartition();

        DN sourceSuffix = sourcePartition.getDirectory().getSuffix();
        DN targetSuffix = targetPartition.getDirectory().getSuffix();

        Collection<Object> removeList = new ArrayList<Object>();
        Collection<Object> addList = new ArrayList<Object>();

        for (Object object : attribute.getValues()) {
            String value = object.toString();
            if (!value.startsWith("ldap:")) continue;
            int i = value.indexOf(' ', 5);

            String name;
            String info;

            if (i < 0) {
                name = value.substring(5);
                info = null;
            } else {
                name = value.substring(5, i);
                info = value.substring(i+1);
            }

            DN dn = new DN(name);
            DN newDn = dn.getPrefix(sourceSuffix).append(targetSuffix);

            String newValue = "ldap:"+newDn+(info == null ? "" : " "+info);

            removeList.add(value);
            addList.add(newValue);
        }

        attribute.removeValues(removeList);
        attribute.addValues(addList);
    }

    public void synchronizeMap(String map) throws Exception {

        log.debug("Synchronizing NIS map "+map+"...");

        String label = NIS.mapLabels.get(map);

        if (label == null) {
            log.debug("Unknown NIS map: "+map);
            return;
        }

        DN targetSuffix = getTargetSuffix();
        
        DNBuilder db = new DNBuilder();
        db.append("ou="+label);
        db.append(targetSuffix);
        DN targetDn = db.toDn();

        synchronize(targetDn);
    }
}
