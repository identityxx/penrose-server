package org.safehaus.penrose.apacheds;

import org.safehaus.penrose.entry.Entry;
import org.safehaus.penrose.util.EntryUtil;

import javax.naming.directory.SearchResult;

/**
 * @author Endi S. Dewata
 */
public class EntryTool {

    public static SearchResult createSearchResult(Entry entry) throws Exception {
        return EntryUtil.toSearchResult(entry);
    }
}
