/**
 * Copyright (c) 2000-2006, Identyx Corporation.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */
package org.safehaus.penrose.cache;

import org.safehaus.penrose.mapping.EntryMapping;
import org.safehaus.penrose.partition.Partition;

/**
 * @author Endi S. Dewata
 */
public class InMemoryEntryCache extends EntryCache {

    public EntryCacheStorage createCacheStorage(Partition partition, EntryMapping entryMapping) throws Exception {

        EntryCacheStorage cacheStorage = new InMemoryEntryCacheStorage(penrose);
        cacheStorage.setCacheConfig(cacheConfig);
        cacheStorage.setPartition(partition);
        cacheStorage.setEntryMapping(entryMapping);

        cacheStorage.init();

        return cacheStorage;
    }
}
