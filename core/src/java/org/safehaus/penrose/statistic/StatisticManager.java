package org.safehaus.penrose.statistic;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public class StatisticManager {

    public final static String ABANDON = "abandon";
    public final static String ADD     = "add";
    public final static String BIND    = "bind";
    public final static String COMPARE = "compare";
    public final static String DELETE  = "delete";
    public final static String MODIFY  = "modify";
    public final static String MODRDN  = "modrdn";
    public final static String SEARCH  = "search";
    public final static String UNBIND  = "unbind";

    public Map<String,Statistic> statistics = new LinkedHashMap<String,Statistic>();

    public StatisticManager() {
        addStatistic(new Statistic(ABANDON));
        addStatistic(new Statistic(ADD));
        addStatistic(new Statistic(BIND));
        addStatistic(new Statistic(COMPARE));
        addStatistic(new Statistic(DELETE));
        addStatistic(new Statistic(MODIFY));
        addStatistic(new Statistic(MODRDN));
        addStatistic(new Statistic(SEARCH));
        addStatistic(new Statistic(UNBIND));
    }

    public void addStatistic(Statistic statistic) {
        statistics.put(statistic.getName(), statistic);
    }

    public Collection<String> getStatisticNames() {
        return statistics.keySet();
    }

    public Statistic getStatistic(String name) {
        return statistics.get(name);
    }

    public void incrementCounter(String name) {
        Statistic statistic = getStatistic(name);
        if (statistic == null) return;
        statistic.incrementCounter();
    }
}
