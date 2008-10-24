package org.safehaus.penrose.statistic;

import java.util.Date;

/**
 * @author Endi Sukma Dewata
 */
public class Statistic {
    
    private String name;

    private long counter;

    public Statistic(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getCounter() {
        return counter;
    }

    public void setCounter(long counter) {
        this.counter = counter;
    }

    public void incrementCounter() {
        counter++;
    }

    public void resetCounter() {
        counter = 0;
    }
}
