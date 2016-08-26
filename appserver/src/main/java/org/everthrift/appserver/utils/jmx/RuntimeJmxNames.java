package org.everthrift.appserver.utils.jmx;

public interface RuntimeJmxNames {
    /**
     * this is the name= part of the object name
     */
    public String getJmxName();

    /**
     * this sets the folders as 00=FirstFolder,01=Second
     */
    public String[] getJmxPath();
}