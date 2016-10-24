package org.everthrift.elastic;

/**
 * Created by fluder on 21.10.16.
 */
public interface IndexTaskIF {

    int getOperation();

    String getIndexName();

    String getMappingName();

    int getVersionType();

    long getVersion();

    String getId();

    String getSource();

    String getParentId();
}
