package org.everthrift.sql.hibernate.model;

import org.jetbrains.annotations.NotNull;

/**
 * Created by fluder on 03/04/2018.
 */
public interface MetaDataProviderIF {

    @NotNull
    String getHbmXml();

    String getComments();
}
