package org.everthrift.sql.jmx;

import java.io.Serializable;

import org.everthrift.appserver.model.DaoEntityIF;

public class ApplicationPropertiesModel implements DaoEntityIF {

    private static final long serialVersionUID = 1L;

    private String id;
    private String persistanceName;
    private String propertyName;
    private String propertyValue;

    public ApplicationPropertiesModel(){

    }

    public String getPropertyName() {
        return propertyName;
    }
    public void setPropertyName(String propertyName) {
        this.propertyName = propertyName;
    }
    public String getPropertyValue() {
        return propertyValue;
    }
    public void setPropertyValue(String propertyValue) {
        this.propertyValue = propertyValue;
    }

    @Override
    public Serializable getPk() {
        return id;
    }
    @Override
    public void setPk(Serializable identifier) {
        id = (String)identifier;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getPersistanceName() {
        return persistanceName;
    }

    public void setPersistanceName(String persistenceName) {
        this.persistanceName = persistenceName;
    }

}
