package com.knockchat.sql.jmx;

import java.util.List;

import org.hibernate.criterion.Restrictions;

import com.knockchat.sql.pgsql.PgSqlModelFactory;

public class ApplicationPropertiesModelFactory extends PgSqlModelFactory<String, ApplicationPropertiesModel> {

	public ApplicationPropertiesModelFactory() {
		super(null, ApplicationPropertiesModel.class);
	}
	
	public List<ApplicationPropertiesModel> findByPersistanceName(String persistanceName){
		return this.getDao().findByCriteria(Restrictions.eq("persistanceName", persistanceName), null);
	}

}
