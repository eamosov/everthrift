package com.knockchat.appserver.jmx;

import java.util.List;

import org.hibernate.criterion.Restrictions;
import org.springframework.stereotype.Component;

import com.knockchat.node.model.pgsql.PgSqlModelFactory;

@Component
public class ApplicationPropertiesModelFactory extends PgSqlModelFactory<String, ApplicationPropertiesModel> {

	public ApplicationPropertiesModelFactory() {
		super(null, ApplicationPropertiesModel.class);
	}
	
	public List<ApplicationPropertiesModel> findByPersistanceName(String persistanceName){
		return this.getDao().findByCriteria(Restrictions.eq("persistanceName", persistanceName), null);
	}

}
