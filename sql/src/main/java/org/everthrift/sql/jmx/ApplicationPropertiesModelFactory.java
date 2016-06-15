package org.everthrift.sql.jmx;

import java.util.List;

import org.everthrift.sql.pgsql.PgSqlModelFactory;
import org.hibernate.criterion.Restrictions;

public class ApplicationPropertiesModelFactory extends PgSqlModelFactory<String, ApplicationPropertiesModel> {

	public ApplicationPropertiesModelFactory() {
		super(null, ApplicationPropertiesModel.class);
	}
	
	public List<ApplicationPropertiesModel> findByPersistanceName(String persistanceName){
		return this.getDao().findByCriteria(Restrictions.eq("persistanceName", persistanceName), null);
	}

}
