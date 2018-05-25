package org.everthrift.sql.hibernate;

import org.everthrift.sql.hibernate.dao.EntityInterceptor;
import org.everthrift.sql.hibernate.model.MetaDataProviderIF;
import org.hibernate.SessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.orm.hibernate5.LocalSessionFactoryBuilder;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;

public class LocalSessionFactoryBean extends org.springframework.orm.hibernate5.LocalSessionFactoryBean {

    private static final Logger LOG = LoggerFactory.getLogger(LocalSessionFactoryBean.class);

    private MetaDataProviderIF metaDataProvider;

    public LocalSessionFactoryBean() {

    }

    @Override
    protected SessionFactory buildSessionFactory(LocalSessionFactoryBuilder sfb) {
        final String hbmXml = metaDataProvider.getHbmXml();


        sfb.addInputStream(new ByteArrayInputStream(hbmXml.getBytes(StandardCharsets.UTF_8)));
        sfb.setInterceptor(EntityInterceptor.INSTANCE);
        final SessionFactory ret = super.buildSessionFactory(sfb);
        return ret;
    }

    public void setMetaDataProvider(MetaDataProviderIF metaDataProvider) {
        this.metaDataProvider = metaDataProvider;
    }
}
