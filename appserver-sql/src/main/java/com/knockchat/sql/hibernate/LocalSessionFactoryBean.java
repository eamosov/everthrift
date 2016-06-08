package com.knockchat.sql.hibernate;

import java.io.ByteArrayInputStream;

import org.hibernate.SessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.orm.hibernate5.LocalSessionFactoryBuilder;

import com.knockchat.sql.hibernate.dao.EntityInterceptor;
import com.knockchat.sql.hibernate.model.MetaDataProvider;

public class LocalSessionFactoryBean extends org.springframework.orm.hibernate5.LocalSessionFactoryBean {


    private static final Logger LOG = LoggerFactory.getLogger(LocalSessionFactoryBean.class);

    private MetaDataProvider metaDataProvider;

    @Override
    protected SessionFactory buildSessionFactory(LocalSessionFactoryBuilder sfb) {
        sfb.addInputStream(new ByteArrayInputStream(metaDataProvider.toHbmXml().getBytes()));
        sfb.setInterceptor(EntityInterceptor.INSTANCE);
        final SessionFactory ret =  super.buildSessionFactory(sfb);
        return ret;
    }

    public void setMetaDataProvider(MetaDataProvider metaDataProvider) {
        this.metaDataProvider = metaDataProvider;
    }
}
