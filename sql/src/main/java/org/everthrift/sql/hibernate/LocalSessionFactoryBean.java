package org.everthrift.sql.hibernate;

import org.everthrift.sql.hibernate.dao.EntityInterceptor;
import org.everthrift.sql.hibernate.model.MetaDataProvider;
import org.hibernate.SessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.orm.hibernate5.LocalSessionFactoryBuilder;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;

public class LocalSessionFactoryBean extends org.springframework.orm.hibernate5.LocalSessionFactoryBean {

    private static final Logger LOG = LoggerFactory.getLogger(LocalSessionFactoryBean.class);

    private MetaDataProvider metaDataProvider;
    private final boolean dumpHbm;

    public LocalSessionFactoryBean(boolean dumpHbm) {
        this.dumpHbm = dumpHbm;
    }

    @Override
    protected SessionFactory buildSessionFactory(LocalSessionFactoryBuilder sfb) {
        final String hbmXml = metaDataProvider.toHbmXml();

        if (dumpHbm) {
            LOG.info("HBM:\n{}", hbmXml);
        }

        sfb.addInputStream(new ByteArrayInputStream(hbmXml.getBytes(StandardCharsets.UTF_8)));
        sfb.setInterceptor(EntityInterceptor.INSTANCE);
        final SessionFactory ret = super.buildSessionFactory(sfb);
        return ret;
    }

    public void setMetaDataProvider(MetaDataProvider metaDataProvider) {
        this.metaDataProvider = metaDataProvider;
    }
}
