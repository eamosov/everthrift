package org.everthrift.thrift;

import org.apache.thrift.TBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.type.filter.AssignableTypeFilter;
import org.springframework.util.ClassUtils;

public class MetaDataMapBuilder {

    private static final Logger log = LoggerFactory.getLogger(MetaDataMapBuilder.class);

    final ClassPathScanningCandidateComponentProvider scanner;

    public MetaDataMapBuilder() {
        scanner = new ClassPathScanningCandidateComponentProvider(false);
        scanner.addIncludeFilter(new AssignableTypeFilter(TBase.class));
    }

    public void build(String basePackage) {

        int i = 0;
        for (BeanDefinition b : scanner.findCandidateComponents(basePackage)) {

            log.debug("find {}", b.getBeanClassName());

            try {
                final Class cls = ClassUtils.resolveClassName(b.getBeanClassName(), ClassUtils.getDefaultClassLoader());
                cls.newInstance();
                i++;
            } catch (Exception e) {
                log.error("Exception", e);
            }
        }

        log.info("Successfully scanned {} TBase classes at {}", i, basePackage);
    }
}
