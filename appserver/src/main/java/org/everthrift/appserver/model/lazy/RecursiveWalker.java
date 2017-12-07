package org.everthrift.appserver.model.lazy;

import org.apache.thrift.TBase;
import org.everthrift.appserver.utils.thrift.scanner.TBaseScanHandler;
import org.everthrift.appserver.utils.thrift.scanner.TBaseScanner;
import org.everthrift.appserver.utils.thrift.scanner.TBaseScannerFactory;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.ClassUtils;

import java.util.List;
import java.util.Map;
import java.util.RandomAccess;

public class RecursiveWalker implements WalkerIF {

    private static final Logger log = LoggerFactory.getLogger(RecursiveWalker.class);

    @NotNull
    public static String SCENARIO_DEFAULT = "default";

    private final static TBaseScannerFactory scannerFactory = new TBaseScannerFactory();

    private static final String defaultFields[] = new String[]{"*"};

    private final Registry registry;

    private final String scenario;

    @Nullable
    private final TBaseScanHandler tBaseScanHandler = new TBaseScanHandler() {

        @Override
        public void apply(Object parent, @Nullable Object o) {

            if (o == null) {
                return;
            }

            final Class cls = o.getClass();
            if (ClassUtils.isPrimitiveOrWrapper(cls) || cls.getCanonicalName().startsWith("java.lang.")) {
                return;
            }

            final TBaseScanner s = scannerFactory.create(cls, scenario);

            if (s == null) {
                log.error("Coudn't get TBaseScanner for class={} and scenario={}", o.getClass()
                                                                                    .getSimpleName(), scenario);
                return;
            }

            s.scan(parent, o, this, registry);
        }
    };

    public RecursiveWalker(Registry registry, String scenario) {
        this.registry = registry;
        this.scenario = scenario;
    }

    @Override
    public void apply(Object o) {
        registry.clear();
        recursive(o);
    }

    private void recursive(@Nullable final Object o) {

        if (o == null) {
            return;
        }

        if (o instanceof RandomAccess) {
            final List _l = (List) o;
            for (int i = 0; i < _l.size(); i++) {
                final Object j = _l.get(i);
                if (j != null) {
                    recursive(j);
                }
            }

        } else if (o instanceof Iterable) {
            for (Object i : ((Iterable) o)) {
                if (i != null) {
                    recursive(i);
                }
            }
        } else if (o instanceof Map) {
            for (Object i : ((Map) o).values()) {
                if (i != null) {
                    recursive(i);
                }
            }
        } else {

            if (o instanceof TBase) {
                scannerFactory.create((Class) o.getClass(), scenario).scan(null, (TBase) o, tBaseScanHandler, registry);
            }

        }
    }
}
