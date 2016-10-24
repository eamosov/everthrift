package org.everthrift.elastic;

import com.google.common.base.Throwables;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import jdk.nashorn.api.scripting.JSObject;
import jdk.nashorn.api.scripting.ScriptObjectMirror;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.util.ClassUtils;

import javax.script.Bindings;
import javax.script.Invocable;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class AbstractJsQueryBuilder {

    private static final Logger log = LoggerFactory.getLogger(AbstractJsQueryBuilder.class);

    private final ScriptEngineManager factory = new ScriptEngineManager();
    private final ScriptEngine engine = factory.getEngineByName("nashorn");
    private final ScriptObjectMirror json;
    private final ScriptObjectMirror emptyArray;

    private final Gson gson = new GsonBuilder().create();

    private final String nativeArray = "var native_array = function() {\n" +
        "    var ret = [];\n" +
        "    for (var i = 0; i < arguments.length; i++) {\n" +
        "        ret.push(arguments[i]);\n" +
        "    }\n" +
        "    return ret;\n" +
        "}";

    public AbstractJsQueryBuilder(String jsSrc) {
        this(new String[]{jsSrc});
    }

    public AbstractJsQueryBuilder(String[] jsSrc) {

        try {
            engine.eval(nativeArray);
            json = (ScriptObjectMirror) engine.eval("JSON");
            emptyArray = (ScriptObjectMirror) engine.eval("[]");

            for (String r : jsSrc) {
                try (final InputStream is = new FileSystemResource(r).getInputStream()) {
                    engine.eval(IOUtils.toString(is));
                }
            }

        } catch (ScriptException | IOException e) {
            throw Throwables.propagate(e);
        }
    }

    public AbstractJsQueryBuilder(Resource[] src) {

        try {
            engine.eval(nativeArray);
            json = (ScriptObjectMirror) engine.eval("JSON");
            emptyArray = (ScriptObjectMirror) engine.eval("[]");

            for (Resource r : src) {
                try (final InputStream is = r.getInputStream()) {
                    engine.eval(IOUtils.toString(is));
                }
            }

        } catch (ScriptException | IOException e) {
            throw Throwables.propagate(e);
        }
    }

    public synchronized void setGlobalVars(Map<String, Object> vars) {
        Bindings b = engine.getBindings(ScriptContext.ENGINE_SCOPE);
        b.putAll(vars.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> toJsArg(e.getValue()))));
        engine.setBindings(b, ScriptContext.ENGINE_SCOPE);
    }

    private Object toJsArg(Object arg) {

        if (arg == null) {
            return null;
        }

        if (arg instanceof List) {
            if (((List) arg).isEmpty()) {
                return emptyArray;
            } else if (((List) arg).size() == 1) {
                try {
                    return ((Invocable) engine).invokeFunction("native_array", toJsArg(((List) arg).get(0)));
                } catch (ScriptException | NoSuchMethodException e) {
                    throw Throwables.propagate(e);
                }
            }
            return toJsArg(((List) arg).toArray());
        } else if (arg instanceof Object[]) {
            final Object args[] = ((Object[]) arg);

            if (args.length == 0) {
                return emptyArray;
            } else if (args.length == 1) {
                try {
                    return ((Invocable) engine).invokeFunction("native_array", toJsArg(args[0]));
                } catch (ScriptException | NoSuchMethodException e) {
                    throw Throwables.propagate(e);
                }
            }

            final Object nativeArgs[] = new Object[args.length];
            for (int i = 0; i < args.length; i++) {
                nativeArgs[i] = toJsArg(args[i]);
            }

            try {
                return ((Invocable) engine).invokeFunction("native_array", nativeArgs);
            } catch (ScriptException | NoSuchMethodException e) {
                throw Throwables.propagate(e);
            }
        } else if (arg instanceof byte[]) {
            return toJsArg(ArrayUtils.toObject((byte[]) arg));
        } else if (arg instanceof char[]) {
            return toJsArg(ArrayUtils.toObject((char[]) arg));
        } else if (arg instanceof int[]) {
            return toJsArg(ArrayUtils.toObject((int[]) arg));
        } else if (arg instanceof long[]) {
            return toJsArg(ArrayUtils.toObject((long[]) arg));
        } else if (arg instanceof float[]) {
            return toJsArg(ArrayUtils.toObject((float[]) arg));
        } else if (arg instanceof double[]) {
            return toJsArg(ArrayUtils.toObject((double[]) arg));
        } else if (ClassUtils.isPrimitiveOrWrapper(arg.getClass()) || arg instanceof String || arg instanceof JSObject) {
            return arg;
        } else if (arg instanceof JsonElement) {
            return json.callMember("parse", arg.toString());
        } else {
            return json.callMember("parse", gson.toJson(arg));
        }
    }

    public synchronized Object getVariable(String jsVarName) {
        return engine.get(jsVarName);
    }

    public synchronized String getQuery(String jsVarName, Object... args) {

        Invocable invocable = (Invocable) engine;

        final Object[] jsArgs = new Object[args.length];
        for (int i = 0; i < args.length; i++) {
            jsArgs[i] = toJsArg(args[i]);
        }

        try {
            return (String) json.callMember("stringify", invocable.invokeFunction(jsVarName, jsArgs), null, 1);
        } catch (ScriptException | NoSuchMethodException e) {
            throw Throwables.propagate(e);
        }
    }

}
