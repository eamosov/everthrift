package org.everthrift.appserver.utils.thrift;

import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Shorts;
import org.apache.thrift.TBase;
import org.apache.thrift.TDoc;
import org.apache.thrift.TEnum;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.TFieldRequirementType;
import org.apache.thrift.meta_data.EnumMetaData;
import org.apache.thrift.meta_data.FieldMetaData;
import org.apache.thrift.meta_data.FieldValueMetaData;
import org.apache.thrift.meta_data.ListMetaData;
import org.apache.thrift.meta_data.MapMetaData;
import org.apache.thrift.meta_data.SetMetaData;
import org.apache.thrift.meta_data.StructMetaData;
import org.apache.thrift.protocol.TType;
import org.everthrift.appserver.controller.ThriftControllerInfo;
import org.everthrift.utils.Pair;
import org.springframework.beans.BeanUtils;

import java.lang.reflect.Method;
import java.net.URLEncoder;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class ThriftFormatter {
    private final String basePath;

    public ThriftFormatter(String basePath) {
        super();
        this.basePath = basePath;
    }

    private final List<TFieldIdEnum> sortedKeys(Map<? extends TFieldIdEnum, FieldMetaData> map) {
        return map.keySet().stream().sorted(new Comparator<TFieldIdEnum>() {

            @Override
            public int compare(TFieldIdEnum o1, TFieldIdEnum o2) {
                return Shorts.compare(o1.getThriftFieldId(), o2.getThriftFieldId());
            }
        }).collect(Collectors.toList());
    }

    public String formatServices(Collection<ThriftControllerInfo> cInfo) {

        Multimap<String, ThriftControllerInfo> mm = Multimaps.index(cInfo, ThriftControllerInfo::getServiceName);

        final StringBuilder sb = new StringBuilder();
        sb.append(head());

        for (Map.Entry<String, Collection<ThriftControllerInfo>> e : mm.asMap().entrySet()) {
            sb.append(formatService(e.getKey(), e.getValue()));
        }
        return sb.toString();
    }

    public String formatService(String serviceName, Collection<ThriftControllerInfo> cInfos) {
        final StringBuilder sb = new StringBuilder();

        sb.append("<div class=\"service\" style=\"padding-bottom:10px;\">\n");
        sb.append(String.format("<span><span class=\"pl-k\">service</span> <span class=\"pl-en\">%s</span> {</span>\n", serviceName));
        sb.append("<div class=\"methods\" style=\"padding: 5px 0px 0px 20px;\">\n");
        for (ThriftControllerInfo i : cInfos) {
            sb.append("<div class=\"method\" style=\"padding-bottom: 5px;\">\n");
            sb.append("<div class=\"pl-c\">" + getTDocMethodComment(i.getArgCls()) + "</div>\n");
            sb.append(formatMethod(i.getMethodName(), i.getArgCls(), i.getResultCls()));
            sb.append("</div>");
        }
        sb.append("</div>\n");
        sb.append("<span>}</span>\n");
        sb.append("</div>\n");
        return sb.toString();
    }

    public String getTDocMethodComment(Class methodArgCls) {
        final Pattern pattern = Pattern.compile("(^.+)\\.([^\\.]+)_args$");
        final Matcher matcher = pattern.matcher(methodArgCls.getCanonicalName());

        if (matcher.matches()) {
            final Class iface;
            try {
                iface = Class.forName(matcher.group(1) + "$Iface");
            } catch (ClassNotFoundException e) {
                return "";
            }
            final String methodName = matcher.group(2);
            for (Method m : iface.getMethods()) {
                if (m.getName().equals(methodName)) {
                    return Optional.ofNullable(m.getAnnotation(TDoc.class)).map(TDoc::value).orElse("");
                }
            }
        }
        return "";
    }

    public String formatMethod(String name, Class<? extends TBase> argsCls, Class<? extends TBase> resultCls) {
        final Pair<Class<? extends TBase>, Map<? extends TFieldIdEnum, FieldMetaData>> resultMap = ThriftUtils.getRootThriftClass(resultCls);

        FieldValueMetaData result;
        try {
            result = resultMap.second.entrySet()
                                     .stream()
                                     .filter(e -> (e.getKey().getFieldName().equals("success")))
                                     .findFirst()
                                     .get()
                                     .getValue().valueMetaData;
        } catch (NoSuchElementException e) {
            result = null;
        }

        final StringBuilder sb = new StringBuilder();
        sb.append("<span class=\"pl-k\">" + formatTypeName(result, false) + "</span> " + "<span class=\"pl-en\">" + name + "</span>" + "(");

        final Pair<Class<? extends TBase>, Map<? extends TFieldIdEnum, FieldMetaData>> argsMap = ThriftUtils.getRootThriftClass(argsCls);
        final List<TFieldIdEnum> argsFields = sortedKeys(argsMap.second);

        boolean coma = false;
        for (TFieldIdEnum f : argsFields) {
            FieldMetaData fmd = argsMap.second.get(f);
            if (coma) {
                sb.append(", ");
            }
            sb.append(String.format("<span class=\"pl-e\">%d:</span><span class=\"pl-k\">%s %s</span> <span class=\"pl-smi\">%s</span>",
                                    f.getThriftFieldId(), formatTypeName(fmd.valueMetaData, false),
                                    formatRequirementType(fmd.requirementType), fmd.fieldName));
            coma = true;
        }
        sb.append(")");

        final List<TFieldIdEnum> resultFields = sortedKeys(resultMap.second);

        if (resultFields.size() > 1) {
            sb.append(" <span class=\"pl-k\">throws</span>(");
            coma = false;
            for (TFieldIdEnum f : resultFields) {
                FieldMetaData fmd = resultMap.second.get(f);
                if (!f.getFieldName().equals("success")) {
                    if (coma) {
                        sb.append(", ");
                    }
                    sb.append(String.format("<span class=\"pl-e\">%d:</span><span class=\"pl-k\">%s %s</span> <span class=\"pl-smi\">%s</span>",
                                            f.getThriftFieldId(), formatTypeName(fmd.valueMetaData, false),
                                            formatRequirementType(fmd.requirementType), fmd.fieldName));
                    coma = true;
                }

            }
            sb.append(")");
        }

        sb.append("\n");
        return sb.toString();
    }

    public String head() {
        return "<head>\n" +
            "  <meta charset=\"utf-8\">\n" +
            style() +
            " </head>";
    }

    public String style() {
        final StringBuilder sb = new StringBuilder();
        sb.append("<style>\n");
        sb.append(".pl-e, .pl-en {color: #6f42c1;}\n");
        sb.append(".pl-k, .pl-k a {color: #d73a49;}\n");
        sb.append(".pl-c {color: #6a737d;}\n");
        sb.append(".pl-smi, .pl-s .pl-s1 {color: #24292e;}\n");
        sb.append(".pl-c1, .pl-s .pl-v {color: #005cc5;}\n");
        sb.append("</style>\n");
        return sb.toString();
    }

    public String formatClass(Class<? extends TBase> cls) throws ClassNotFoundException {

        if (!TBase.class.isAssignableFrom(cls)) {
            throw new ClassNotFoundException(cls.getCanonicalName());
        }

        final Pair<Class<? extends TBase>, Map<? extends TFieldIdEnum, FieldMetaData>> _cls = ThriftUtils.getRootThriftClass(cls);

        if (_cls == null) {
            throw new ClassNotFoundException(cls.getCanonicalName());
        }

        final List<TFieldIdEnum> fields = sortedKeys(_cls.second);

        final StringBuilder sb = new StringBuilder();

        sb.append(head());

        final String type = Exception.class.isAssignableFrom(cls) ? "exception" : "struct";

        sb.append("<div class=\"" + type + "\">\n");
        sb.append(String.format("<div class=\"pl-c\">%s</div>\n", Optional.ofNullable(cls.getAnnotation(TDoc.class))
                                                                          .map(TDoc::value)
                                                                          .orElse("")));
        sb.append("<br/>");
        sb.append("<span><span class=\"pl-k\">" + type + "</span> " + linkClass(_cls.first, true) + " {</span>\n");

        sb.append("<table class=\"fields\" style=\"padding-left: 20px;\">\n");

        for (TFieldIdEnum f : fields) {
            FieldMetaData fmd = _cls.second.get(f);
            sb.append("<tr>\n");
            sb.append(String.format("<td><span class=\"pl-e\">%d:</span></td>\n", f.getThriftFieldId()));
            sb.append(String.format("<td><span class=\"pl-k\">%s %s</span></td>\n", formatTypeName(fmd.valueMetaData, true), formatRequirementType(fmd.requirementType)));
            sb.append(String.format("<td><span class=\"pl-smi\">%s</span></td>\n", fmd.fieldName));
            sb.append(String.format("<td><span class=\"pl-c\">%s</span></td>\n", Optional.ofNullable(BeanUtils.getPropertyDescriptor(_cls.first, fmd.fieldName)
                                                                                                              .getReadMethod()
                                                                                                              .getAnnotation(TDoc.class))
                                                                                         .map(TDoc::value)
                                                                                         .orElse("")));
            sb.append("</tr>\n");
        }

        sb.append("</table>\n");

        sb.append("<span>}</span></div>\n");
        return sb.toString();
    }

    public String formatTEnum(Class<? extends TEnum> cls) throws ClassNotFoundException {

        if (!TEnum.class.isAssignableFrom(cls)) {
            throw new ClassNotFoundException(cls.getCanonicalName());
        }

        final List<TEnum> values = Lists.newArrayList(cls.getEnumConstants());
        Collections.sort(values, (o1, o2) -> (Ints.compare(o1.getValue(), o2.getValue())));

        final StringBuilder sb = new StringBuilder();

        sb.append(head());

        sb.append("<div class=\"enum\">\n");
        sb.append(String.format("<div class=\"pl-c\">%s</div>\n", Optional.ofNullable(cls.getAnnotation(TDoc.class))
                                                                          .map(TDoc::value)
                                                                          .orElse("")));
        sb.append("<br/>");

        sb.append("<span><span class=\"pl-k\">enum</span> " + linkEnum(cls, true) + " {</span>\n");

        sb.append("<table class=\"fields\" style=\"padding-left: 20px;\">\n");

        for (TEnum e : values) {
            sb.append("<tr>\n");
            sb.append(String.format("<td><span class=\"pl-smi\">%s</span></td>\n", ((Enum) e).name()));
            sb.append(String.format("<td><span class=\"pl-c1\"> = %d</span></td>\n", e.getValue()));
            try {
                sb.append(String.format("<td style=\"padding-left: 10px;\"><span class=\"pl-c\">%s</span></td>\n", Optional
                    .ofNullable(cls.getField(((Enum) e).name()).getAnnotation(TDoc.class))
                    .map(TDoc::value)
                    .orElse("")));
            } catch (NoSuchFieldException e1) {
            }
            sb.append("</tr>\n");
        }

        sb.append("</table>\n");
        sb.append("<span>}</span></div>\n");
        return sb.toString();
    }

    private String linkClass(Class cls, boolean span) {
        final String format;
        if (span) {
            format = "<a href=\"%s\"><span class=\"pl-en\">%s</span></a>";
        } else {
            format = "<a href=\"%s\">%s</a>";
        }
        return String.format(format, basePath + "struct/" + URLEncoder.encode(cls.getCanonicalName()) + "/", cls.getSimpleName());
    }

    private String linkEnum(Class cls, boolean span) {
        final String format;
        if (span) {
            format = "<a href=\"%s\"><span class=\"pl-en\">%s</span></a>";
        } else {
            format = "<a href=\"%s\">%s</a>";
        }
        return String.format(format, basePath + "enum/" + URLEncoder.encode(cls.getCanonicalName()) + "/", cls.getSimpleName());
    }

    public static String formatRequirementType(int requirementType) {
        switch (requirementType) {
            case TFieldRequirementType.DEFAULT:
                return "";
            case TFieldRequirementType.OPTIONAL:
                return "optional";
            case TFieldRequirementType.REQUIRED:
                return "required";
        }
        return "";
    }

    public String formatTypeName(FieldValueMetaData vmd, boolean span) {

        if (vmd == null) {
            return "void";
        }

        if (vmd instanceof EnumMetaData) {
            return linkEnum(((EnumMetaData) vmd).enumClass, span);
        }

        if (vmd instanceof ListMetaData) {
            return "list&lt;" + formatTypeName(((ListMetaData) vmd).elemMetaData, span) + "&gt;";
        }

        if (vmd instanceof MapMetaData) {
            return "map&lt;" + formatTypeName(((MapMetaData) vmd).keyMetaData, span) + "," + formatTypeName(((MapMetaData) vmd).valueMetaData, span)
                + "&gt;";
        }

        if (vmd instanceof SetMetaData) {
            return "list&lt;" + formatTypeName(((SetMetaData) vmd).elemMetaData, span) + "&gt;";
        }

        if (vmd instanceof StructMetaData) {
            return linkClass(((StructMetaData) vmd).structClass, span);
        }

        switch (vmd.type) {
            case TType.VOID:
                return "void";
            case TType.BOOL:
                return "bool";
            case TType.BYTE:
                return "byte";
            case TType.DOUBLE:
                return "double";
            case TType.I16:
                return "i16";
            case TType.I32:
                return "i32";
            case TType.I64:
                return "i64";
            case TType.STRING:
                return "string";
        }

        return "unknown";
    }

}
