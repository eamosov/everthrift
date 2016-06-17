package org.everthrift.appserver.utils.thrift;

import java.net.URLEncoder;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

import org.apache.thrift.TBase;
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

import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Shorts;

public class ThriftFormatter {
	private final String basePath;

	public ThriftFormatter(String basePath) {
		super();
		this.basePath = basePath;
	}
	
	private final List<TFieldIdEnum> sortedKeys(Map<? extends TFieldIdEnum, FieldMetaData> map){
		return map.keySet().stream().sorted(new Comparator<TFieldIdEnum>(){

			@Override
			public int compare(TFieldIdEnum o1, TFieldIdEnum o2) {
				return Shorts.compare(o1.getThriftFieldId(), o2.getThriftFieldId());
			}}).collect(Collectors.toList());
	}
	
	public String formatServices(Collection<ThriftControllerInfo> cInfo){
		
		Multimap<String, ThriftControllerInfo> mm = Multimaps.index(cInfo, ThriftControllerInfo::getServiceName);
		
		final StringBuilder sb = new StringBuilder();
		for (Map.Entry<String, Collection<ThriftControllerInfo>> e: mm.asMap().entrySet()){
			sb.append(formatService(e.getKey(), e.getValue()));
		}
		return sb.toString();
	}
	
	public String formatService(String serviceName, Collection<ThriftControllerInfo> cInfos){
		final StringBuilder sb = new StringBuilder();
		
		sb.append("<div class=\"service\">\n");
		sb.append(String.format("<span>%s{</span>\n", serviceName));		
		sb.append("<ol>\n");
		for (ThriftControllerInfo i: cInfos){
			sb.append(String.format("\t<li style=\"list-style-type: none;\">%s</li>\n", formatMethod(i)));
		}
		sb.append("</ol>\n");
		sb.append("<span>}</span>\n");
		sb.append("<div/>\n");
		return sb.toString();
	}

	public String formatMethod(ThriftControllerInfo cInfo){
		
		
		final Pair<Class<? extends TBase>, Map<? extends TFieldIdEnum, FieldMetaData>> resultMap =  ThriftUtils.getRootThriftClass(cInfo.getResultCls());
		
		FieldValueMetaData result;
		try{
			result = resultMap.second.entrySet().stream().filter( e -> (e.getKey().getFieldName().equals("success"))).findFirst().get().getValue().valueMetaData;
		}catch (NoSuchElementException e){
			result = null;
		}
		
		final StringBuilder sb = new StringBuilder();
		sb.append("<span class=\"method\">" + formatTypeName(result) + " " + cInfo.getMethodName() + "(");

		final Pair<Class<? extends TBase>, Map<? extends TFieldIdEnum, FieldMetaData>> argsMap =  ThriftUtils.getRootThriftClass(cInfo.getArgCls());
		final List<TFieldIdEnum> argsFields = sortedKeys(argsMap.second);

		boolean coma = false;
		for (TFieldIdEnum f: argsFields){
			FieldMetaData fmd = argsMap.second.get(f);
			if (coma)
				sb.append(", ");
			sb.append(String.format("%d:%s %s %s", f.getThriftFieldId(), formatTypeName(fmd.valueMetaData), formatRequirementType(fmd.requirementType), fmd.fieldName));
			coma = true;
		}
		sb.append(")");
		
		final List<TFieldIdEnum> resultFields = sortedKeys(resultMap.second);
		
		if (resultFields.size()>1){
			sb.append(" throws(");
			coma = false;
			for (TFieldIdEnum f: resultFields){
				FieldMetaData fmd = resultMap.second.get(f);
				if (!f.getFieldName().equals("success")){
					if (coma)
						sb.append(", ");
					sb.append(String.format("%d:%s %s %s", f.getThriftFieldId(), formatTypeName(fmd.valueMetaData), formatRequirementType(fmd.requirementType), fmd.fieldName));
					coma = true;
				}
				
			}			
			sb.append(")");
		}		
		
		sb.append("</span>\n");

		return sb.toString();
	}
	
	public String formatClass(Class<? extends TBase> cls) throws ClassNotFoundException{
		
		if (!TBase.class.isAssignableFrom(cls))
			throw new ClassNotFoundException(cls.getCanonicalName());

		final Pair<Class<? extends TBase>, Map<? extends TFieldIdEnum, FieldMetaData>> _cls = ThriftUtils.getRootThriftClass(cls);
		
		if (_cls == null)
			throw new ClassNotFoundException(cls.getCanonicalName());
		
		final List<TFieldIdEnum> fields = sortedKeys(_cls.second);
		
		final StringBuilder sb = new StringBuilder();
		
		final String type = Exception.class.isAssignableFrom(cls) ? "exception" : "struct";
		
		sb.append("<div class=\"" + type + "\"><span>" + type + " " + linkClass(_cls.first) + "{</span>\n");
		
		sb.append("<ol>\n");
		for (TFieldIdEnum f: fields){
			FieldMetaData fmd = _cls.second.get(f);
			sb.append(String.format("\t<li style=\"list-style-type: none;\">%d:%s %s %s</li>\n", f.getThriftFieldId(), formatTypeName(fmd.valueMetaData), formatRequirementType(fmd.requirementType), fmd.fieldName));
		}
		
		sb.append("</ol>\n");
		
		sb.append("<span>}</span></div>\n");
		return sb.toString();
	}
	
	public String formatTEnum(Class<? extends TEnum> cls) throws ClassNotFoundException{
		
		if (!TEnum.class.isAssignableFrom(cls))
			throw new ClassNotFoundException(cls.getCanonicalName());
		
		final List<TEnum> values = Lists.newArrayList(cls.getEnumConstants());
		Collections.sort(values, (o1, o2) -> (Ints.compare(o1.getValue(), o2.getValue())));
		
		final StringBuilder sb = new StringBuilder();
		sb.append("<div class=\"enum\">\n");
		sb.append("<span>enum " + cls.getSimpleName() + "{</span>\n");
		sb.append("<ol>\n");
		for(TEnum e: values){
			sb.append(String.format("<li style=\"list-style-type: none;\">%s = %d</li>\n", ((Enum)e).name(), e.getValue()));
		}
		sb.append("</ol>\n");
		sb.append("<span>}</span></div>\n");
		return sb.toString();
	}
	
	private String linkClass(Class cls){
		return String.format("<a href=\"%s\">%s</a>", basePath + "struct/" + URLEncoder.encode(cls.getCanonicalName()) + "/", cls.getSimpleName());
	}

	private String linkEnum(Class cls){
		return String.format("<a href=\"%s\">%s</a>", basePath + "enum/" +  URLEncoder.encode(cls.getCanonicalName())+ "/", cls.getSimpleName());
	}

	public static String formatRequirementType(int requirementType){
		switch (requirementType){
			case TFieldRequirementType.DEFAULT:
				return "";
			case TFieldRequirementType.OPTIONAL:
				return "optional";
			case TFieldRequirementType.REQUIRED:
				return "required";
		}
		return "";
	}
	
	public String formatTypeName(FieldValueMetaData vmd){
		
		if (vmd == null)
			return "void";
		
		if (vmd instanceof EnumMetaData){
			return linkEnum(((EnumMetaData)vmd).enumClass);
		}
		
		if (vmd instanceof ListMetaData){
			return "list&lt;" + formatTypeName(((ListMetaData)vmd).elemMetaData) + "&gt;";
		}
		
		if (vmd instanceof MapMetaData){
			return "map&lt;" + formatTypeName(((MapMetaData)vmd).keyMetaData) + "," + formatTypeName(((MapMetaData)vmd).valueMetaData)+ "&gt;";
		}

		if (vmd instanceof SetMetaData){
			return "list&lt;" + formatTypeName(((SetMetaData)vmd).elemMetaData) + "&gt;";
		}
		
		if (vmd instanceof StructMetaData){
			return linkClass(((StructMetaData)vmd).structClass);
		}
		
		switch(vmd.type){
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
