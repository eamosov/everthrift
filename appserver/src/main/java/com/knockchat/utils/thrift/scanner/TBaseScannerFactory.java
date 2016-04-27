package com.knockchat.utils.thrift.scanner;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.WordUtils;
import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.meta_data.FieldMetaData;
import org.apache.thrift.meta_data.FieldValueMetaData;
import org.apache.thrift.meta_data.ListMetaData;
import org.apache.thrift.meta_data.MapMetaData;
import org.apache.thrift.meta_data.SetMetaData;
import org.apache.thrift.meta_data.StructMetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.knockchat.appserver.model.lazy.LazyAccessor;
import com.knockchat.appserver.model.lazy.LazyMethod;
import com.knockchat.appserver.model.lazy.Registry;
import com.knockchat.utils.thrift.ThriftUtils;

import it.unimi.dsi.fastutil.ints.Int2ReferenceMap;
import it.unimi.dsi.fastutil.ints.Int2ReferenceOpenHashMap;
import javassist.CannotCompileException;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtMethod;
import javassist.NotFoundException;

public class TBaseScannerFactory {
	
	private static final Logger log = LoggerFactory.getLogger(TBaseScannerFactory.class);
	
	
	private volatile Int2ReferenceMap<TBaseScanner> scanners = new Int2ReferenceOpenHashMap<TBaseScanner>();

	static enum ReturnType{
		VOID,
		OBJECT,
		LIST,
		SET,
		MAP,
		RUNTIME
	}

	static class PropertyInfo{
		String name;
		
		String getterName;
		ReturnType getterType;
		
		String loaderName;
		ReturnType loaderType;
		boolean needPassThis;
		
		String javaType(ReturnType rt){
			switch (rt){
			case OBJECT:
			case RUNTIME:
				return "Object";
			case LIST:
				return "java.util.List";
			case MAP:
				return "java.util.Map";
			default:
				throw new RuntimeException("invalid type:" + getterType);
			}
		}
		
		private String indent(int c, String input){
			StringBuilder sb = new StringBuilder();
			for (int i=0; i<c; i++)
				sb.append("\t");
			
			return input.replaceAll("(?m)^", sb.toString());		
		}
		
		String listApply(final String varName){
			final StringBuilder sb = new StringBuilder();
			sb.append(String.format("if (%s.size() !=0) {\n", varName));			
			sb.append(String.format("\tif (%s instanceof java.util.RandomAccess){\n", varName));
			sb.append(String.format("\t\tfor (int i=0; i < %s.size(); i++){\n", varName));
			sb.append(String.format("\t\t\th.apply(_obj, %s.get(i));\n", varName));
			sb.append("\t\t}\n");
			sb.append("\t}else{\n");
			sb.append(String.format("\t\tfinal java.util.Iterator it = %s.iterator();\n", varName));
			sb.append("\t\twhile (it.hasNext()){\n");
			sb.append("\t\t\th.apply(_obj, it.next());\n");			
			sb.append("\t\t}\n");
			sb.append("\t}\n");
			sb.append("}\n");
			return sb.toString();
		}

		String setApply(final String varName){
			final StringBuilder sb = new StringBuilder();
			sb.append(String.format("if (%s.size() !=0) {\n", varName));
			sb.append(String.format("\tfinal java.util.Iterator it = %s.iterator();\n", varName));			
			sb.append("\twhile (it.hasNext()){\n");
			sb.append("\t\th.apply(_obj, it.next());\n");			
			sb.append("\t}\n");
			sb.append("}\n");
			return sb.toString();
		}

		String mapApply(final String varName){
//			final java.util.Map _{0}=(java.util.Map)obj.get{1}();	
//			{2}
//			if (_{0} != null && _{0}.size() !=0) '{'
//				final java.util.Iterator it = _{0}.entrySet().iterator();
//				while(it.hasNext())'{'
//					{3}
//				'}'
//			'}'
			final StringBuilder sb = new StringBuilder();
			
			sb.append(String.format("if (%s.size() !=0) {\n", varName));
			sb.append(String.format("\tfinal java.util.Iterator it = %s.entrySet().iterator();\n", varName));
			sb.append("\twhile(it.hasNext()){\n");
			sb.append("\t\tObject _i = ((java.util.Map.Entry)it.next()).getValue();\n");
			sb.append(indent(2, apply("_i", ReturnType.OBJECT)) + "\n");
			sb.append("\t}\n");
			sb.append("}\n");
			return sb.toString();
		}
		
		String apply(String varName, ReturnType type){
			if (type == ReturnType.OBJECT){
				return String.format("h.apply(_obj, %s);", varName);
			}else if (type == ReturnType.LIST){
				return listApply(varName);
			}else if (type == ReturnType.SET){
				return setApply(varName);
			}else if (type == ReturnType.MAP){
				return mapApply(varName);
			}else {
				final StringBuilder sb = new StringBuilder();
				sb.append(String.format("if ( %s instanceof java.util.List){\n", varName));
				sb.append(indent(1, listApply("((java.util.List)" + varName + ")")));
				sb.append(String.format("}else if ( %s instanceof java.util.Set){\n", varName));
				sb.append(indent(1, setApply("((java.util.Set)" + varName + ")")));
				sb.append(String.format("}else if ( %s instanceof java.util.Map){\n", varName));
				sb.append(indent(1, mapApply("((java.util.Map)" + varName + ")")));
				sb.append("}else {\n");
				sb.append(String.format("\th.apply(_obj, %s);\n", varName));
				sb.append("}\n");
				return sb.toString();
			}
//				return String.format("h.apply(_obj, %s);", varName);
		}
		
		String code(){
			final StringBuilder sb = new StringBuilder();
			
			int _indent = 0;
			
			final String getterVarName = "_" + name;
			final String loaderVarName = "_" + "l" + "_" + name;
			
			if (getterName !=null){
				
				sb.append(String.format("%s %s = obj.%s();\n", javaType(getterType), getterVarName, getterName));
				sb.append(String.format("if (%s !=null) {\n", getterVarName));
				sb.append(indent(_indent+1, apply(getterVarName, getterType)));
				sb.append("\n}\n");
			}
			
			if (loaderName !=null){
				
				if (getterName !=null){
					sb.append(String.format("if (%s == null) {\n", getterVarName));
					_indent ++;
				}
			
				final StringBuilder _loader = new StringBuilder();
				if (loaderType != ReturnType.VOID){
					_loader.append(String.format("%s %s = ", javaType(loaderType), loaderVarName));
				}				
				_loader.append(String.format("obj.%s(%s);\n", loaderName, needPassThis ? "r, parent" : "r"));
				sb.append(indent(_indent, _loader.toString()));
				
				if (loaderType != ReturnType.VOID){
					sb.append(indent(_indent, String.format("if (%s !=null) {\n", loaderVarName)));
					sb.append(indent(_indent+1, apply(loaderVarName, loaderType)));
					sb.append(indent(_indent, "\n}\n"));					
				}
				
				if (getterName !=null){
					sb.append("}");
					_indent --;
				}
			}
			
			return sb.toString();
		}
	}
		
	public TBaseScannerFactory() {
	}
	
	public TBaseScanner create(Class tModel, String scenario){
				
		int key = 1;
		key = 31 * key + scenario.hashCode();
		key = 31 * key + System.identityHashCode(tModel);
				
		TBaseScanner s = scanners.get(key);
		if (s !=null)
			return s;
		
		return _create(key, tModel, scenario);
	}
	
	private synchronized TBaseScanner _create(final int key, Class tModel, String scenario){
		
		TBaseScanner s = scanners.get(key);
		if (s !=null)
			return s;
		
		final ClassPool pool = ClassPool.getDefault();
		final String scannerClassName = tModel.getPackage().getName() + "." + tModel.getSimpleName() + "Scanner_" + scenario;
		final CtClass cc = pool.makeClass(scannerClassName);
		
		try {
			cc.setSuperclass(pool.get(AbstractTBaseScanner.class.getName()));
			cc.setInterfaces(new CtClass[]{pool.get(TBaseScanner.class.getName())});
			final String code = buildScannerCode("scan", tModel, scenario);
			log.debug("build scan code for {}: {}", tModel.getSimpleName(), code);
			cc.addMethod(CtMethod.make(code, cc));
			cc.addMethod(CtMethod.make("public String getGeneratedCode(){return \"" + code.replaceAll("\"", "\\\\\"").replaceAll("\n", "\\\\n") + "\";}", cc));
						
			s = (TBaseScanner)cc.toClass(tModel.getClassLoader(), null).newInstance();
			
			final Int2ReferenceMap<TBaseScanner> _scanners = new Int2ReferenceOpenHashMap<TBaseScanner>(scanners);
			_scanners.put(key, s);
			scanners = _scanners;
			return s;
		} catch (CannotCompileException | NotFoundException | InstantiationException | IllegalAccessException | IOException e) {
			throw new RuntimeException(e);
		}								
	}
	
	private String indent(int c, String input){
		StringBuilder sb = new StringBuilder();
		for (int i=0; i<c; i++)
			sb.append("\t");
		
		return input.replaceAll("(?m)^", sb.toString());		
	}
	
	private boolean hasScenario(String []value, String scenario){
		
		if (Arrays.equals(value, new String[]{""}))
			return true;
		
		for (String s: value){
			if (s.equals(scenario))
				return true;
		}
		return false;
	}
	
	private Method getMethod(Class cls, String name, Class ... parameterTypes){
		try {
			return cls.getMethod(name, parameterTypes);
		} catch (NoSuchMethodException | SecurityException e) {
			return null;
		}
	}
	

	private String buildScannerCode(String methodName, Class cls, String scenario) throws IOException{
		
		final StringBuilder code = new StringBuilder();
		code.append(String.format("public void %s(Object parent, Object _obj, com.knockchat.utils.thrift.scanner.TBaseScanHandler h, com.knockchat.appserver.model.lazy.Registry r){\n", methodName));
		code.append(String.format("\tfinal %s obj = (%s)_obj;\n\n", cls.getName(), cls.getName()));
				
		final Map<String, PropertyInfo> props = Maps.newHashMap();
								
		if (TBase.class.isAssignableFrom(cls)){
			final Map<? extends TFieldIdEnum, FieldMetaData> md = (Map)ThriftUtils.getRootThriftClass(cls).second;
			
			for (Entry<? extends TFieldIdEnum, FieldMetaData> e:md.entrySet()){
				final FieldValueMetaData v = e.getValue().valueMetaData;
				
				if ( (v instanceof StructMetaData) ||
					(v instanceof ListMetaData && ((ListMetaData) v).elemMetaData instanceof StructMetaData) ||
					(v instanceof SetMetaData && ((SetMetaData) v).elemMetaData instanceof StructMetaData) ||
					((v instanceof MapMetaData) && (((MapMetaData) v).keyMetaData instanceof StructMetaData || ((MapMetaData) v).valueMetaData instanceof StructMetaData))
					){
									
					final PropertyInfo pi = new PropertyInfo();
					pi.name = e.getKey().getFieldName();
					pi.getterName = "get" + WordUtils.capitalize(pi.name);

					if (v instanceof StructMetaData){

						pi.getterType = ReturnType.OBJECT;
						
					}else if (v instanceof ListMetaData && ((ListMetaData) v).elemMetaData instanceof StructMetaData){
						
						pi.getterType = ReturnType.LIST;
						
					}else if (v instanceof SetMetaData && ((SetMetaData) v).elemMetaData instanceof StructMetaData){

						pi.getterType = ReturnType.SET;
						
					}else if (v instanceof MapMetaData && ((MapMetaData) v).valueMetaData instanceof StructMetaData){
						pi.getterType = ReturnType.MAP;
					}
					
					if (pi.getterType !=null)
						props.put(pi.name, pi);
				}
			}			
		}				
		
		for (Method m: cls.getMethods()){
			final LazyAccessor _a = m.getAnnotation(LazyAccessor.class);
			if (_a !=null){
				
				if (m.getParameterTypes().length!=0 || !m.getName().startsWith("get"))
					throw new RuntimeException(String.format("Incompartible method: cls=%s, method=%s, method annotation=%s", cls.getSimpleName(), m.getName(), _a.toString()));
				
				final String fieldName = m.getName().substring(3, 4).toLowerCase() + m.getName().substring(4);
				
				if (hasScenario(_a.value(), scenario)){
					
					PropertyInfo pi = props.get(fieldName);
					if (pi == null){
						pi = new PropertyInfo();
						pi.name = fieldName;
						props.put(pi.name, pi);
					}
					
					pi.getterName = m.getName();
					pi.getterType = ReturnType.RUNTIME;
				}else{
					props.remove(fieldName);
				}
			}
		}

		for (Method m: cls.getMethods()){
			final LazyMethod _a = m.getAnnotation(LazyMethod.class);
			if (_a !=null){
				
				if (!m.getName().startsWith("load"))
					throw new RuntimeException(String.format("Incompartible method: cls=%s, method=%s, method annotation=%s", cls.getSimpleName(), m.getName(), _a.toString()));
				
				final String fieldName = m.getName().substring(4, 5).toLowerCase() + m.getName().substring(5);
				
				if (hasScenario(_a.value(), scenario)){
					
					PropertyInfo pi = props.get(fieldName);
					if (pi == null){
						pi = new PropertyInfo();
						pi.name = fieldName;
						props.put(pi.name, pi);
					}

					pi.loaderName = m.getName();

					if (m.getReturnType().equals(Void.class) || m.getReturnType().equals(Void.TYPE))
						pi.loaderType = ReturnType.VOID;
					else if (pi.getterType !=null)
						pi.loaderType = pi.getterType;
					else
						pi.loaderType = ReturnType.RUNTIME;
					
					if (Arrays.equals(m.getParameterTypes(), new Class[]{Registry.class}))
						pi.needPassThis = false;
					else if (Arrays.equals(m.getParameterTypes(), new Class[]{Registry.class, Object.class}))
						pi.needPassThis = true;
					else
						throw new RuntimeException(String.format("Incompartible method: cls=%s, method=%s, method annotation=%s", cls.getSimpleName(), m.getName(), _a.toString()));

				}
			}
		}

		for (PropertyInfo pi : props.values()){
			code.append(indent(1, pi.code()));
			code.append("\n");
		}
		
		
		code.append("}\n");
		
		return code.toString();
	}	
	
}
