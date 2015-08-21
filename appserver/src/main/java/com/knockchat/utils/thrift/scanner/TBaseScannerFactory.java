package com.knockchat.utils.thrift.scanner;

import it.unimi.dsi.fastutil.ints.Int2ReferenceMap;
import it.unimi.dsi.fastutil.ints.Int2ReferenceOpenHashMap;

import java.io.IOException;
import java.lang.reflect.Method;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import javassist.CannotCompileException;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtMethod;
import javassist.NotFoundException;

import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.meta_data.FieldMetaData;
import org.apache.thrift.meta_data.FieldValueMetaData;
import org.apache.thrift.meta_data.ListMetaData;
import org.apache.thrift.meta_data.MapMetaData;
import org.apache.thrift.meta_data.SetMetaData;
import org.apache.thrift.meta_data.StructMetaData;
import org.apache.velocity.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.license.util.IOUtils;

import com.google.common.collect.Maps;
import com.knockchat.appserver.model.lazy.LazyMethod;
import com.knockchat.appserver.model.lazy.LazyMethods;
import com.knockchat.appserver.model.lazy.Registry;
import com.knockchat.utils.thrift.Utils;

public class TBaseScannerFactory {
	
	private static final Logger log = LoggerFactory.getLogger(TBaseScannerFactory.class);
	
	
	private volatile Int2ReferenceMap<TBaseScanner> scanners = new Int2ReferenceOpenHashMap<TBaseScanner>();
	
	private final String loadTemplate;

	public TBaseScannerFactory() {
		try {
			loadTemplate = IOUtils.readToString(TBaseScannerFactory.class.getClassLoader().getResourceAsStream("load.txt"));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	public TBaseScanner create(Class<? extends TBase> tModel, String scenario){
				
		int key = 1;
		key = 31 * key + scenario.hashCode();
		key = 31 * key + System.identityHashCode(tModel);
				
		TBaseScanner s = scanners.get(key);
		if (s !=null)
			return s;
		
		return _create(key, tModel, scenario);
	}
	
	private synchronized TBaseScanner _create(final int key, Class<? extends TBase> tModel, String scenario){
		
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
		
		//StringUtils.ch
		return input.replaceAll("(?m)^", sb.toString()).replaceAll("^(\t+)", "");		
	}
	
	private boolean hasScenario(LazyMethod a, String scenario){
		
		if (Arrays.equals(a.value(), new String[]{""}))
			return true;
		
		for (String s: a.value()){
			if (s.equals(scenario))
				return true;
		}
		return false;
	}

	public String buildScannerCode(String methodName, Class<? extends TBase> cls, String scenario) throws IOException{
		
		final StringBuilder code = new StringBuilder();
		code.append(String.format("public void %s(org.apache.thrift.TBase _obj, com.knockchat.utils.thrift.scanner.TBaseScanHandler h, com.knockchat.appserver.model.lazy.Registry r){\n", methodName));
		code.append(String.format("\tfinal %s obj = (%s)_obj;\n\n", cls.getName(), cls.getName()));
		
		final Map<? extends TFieldIdEnum, FieldMetaData> md = Utils.getRootThriftClass(cls).second;
		
		final Map<String, TFieldIdEnum> allFields = Maps.newHashMap();
		
		for (Entry<? extends TFieldIdEnum, FieldMetaData> e:md.entrySet()){
			final FieldValueMetaData v = e.getValue().valueMetaData;
			
			if ( (v instanceof StructMetaData) ||
				(v instanceof ListMetaData && ((ListMetaData) v).elemMetaData instanceof StructMetaData) ||
				(v instanceof SetMetaData && ((SetMetaData) v).elemMetaData instanceof StructMetaData) ||
				((v instanceof MapMetaData) && (((MapMetaData) v).keyMetaData instanceof StructMetaData || ((MapMetaData) v).valueMetaData instanceof StructMetaData))
				){
				
				allFields.put(e.getKey().getFieldName(), e.getKey());
			}
		}
		
		final Map<String, LazyMethod> clsAnnotations = Maps.newHashMap();
		final LazyMethod a = cls.getClass().getAnnotation(LazyMethod.class);
		if (a!=null){
			
			if (a.method() == null)
				throw new RuntimeException("@LazyMethod without method name on " + cls.getCanonicalName());
			
			clsAnnotations.put(a.method(), a);
		}
		
		final LazyMethods aa = cls.getClass().getAnnotation(LazyMethods.class);
		if (aa!=null){
			for (LazyMethod _a: aa.value()){
				
				if (_a.method() == null)
					throw new RuntimeException("@LazyMethod without method name on " + cls.getCanonicalName());
				
				clsAnnotations.put(_a.method(), _a);
			}
		}
		
		final Iterator<Map.Entry<String, LazyMethod>> it = clsAnnotations.entrySet().iterator();
		while(it.hasNext()){
			final Map.Entry<String, LazyMethod> e = it.next();
			
			//Check annotation on corresponding method - must be @LazyMethod
			final Method m;
			try {
				m = cls.getMethod(e.getKey(), Registry.class);
			} catch (NoSuchMethodException | SecurityException e1) {
				throw new RuntimeException(String.format("Method '%s' not found in class %s", e.getKey(), cls.getSimpleName()));
			}
			final LazyMethod _a = m.getAnnotation(LazyMethod.class);
			if ( !(_a != null && Arrays.equals(a.value(), new String[]{""}) && a.method().equals("")))
				throw new RuntimeException(String.format("Incompartible annotations: cls=%s,  method=%s, class annotation=%s, method annotation=%s", cls.getSimpleName(), m.getName(), e.getValue().toString(), _a.toString()));

			if (!hasScenario(e.getValue(), scenario))
				it.remove();
		}
		
		for (Method m: cls.getMethods()){
			final LazyMethod _a = m.getAnnotation(LazyMethod.class);
			if (_a !=null){
				
				if (!Arrays.equals(m.getParameterTypes(), new Class[]{Registry.class}))
					throw new RuntimeException(String.format("Incompartible method: cls=%s, method=%s, method annotation=%s", cls.getSimpleName(), m.getName(), _a.toString()));
				
				if (!(_a.method().equals("") || _a.method().equals(m.getName())))
					throw new RuntimeException(String.format("Incompartible annotations: cls=%s, method=%s, method annotation=%s", cls.getSimpleName(), m.getName(), _a.toString()));
				
				if (hasScenario(_a, scenario) && !clsAnnotations.containsKey(m.getName()))
					clsAnnotations.put(m.getName(), _a);
			}
		}				
				
		for (TFieldIdEnum id : allFields.values()){			
			final FieldValueMetaData v = md.get(id).valueMetaData;
			final String fieldName = id.getFieldName();
			
			final String loadMethodName = "load" + StringUtils.capitalizeFirstLetter(fieldName);
			final String load;
			
			if (clsAnnotations.remove(loadMethodName) !=null){
				load = MessageFormat.format(loadTemplate, fieldName, StringUtils.capitalizeFirstLetter(fieldName));				
			}else{
				load = "";
			}

			if (v instanceof StructMetaData){
				
				final String template = IOUtils.readToString(TBaseScannerFactory.class.getClassLoader().getResourceAsStream("struct.txt"));				

				code.append(MessageFormat.format(template, fieldName, StringUtils.capitalizeFirstLetter(fieldName), indent(1,load), ((StructMetaData) v).structClass.getCanonicalName()));										
				
			}else if (v instanceof ListMetaData && ((ListMetaData) v).elemMetaData instanceof StructMetaData){
				
				
				final String template = IOUtils.readToString(TBaseScannerFactory.class.getClassLoader().getResourceAsStream("list.txt"));				
				code.append(MessageFormat.format(template, fieldName, StringUtils.capitalizeFirstLetter(fieldName), indent(1,load)));										
				
			}else if (v instanceof SetMetaData && ((SetMetaData) v).elemMetaData instanceof StructMetaData){

				final String template = IOUtils.readToString(TBaseScannerFactory.class.getClassLoader().getResourceAsStream("set.txt"));				
				code.append(MessageFormat.format(template, fieldName, StringUtils.capitalizeFirstLetter(fieldName), indent(1,load)));										
				
			}else if (v instanceof MapMetaData){
				
				if (((MapMetaData) v).keyMetaData instanceof StructMetaData || ((MapMetaData) v).valueMetaData instanceof StructMetaData){
					
					final String template = IOUtils.readToString(TBaseScannerFactory.class.getClassLoader().getResourceAsStream("map.txt"));
					
					final StringBuilder body = new StringBuilder();
					
					if (((MapMetaData) v).keyMetaData instanceof StructMetaData){
						body.append("h.apply((org.apache.thrift.TBase)(((java.util.Map.Entry)it.next()).getKey()));\n");
					}

					if (((MapMetaData) v).valueMetaData instanceof StructMetaData){
						body.append("h.apply((org.apache.thrift.TBase)(((java.util.Map.Entry)it.next()).getValue()));\n");
					}

					code.append(MessageFormat.format(template, fieldName, StringUtils.capitalizeFirstLetter(fieldName), indent(1,load), indent(3,body.toString())));						
				}				
			}			
		}
		
		for (Map.Entry<String, LazyMethod> e:clsAnnotations.entrySet()){
			code.append("\tobj." + e.getKey() + "(r);\n");
		}
		
		code.append("}\n");
		
		return code.toString();
	}	
	
}
