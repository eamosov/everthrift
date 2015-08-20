package com.knockchat.utils.thrift.scanner;

import it.unimi.dsi.fastutil.ints.Int2ReferenceMap;
import it.unimi.dsi.fastutil.ints.Int2ReferenceOpenHashMap;

import java.io.IOException;
import java.lang.reflect.Method;
import java.text.MessageFormat;
import java.util.Arrays;
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
	
	public TBaseScanner create(Class<? extends TBase> tModel, String ... scenario){
				
		int key = 1;
		key = 31 * key + Arrays.hashCode(scenario);
		key = 31 * key + System.identityHashCode(tModel);
				
		TBaseScanner s = scanners.get(key);
		if (s !=null)
			return s;
		
		return _create(key, tModel, scenario);
	}
	
	private synchronized TBaseScanner _create(final int key, Class<? extends TBase> tModel, String ... scenario){
		
		TBaseScanner s = scanners.get(key);
		if (s !=null)
			return s;
		
		final ClassPool pool = ClassPool.getDefault();
		final String scannerClassName = tModel.getPackage().getName() + "." + tModel.getSimpleName() + "Scanner" + Arrays.hashCode(scenario);
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

	public String buildScannerCode(String methodName, Class<? extends TBase> cls, String[] scenario) throws IOException{
		
		final StringBuilder code = new StringBuilder();
		code.append(String.format("public void %s(org.apache.thrift.TBase _obj, com.knockchat.utils.thrift.scanner.TBaseScanHandler h){\n", methodName));
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
				
		final Map<String, TFieldIdEnum> filteredFields = Maps.newHashMap();
		for (String s: scenario){
			if (s.equals("*"))
				filteredFields.putAll(allFields);
			else if (s.charAt(0) == '!'){
				filteredFields.remove(s.substring(1));
			}else{
				final TFieldIdEnum _id = allFields.get(s);
				if (_id !=null){
					filteredFields.put(s, _id);
				}else{
					log.error("Coud't find field {} in {}", s, cls.getSimpleName());
				}
			}
		}
				
		for (TFieldIdEnum id : filteredFields.values()){			
			final FieldValueMetaData v = md.get(id).valueMetaData;
			final String fieldName = id.getFieldName();
			
			String load;
			try {
				final Method loadMethod = cls.getMethod("load" + StringUtils.capitalizeFirstLetter(fieldName));
				load = MessageFormat.format(loadTemplate, fieldName, StringUtils.capitalizeFirstLetter(fieldName));
			} catch (NoSuchMethodException | SecurityException e1) {
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
		
		code.append("}\n");
		
		return code.toString();
	}	
	
}
