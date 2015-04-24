package com.knockchat.utils.thrift.scanner;

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

import com.google.common.collect.Maps;
import com.knockchat.utils.thrift.Utils;

public class TBaseScannerFactory {
	
	private static final Logger log = LoggerFactory.getLogger(TBaseScannerFactory.class);
	
	private static class Key{
		final Class<? extends TBase> cls;
		final String scenario[];
		
		public Key(Class<? extends TBase> cls, String[] scenario) {
			super();
			this.cls = cls;
			this.scenario = scenario;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((cls == null) ? 0 : cls.hashCode());
			result = prime * result + Arrays.hashCode(scenario);
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			Key other = (Key) obj;
			if (cls == null) {
				if (other.cls != null)
					return false;
			} else if (!cls.equals(other.cls))
				return false;
			if (!Arrays.equals(scenario, other.scenario))
				return false;
			return true;
		}
		
	}
	
	private volatile Map<Key, TBaseScanner> scanners = Maps.newHashMap();

	public TBaseScannerFactory() {
		
	}
	
	public TBaseScanner create(Class<? extends TBase> cls, String ... scenario){
		
		final Key key = new Key(cls, scenario);
		
		TBaseScanner s = scanners.get(key);
		if (s !=null)
			return s;
		
		return _create(key, cls, scenario);
	}
	
	private synchronized TBaseScanner _create(final Key key, Class<? extends TBase> cls, String ... scenario){
		
		TBaseScanner s = scanners.get(key);
		if (s !=null)
			return s;

		final ClassPool pool = ClassPool.getDefault();
		final CtClass cc = pool.makeClass("com.knockchat.utils.thrift.generated." + cls.getSimpleName() + "Scanner" + Arrays.hashCode(scenario));		
		try {
			cc.setSuperclass(pool.get(AbstractTBaseScanner.class.getName()));
			final String code = buildScannerCode("scan", cls, scenario);
			log.debug("build scan code for {}: {}", cls.getSimpleName(), code);
			cc.addMethod(CtMethod.make(code, cc));
			s = (TBaseScanner)cc.toClass().newInstance();
			
			final Map<Key, TBaseScanner> _scanners = Maps.newHashMap(scanners);
			_scanners.put(key, s);
			scanners = _scanners;
			return s;
		} catch (CannotCompileException | NotFoundException | InstantiationException | IllegalAccessException e) {
			throw new RuntimeException(e);
		}						
		
	}

	public static String buildScannerCode(String methodName, Class<? extends TBase> cls, String[] scenario){
		
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
			
			if (v instanceof StructMetaData){
				
				code.append(String.format("\tfinal Object _%s=obj.get%s();\n", fieldName, StringUtils.capitalizeFirstLetter(fieldName)));
				code.append(String.format("\tif (_%s !=null) { h.apply((org.apache.thrift.TBase)_%s);}\n\n", fieldName, fieldName));
				
			}else if ( (v instanceof ListMetaData && ((ListMetaData) v).elemMetaData instanceof StructMetaData) ||
					   (v instanceof SetMetaData && ((SetMetaData) v).elemMetaData instanceof StructMetaData)){
				
				code.append(String.format("\tfinal java.util.Collection _%s=(java.util.Collection)obj.get%s();\n", fieldName, StringUtils.capitalizeFirstLetter(fieldName)));
				code.append(String.format("\tif (_%s !=null) { final java.util.Iterator it = _%s.iterator(); while (it.hasNext()) {h.apply((org.apache.thrift.TBase)it.next());}}\n", fieldName, fieldName));
			}else if (v instanceof MapMetaData){
				
				if (((MapMetaData) v).keyMetaData instanceof StructMetaData || ((MapMetaData) v).valueMetaData instanceof StructMetaData){
				
					code.append(String.format("\tfinal java.util.Map _%s=(java.util.Map)obj.get%s();\n", fieldName, StringUtils.capitalizeFirstLetter(fieldName)));
					code.append(String.format("\tif ( _%s != null) {\n", fieldName));
					code.append(String.format("\t\tfinal java.util.Iterator it = _%s.entrySet().iterator();\n", fieldName));
					code.append(String.format("\t\twhile(it.hasNext()){\n"));
					
					if (((MapMetaData) v).keyMetaData instanceof StructMetaData){
						code.append("\t\t\th.apply((org.apache.thrift.TBase)(((java.util.Map.Entry)it.next()).getKey()));\n");
					}

					if (((MapMetaData) v).valueMetaData instanceof StructMetaData){
						code.append("\t\t\th.apply((org.apache.thrift.TBase)(((java.util.Map.Entry)it.next()).getValue()));\n");
					}
					
					code.append("\t\t}\n");
					code.append("\t}\n");					
				}				
			}			
		}		
		
		code.append("}\n");
		
		return code.toString();
	}	
	
}
