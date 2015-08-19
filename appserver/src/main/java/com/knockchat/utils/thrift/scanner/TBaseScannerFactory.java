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
	
	public TBaseScanner create(Class<? extends TBase> tModel, String ... scenario){
		
		final Key key = new Key(tModel, scenario);
		
		TBaseScanner s = scanners.get(key);
		if (s !=null)
			return s;
		
		return _create(key, tModel, scenario);
	}
	
	private synchronized TBaseScanner _create(final Key key, Class<? extends TBase> tModel, String ... scenario){
		
		TBaseScanner s = scanners.get(key);
		if (s !=null)
			return s;
		
		final ClassPool pool = ClassPool.getDefault();
		final Class<? extends TBase> rootCls =  Utils.getRootThriftClass(tModel).first;
		final String scannerClassName = rootCls.getPackage().getName() + "." + tModel.getSimpleName() + "Scanner" + Arrays.hashCode(scenario);
		final CtClass cc = pool.makeClass(scannerClassName);
		
		try {
			//cc.setSuperclass(pool.get(cls.getName()));
			cc.setSuperclass(pool.get(AbstractTBaseScanner.class.getName()));
			cc.setInterfaces(new CtClass[]{pool.get(TBaseScanner.class.getName())});
			final String code = buildScannerCode("scan", rootCls, scenario);
			log.debug("build scan code for {}: {}", rootCls.getSimpleName(), code);
			cc.addMethod(CtMethod.make(code, cc));
			cc.addMethod(CtMethod.make("public String getGeneratedCode(){return \"" + code.replaceAll("\"", "\\\\\"").replaceAll("\n", "\\\\n") + "\";}", cc));
						
			s = (TBaseScanner)cc.toClass(rootCls.getClassLoader(), null).newInstance();
			
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
				
				final String type = ((StructMetaData) v).structClass.getCanonicalName();
				
				code.append(String.format("\t%s _%s=obj.get%s();\n", type, fieldName, StringUtils.capitalizeFirstLetter(fieldName)));
				code.append(String.format("\t\n\tif (_%s == null){\n\t\ttry{\n\t\t\tobj.set%s((_%s = obj.load%s()));\n\t\t}catch (org.apache.thrift.LoadException e){\n\t\t}\n\t}\n\t\n\tif (_%s !=null) {\n\t\th.apply((org.apache.thrift.TBase)_%s);\n\t}\n",
						fieldName, StringUtils.capitalizeFirstLetter(fieldName), fieldName, StringUtils.capitalizeFirstLetter(fieldName), fieldName, fieldName));
				
			}else if (v instanceof ListMetaData && ((ListMetaData) v).elemMetaData instanceof StructMetaData){
				
				code.append(String.format("\t\n\tjava.util.List _%s=(java.util.List)obj.get%s();\n\t\n\tif (_%s == null){\n\t\ttry{\n\t\t\tobj.set%s((_%s = obj.load%s()));\n\t\t}catch(org.apache.thrift.LoadException e){\n\t\t}\n\t}\n\t\n\tif (_%s !=null && _%s.size() !=0) {\n\t\tif (_%s instanceof java.util.RandomAccess){\n\t\t\tfinal java.util.List _l = (java.util.List)_%s;\n\t\t\tfor (int i=0; i < _l.size(); i++){\n\t\t\t\th.apply((org.apache.thrift.TBase)_l.get(i));\n\t\t\t}\n\t\t}else{\n\t\t\tfinal java.util.Iterator it = _%s.iterator();\n\t\t\twhile (it.hasNext()) {\n\t\t\t\th.apply((org.apache.thrift.TBase)it.next());\n\t\t\t}\t\t\n\t\t}\n\t}\n",
						fieldName, StringUtils.capitalizeFirstLetter(fieldName), fieldName, StringUtils.capitalizeFirstLetter(fieldName), fieldName, StringUtils.capitalizeFirstLetter(fieldName), fieldName, fieldName, fieldName, fieldName, fieldName));
				
			}else if (v instanceof SetMetaData && ((SetMetaData) v).elemMetaData instanceof StructMetaData){
				
				code.append(String.format("\t\n\tjava.util.Set _%s=(java.util.Set)obj.get%s();\n\t\n\tif (_%s == null){\n\t\ttry{\n\t\t\tobj.set%s((_%s = obj.load%s()));\n\t\t}catch(org.apache.thrift.LoadException e){\n\t\t}\n\t}\n\t\n\tif (_%s !=null && _%s.size() !=0) {\n\t\tfinal java.util.Iterator it = _%s.iterator();\n\t\twhile (it.hasNext()) {\n\t\t\th.apply((org.apache.thrift.TBase)it.next());\n\t\t}\t\t\n\t}\n",
						fieldName, StringUtils.capitalizeFirstLetter(fieldName), fieldName, StringUtils.capitalizeFirstLetter(fieldName), fieldName, StringUtils.capitalizeFirstLetter(fieldName), fieldName, fieldName, fieldName));
				
			}else if (v instanceof MapMetaData){
				
				if (((MapMetaData) v).keyMetaData instanceof StructMetaData || ((MapMetaData) v).valueMetaData instanceof StructMetaData){
				
					code.append(String.format("\tfinal java.util.Map _%s=(java.util.Map)obj.get%s();\n", fieldName, StringUtils.capitalizeFirstLetter(fieldName)));
					code.append(String.format("\t\n\tif (_%s == null){\n\t\ttry{\n\t\t\tobj.set%s((_%s = obj.load%s()));\n\t\t}catch(org.apache.thrift.LoadException e){\n\t\t}\n\t}\n",
							fieldName, StringUtils.capitalizeFirstLetter(fieldName), fieldName, StringUtils.capitalizeFirstLetter(fieldName)));
					code.append(String.format("\tif ( _%s != null && _%s.size() !=0) {\n", fieldName, fieldName));
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
