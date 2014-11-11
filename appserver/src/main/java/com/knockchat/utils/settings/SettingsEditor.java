package com.knockchat.utils.settings;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.knockchat.utils.EscapeChars;


/**
 * 
 * @author efreet (Amosov Evgeniy)
 *
 */
public class SettingsEditor {
	
	private static final Logger log = LoggerFactory.getLogger(SettingsEditor.class);

	Object o;
	@SuppressWarnings("unchecked")
	Class cls;
	String name;

	// HashMap<String, Class> h = new HashMap<String, Class>();

	SettingsEditor(Object _o, String aName) {

		/*
		 * h.put("Byte", Byte.class); h.put("Double", Double.class);
		 * h.put("Float", Float.class); h.put("Integer", Integer.class);
		 * h.put("Long", Long.class); h.put("Short", Short.class);
		 */

		o = _o;
		cls = o.getClass();
		name = aName;
	}
	
	String getName(){
		return name;
	}
	
	class UnsupportedTypeException extends Exception{
		
	}
	
	private Object toObject(Class cls, String val) throws UnsupportedTypeException{
		if (cls.equals(Byte.class)  || cls.equals(byte.class)) {
			return Byte.parseByte(val);
		} else if (cls.equals(Double.class)  || cls.equals(double.class) ) {
			return Double.parseDouble(val);
		} else if (cls.equals(Float.class) || cls.equals(float.class)) {
			return Float.parseFloat(val);
		} else if (cls.equals(Integer.class) || cls.equals(int.class)) {
			return Integer.parseInt(val);
		} else if (cls.equals(Long.class) || cls.equals(long.class)) {
			return Long.parseLong(val);
		} else if (cls.equals(Short.class) || cls.equals(short.class)) {
			return Short.parseShort(val);
		} else if (cls.equals(Boolean.class) || cls.equals(boolean.class)){
			return Boolean.parseBoolean(val);
		} else if (cls.equals(String.class)) {
			return val;
		} else {
			throw new UnsupportedTypeException();
		}		
	}

	@SuppressWarnings("unchecked")
	void parseVal(Class varCls, Field f, String val)
			throws NumberFormatException, IllegalArgumentException,
			IllegalAccessException {
		if (varCls.equals(Byte.class) || varCls.equals(byte.class)) {
			f.setByte(o, Byte.parseByte(val));
		} else if (varCls.equals(Double.class) || varCls.equals(double.class)) {
			f.setDouble(o, Double.parseDouble(val));
		} else if (varCls.equals(Float.class) || varCls.equals(float.class)) {
			f.setFloat(o, Float.parseFloat(val));
		} else if (varCls.equals(Integer.class) || varCls.equals(int.class)) {
			f.setInt(o, Integer.parseInt(val));
		} else if (varCls.equals(Long.class) || varCls.equals(long.class)) {
			f.setLong(o, Long.parseLong(val));
		} else if (varCls.equals(Short.class) || varCls.equals(short.class)) {
			f.setShort(o, Short.parseShort(val));
		} else if (varCls.equals(Boolean.class) || varCls.equals(boolean.class)){
			f.setBoolean(o, Boolean.parseBoolean(val));
		} else if (varCls.equals(String.class)) {
			f.set(o, val);
		} else {
			// skip unknown type
		}
	}

	@SuppressWarnings("unchecked")
	void setArray(Class varCls, Object o, Integer i, String val) {
		
		//JNative.log("class=" + varCls.getName() + " index=" + i + " val=" +  val);
		
		if (varCls.equals(Byte.class) || varCls.equals(byte.class)) {
			Array.setByte(o, i, Byte.parseByte(val));
		} else if (varCls.equals(Double.class) || varCls.equals(double.class)) {
			Array.setDouble(o, i, Double.parseDouble(val));
		} else if (varCls.equals(Float.class) || varCls.equals(float.class)) {
			Array.setFloat(o, i, Float.parseFloat(val));
		} else if (varCls.equals(Integer.class) || varCls.equals(int.class)) {
			Array.setInt(o, i, Integer.parseInt(val));
		} else if (varCls.equals(Long.class) || varCls.equals(long.class)) {
			Array.setLong(o, i, Long.parseLong(val));
		} else if (varCls.equals(Short.class) || varCls.equals(short.class)) {
			Array.setShort(o, i, Short.parseShort(val));
		} else if (varCls.equals(Boolean.class) || varCls.equals(boolean.class)){
			Array.setBoolean(o, i, Boolean.parseBoolean(val));
		} else if (varCls.equals(String.class)) {
			Array.set(o, i, val);
		} else {
			// skip unknown type
		}
	}
	
	@SuppressWarnings("unchecked")
	List parseList(Object arr, String val) {
				
		if (arr == null || ((List) arr).size() == 0){
			log.error("Coudn't define component clas of list, val={}", val);
			return null;
		}
		
		final Class componentType = ((List) arr).get(0).getClass();
		
		final String[] elements = val.split(",");
		
		final List list = new ArrayList();
				
		for (int n=0; n < elements.length; n++){
			try {
				list.add(toObject(componentType, elements[n]));
			}catch (UnsupportedTypeException e) {
				log.error("UnsupportedTypeException", e);
			}
		}
		
		return list;
	}
	
	private static interface ArrayPointerIF{
		Object get() throws IllegalArgumentException, IllegalAccessException;
		void set(Object arr) throws IllegalArgumentException, IllegalAccessException;
	}
	
	private static class AA implements ArrayPointerIF{
		final Object arr;
		final int i;
		
		AA(Object arr, int i){
			this.arr = arr;
			this.i = i;
		}
		
		@Override
		public Object get() {
			return Array.get(this.arr, i);
		}

		@Override
		public void set(Object arr) {
			Array.set(this.arr, i, arr);			
		}
		
	}

	@SuppressWarnings("unchecked")
	void parseArray(ArrayPointerIF ap, String val) throws IllegalArgumentException, IllegalAccessException {
		
		Object arr = ap.get();
		
		final Class arrClass = arr.getClass().getComponentType(); /* Array.get(arr, 0).getClass();*/

		if (arrClass.isArray()) {
			String[] elements = val.split(";");
			int n = elements.length;
			
			for (int i = 0; i < Array.getLength(arr); i++) {
				if (i < n) {
					parseArray(new AA(arr, i), elements[i]);
				} else if (Array.get(Array.get(arr, i), 0).getClass().equals(String.class) ) {
					parseArray(new AA(arr, i), "");
				} else {
					parseArray(new AA(arr, i), "0");
				}
			}
		} else {

			String[] elements = val.split(",");
			int n = elements.length;
			
			if (Array.getLength(arr) != n){
				arr = Array.newInstance(arrClass, n);
				ap.set(arr);
			}
			
			for (int i = 0; i < Array.getLength(arr); i++) {

				if (i < n) {
					setArray(arrClass, arr, i, elements[i]);
				} else if (arrClass.equals(String.class)) {
					setArray(arrClass, arr, i, "");
				} else {
					setArray(arrClass, arr, i, "0");
				}

			}
		}
	}

	@SuppressWarnings("unchecked")
	boolean setValue(final String fullVarName, String val, boolean hooks) {
		final Field f;
		
		//JNative.log("var=" + var + " val="+val);
		final String var = fullVarName.replace(name + ".", "");

		try {
			f = cls.getField(var);
		} catch (SecurityException e) {
			log.error("", e);
			return false;
		} catch (NoSuchFieldException e) {
			log.error("", e);
			return false;
		}
		
		if (f.isAnnotationPresent(NoPersist.class)){
			log.debug("NoPersist present, skiping {}", var);
			return false;
		}
		
		if (Modifier.isFinal(f.getModifiers())){
			return false;
		}

		try {
			Class varCls = f.getType();

			if (varCls.isArray()) {
				parseArray(new ArrayPointerIF(){

					@Override
					public Object get() throws IllegalArgumentException, IllegalAccessException {
						return f.get(o);
					}

					@Override
					public void set(Object arr) throws IllegalArgumentException, IllegalAccessException {
						f.set(o, arr);						
					}
					
				}, val);
			}else if (varCls.isAssignableFrom(List.class)){
				f.set(o, parseList(f.get(o), val));
			} else {
				parseVal(varCls, f, val);
			}

		} catch (IllegalArgumentException e) {
			log.error("var:{}, val:{}, exception:{}", new Object[]{fullVarName, val, e});
			return false;
		} catch (IllegalAccessException e) {
			log.error("", e);
			return false;
		}
		
		if (hooks && o instanceof PersistSettingsIF && !f.isAnnotationPresent(NoCluster.class)){
			((PersistSettingsIF)o).updated(fullVarName, val);
		}
		
		return true;
	}

	int getArrayEfectiveLen(Object arr) {
		int length = Array.getLength(arr);
		int i;

		for (i = length - 1; i >= 0; i--) {
			Object arrItem = Array.get(arr, i);

			if (arrItem.getClass().isArray()) {

				if (getArrayEfectiveLen(arrItem) > 0) {
					return i + 1;
				} else {
					continue;
				}

			} else if (!arrItem.getClass().equals(String.class) && arrItem.toString().equals("0")) {
				continue;
			} else if (arrItem.toString().equals("")) {
				continue;
			}

			return i + 1;
		}

		return 0;
	}

	String getArrayValue(Object fieldObj) {
		int length = getArrayEfectiveLen(fieldObj);
		String out = new String();

		if (length == 0) {
			//return Array.get(fieldObj, 0).getClass().equals(out.getClass()) ? ""
			return fieldObj.getClass().getComponentType().equals(String.class) ? ""
					: "0";
		}

		for (int i = 0; i < length; i++) {
			Object arrItem = Array.get(fieldObj, i);

			if (arrItem.getClass().isArray()) {
				out += getArrayValue(arrItem);
				if (i < length - 1)
					out += ";";
			} else {
				out += Array.get(fieldObj, i).toString();
				if (i < length - 1)
					out += ",";
			}
		}

		return out;
	}

	String getValue(String var) throws SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException {
		
		var = var.replace(name + ".", "");
		
		Field f = cls.getField(var);
		Object fieldObj = f.get(o);

		if (f.getType().isArray()) {
			return getArrayValue(fieldObj);
		}else if (f.getType().isAssignableFrom(List.class)){
			final Object arrayObj[] =  new Object[((List)fieldObj).size()];
			for (int i=0; i<((List)fieldObj).size(); i++)
				arrayObj[i] = ((List)fieldObj).get(i);
			
			return getArrayValue(arrayObj);
		}

		return f.get(o).toString();
	}

	void genXML(StringBuilder out) {
		Field f[] = cls.getFields();

		//out += "<GLOBALS \n";

		for (Field i : f) {
				try {
					out.append(i.getName());
					out.append("=\"");
					out.append(EscapeChars.forXML(getValue(i.getName())));
					out.append("\" \n");
				} catch (SecurityException e) {
					log.error("", e);
				} catch (IllegalArgumentException e) {
					log.error("", e);
				} catch (NoSuchFieldException e) {
					log.error("", e);
				} catch (IllegalAccessException e) {
					log.error("", e);
				}
		}

		//out += "/>";
		return;
	}
	
	@SuppressWarnings("unchecked")
	String getType(Class cls){
		if (cls.equals(Byte.class) || cls.equals(byte.class)){
			return "int8_t";
		}else if (cls.equals(Double.class) || cls.equals(double.class)){
			return "double";
		}else if (cls.equals(Float.class) || cls.equals(float.class)){
			return "float";
		}else if (cls.equals(Integer.class) || cls.equals(int.class)){
			return "int";
		}else if (cls.equals(Long.class) || cls.equals(long.class)){
			return "long";
		}else if (cls.equals(Short.class) || cls.equals(short.class)){
			return "int16_t";
		}else if (cls.equals(Boolean.class) || cls.equals(boolean.class)){
			return "boolean";
		}
		
		return "String";
	}
	
	@SuppressWarnings("unchecked")
	String genTypes(){
		String out = new String();
		
		Field f[] = cls.getFields();
		
		for (Field i: f){
			Object fieldObj;
			try {
				fieldObj = i.get(o);
				
				log.debug("field '{}' class '{}'", i.getName(), i.getType().toString());
				
				if (i.getType().isArray()) {
					
					if (fieldObj.getClass().getComponentType().isArray()){
						Class cls2= fieldObj.getClass().getComponentType();
						Object arrElement = Array.get(fieldObj, 0);
						out += getType(cls2.getComponentType()) + " " + name + "." + i.getName() + "[" + Array.getLength(arrElement) + "][" + Array.getLength(fieldObj) + "];\n";
					}else{
						out += getType(fieldObj.getClass().getComponentType()) + " " + name + "." + i.getName() + "[" + Array.getLength(fieldObj) + "];\n";						
					}
										 
				}else if (i.getType().isAssignableFrom(String.class)){				
					out+= "char " + name + "." + i.getName() + "[" + ((String)fieldObj).length() +  "];\n";
				}else if (i.getType().isAssignableFrom(List.class)){
					out += getType(((List)fieldObj).get(0).getClass()) + " " + name + "." + i.getName() + "[" + ((List)fieldObj).size() + "];\n";
				}else {
					out+= getType(i.getType()) + " " + name + "." + i.getName() + ";\n";
				}

			} catch (IllegalArgumentException e) {
				log.error("", e);
			} catch (IllegalAccessException e) {
				log.error("", e);
			}
			
		}
		return out;
	}

}
