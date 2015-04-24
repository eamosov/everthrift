package com.knockchat.utils.thrift;

import java.util.Map;

import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.meta_data.FieldMetaData;

import com.knockchat.utils.Pair;

public class Utils {

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static Pair<Class<? extends TBase>, Map<? extends TFieldIdEnum, FieldMetaData>> getRootThriftClass(Class<? extends TBase> cls){
		Map<? extends TFieldIdEnum, FieldMetaData> map = null;
		Class<? extends TBase> nextThriftClass = cls;
		Class<? extends TBase> thriftClass;
		do{
			thriftClass = nextThriftClass;
			try{
				map = FieldMetaData.getStructMetaDataMap(thriftClass);
			}catch(Exception e){
				map = null;
			}
			nextThriftClass = (Class<? extends TBase>)thriftClass.getSuperclass();
		}while(map == null && nextThriftClass !=null);
		
		if (map == null)
			return null;
		
		return Pair.<Class<? extends TBase>, Map<? extends TFieldIdEnum, FieldMetaData>>create(thriftClass, map);
	}	
}
