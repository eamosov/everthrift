package com.knockchat.utils.meta.thrift;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.meta_data.EnumMetaData;
import org.apache.thrift.meta_data.FieldValueMetaData;
import org.apache.thrift.meta_data.StructMetaData;
import org.apache.thrift.protocol.TType;
import org.hibernate.mapping.Map;
import org.hibernate.mapping.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.knockchat.utils.meta.MetaProperty;

public class ThriftMetaProperty<F extends TFieldIdEnum> implements MetaProperty {
	
	private static final Logger log = LoggerFactory.getLogger(ThriftMetaProperty.class);
	
	private final F f;
	private final FieldValueMetaData md;
	private Class type;

	public ThriftMetaProperty(F f, FieldValueMetaData md, Class type) {
		this.f = f;
		this.md = md;
		this.type = type;
	}
	
	public Class<?> getType() {
		return type !=null ? type : getThriftType();
	}

	private Class<?> getThriftType() {
		switch(md.type){
		case TType.BOOL:
			return Boolean.TYPE;
		case TType.BYTE:
			return Byte.TYPE;
		case TType.DOUBLE:
			return Double.TYPE;
		case TType.I16:
			return Short.TYPE;
		case TType.I32:
			return Integer.TYPE;
		case TType.I64:
			return Long.TYPE;
		case TType.STRING:
			return String.class;
		case TType.STRUCT:
			return ((StructMetaData)md).structClass;
		case TType.MAP:
			return Map.class;
		case TType.SET:
			return Set.class;
		case TType.LIST:
			return List.class;
		case TType.ENUM:
			return ((EnumMetaData)md).enumClass;
		default:
			return null;
		}
	}

	@Override
	public String getName() {
		return f.getFieldName();
	}

	@Override
	public Object get(Object target) {
		if (((TBase)target).isSet(f))
			return ((TBase)target).getFieldValue(f);
		else
			return null;
	}
	
	private boolean isBox(Class box, Class primitive){
		
		return (box.equals(Byte.class) && primitive.equals(Byte.TYPE)) ||
				(box.equals(Double.class) && primitive.equals(Double.TYPE)) ||
				(box.equals(Short.class) && primitive.equals(Short.TYPE)) ||
				(box.equals(Integer.class) && primitive.equals(Integer.TYPE)) ||
				(box.equals(Long.class) && primitive.equals(Long.TYPE));
	}

	@Override
	public void set(Object target, Object value) {
								
		if (value!=null && !getType().isAssignableFrom(value.getClass()) && !isBox(value.getClass(), getType())){

			if (value instanceof Number){
				switch(md.type){
				case TType.BYTE:
					value = ((Number)value).byteValue();
					break;
				case TType.DOUBLE:
					value = ((Number)value).doubleValue();
					break;
				case TType.I16:
					value = ((Number)value).shortValue();
					break;
				case TType.I32:
					value = ((Number)value).intValue();
					break;
				case TType.I64:
					value = ((Number)value).longValue();
					break;
				}
			}
		}else if (value !=null && value.getClass().equals(byte[].class) && this.getType().equals(byte[].class)){
			//В thrift setFieldValue ожидает ByteBuffer для типа binary
			value = ByteBuffer.wrap(Arrays.copyOf((byte[])value, ((byte[])value).length));
		}
		
		((TBase)target).setFieldValue(f, value);

	}

}
