package com.knockchat.hibernate;

import java.util.List;

import org.hibernate.transform.ResultTransformer;

import com.knockchat.utils.meta.MetaClass;
import com.knockchat.utils.meta.MetaClasses;
import com.knockchat.utils.meta.MetaProperty;

public class MetaResultTransformer implements ResultTransformer {

    private final MetaClass metaClass;

    public static MetaResultTransformer getTransformer(Class targetClass){
        if ( targetClass == null ) {
            throw new IllegalArgumentException( "resultClass cannot be null" );
        }
        return new MetaResultTransformer(targetClass);
    }

    private MetaResultTransformer(Class targetClass) {
        metaClass = MetaClasses.get(targetClass);
        if (metaClass == null)
        	throw new RuntimeException("couldn't find metaclass for " + targetClass.getSimpleName());
    }

    @Override
    public Object transformTuple(Object[] tuple, String[] aliases) {
        Object result = metaClass.newInstance();
        for (int i = 0; i < tuple.length; i++)
            setProperty(result,aliases[i], tuple[i]);
        return result;
    }

    @Override
    public List transformList(List collection) {
        return collection;
    }

    private void setProperty(Object target,String properyName, Object  value){
    	final MetaProperty p = metaClass.getProperty(properyName);
//    	if (p == null)
//    		p = metaClass.getProperty(CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, properyName.toUpperCase()));    		
    	
    	if (p == null)
    		throw new RuntimeException("Property " + properyName + " not found in class " + metaClass.getName());
    	
    	p.set(target,value);
    }
}
