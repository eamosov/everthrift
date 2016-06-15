package org.everthrift.appserver.model.lazy;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 *
 	examples:
 	
 	@LazyMethod
	public AddressLineModel loadAddress(Registry r, Object parent){
	   ...//нужно присвоить значение свойству "address" и вернуть этот объект
	}

 	@LazyMethod
	public void loadAddress(Registry r, Object parent){
	}

 	@LazyMethod
	public void loadAddress(Registry r){
	   ...//загрузить свойство "address" через Registry в batch режиме
	}
	
 * @author fluder
 *
 */

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD})
public @interface LazyMethod {
	String[] value() default ""; //scenarios "default", "admin", "json" ...
}
