package com.knockchat.appserver.model.lazy;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.TYPE})
public @interface LazyAccessor {
	String[] value() default ""; //scenarios "default", "admin", "json" ...
}

/*

com.knockchat.node.model.account.AccountModel _account=obj.getAccount();  <-- LazyAccessor

if (_account == null){
        obj.loadAccount(r);  <-- LazyMethod
}
else    
if (_account !=null) {
        h.apply(_account);
}}

*/