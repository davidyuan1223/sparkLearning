package org.apache.spark.annotation;

import java.lang.annotation.*;

@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE,ElementType.FIELD,ElementType.METHOD,ElementType.PARAMETER,
ElementType.CONSTRUCTOR,ElementType.LOCAL_VARIABLE,ElementType.PACKAGE})
public @interface Stable {
}
