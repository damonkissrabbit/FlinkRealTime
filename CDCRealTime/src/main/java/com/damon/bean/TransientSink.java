package com.damon.bean;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.FIELD) // 表示该注解可以用于类的属性字段上
@Retention(RetentionPolicy.RUNTIME)  // 表示该注解在程序运行时仍然可用
// 定义了该注解的名称为 TransientSink
// java 中使用 @interface来定义一个注解
public @interface TransientSink{
}
