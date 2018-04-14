package com.shuanghe.scala

class MyAnnotationTest(var value: Int) extends annotation.Annotation {

}

@MyAnnotationTest(100)
class MyTest