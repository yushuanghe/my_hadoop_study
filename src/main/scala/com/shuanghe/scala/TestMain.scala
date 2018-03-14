package com.shuanghe.scala

import scala.beans.BeanProperty

object TestMain {
    def main(args: Array[String]): Unit = {
        val a=new TestMain
        a.setA("haha")
        println(a.getA)
    }
}

class TestMain{
    @BeanProperty var a:String= _
}
