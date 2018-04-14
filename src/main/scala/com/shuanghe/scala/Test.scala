package com.shuanghe.scala

import scala.collection.Iterator
import scala.io.{BufferedSource, Source}

class Student {
    val classNumber: Int = 10
    val classScores: Array[Int] = new Array[Int](classNumber)
}

//提前定义
class PEStudent extends {
    override val classNumber: Int = 3
} with Student

////提前定义
//class Person extends {
//    val msg: String = "init"
//} with SayHello {}

object MainClass {
    def main(args: Array[String]): Unit = {
        //        val s = new Student
        //        println(s.classScores.length)
        //
        //        val p = new PEStudent
        //        println(p.classScores.length)
        //        println(p.classNumber)
        //
        //        //提前定义
        //        //        val p2 = new {
        //        //            override val classNumber: Int = 3
        //        //        } with Student
        //        //        println(p2.classScores.length)
        //        //        println(p2.classNumber)
        //
        //        println(new Product("dali", 22.2).equals(new Product("dali", 22.2)))

        //方法一: 使用Source.getLines返回的迭代器
        val source: BufferedSource = Source.fromFile("C:\\data\\我的坚果云\\Bigdata\\Spark\\DEPLOY_MODE.sh", "UTF-8")
        //        val lineIterator:Iterator[String] = source.getLines
        //        for (line <- lineIterator) println(line)

        //方法二: 将Source.getLines返回的迭代器，转换成数组
        //这里说明一点: 一个BufferedSource对象的getLines方法，只能调用一次，一次调用完之后，遍历了迭代器里所有的内容，就已经把文件里的内容读取完了
        //如果反复调用source.getLines，是获取不到内容的
        //此时，必须重新创建一个BufferedSource对象
        //                val lines = source.getLines.toArray
        //                for(line <- lines) println(line)

        //方法三: 调用Source.mkString，返回文本中所有的内容
        //        val lines = source.mkString
        //        println(lines)

        //BufferedSource，也实现了一个Iterator[Char]的这么一个trait
        //        for(c <- source) println(c)

        //        val source2 = Source.fromURL("http://www.baidu.com", "UTF-8")
        //        println(source2.mkString)
        //        val source2 = Source.fromString("Hello World")
        //        println(source2.mkString)
        source.close()

        //执行外部系统命令
        //        import sys.process._
        //        "dir C:\\data" !

        val pattern1 = "[a-z]+".r
        val str = "hello 123 world 456"
        for (matchString <- pattern1.findAllIn(str)) println(matchString)

        println(pattern1.findFirstIn(str))
    }
}

//equals
class Product(val name: String, val price: Double) {

    def canEqual(other: Any): Boolean = other.isInstanceOf[Product]

    override def equals(other: Any): Boolean =
        other match {
            case that: Product =>
                (that canEqual this) &&
                        name == that.name &&
                        price == that.price
            case _ => false
        }

    override def hashCode(): Int = {
        val state = Seq(name, price)
        state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
    }
}