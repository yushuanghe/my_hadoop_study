package com.shuanghe.scala

import java.io.{File, PrintWriter}

object TestFile {
    def main(args: Array[String]): Unit = {
        //        testFileCopy
        //        testWriteFile
        recursiveTraversalSubDir
        //        serializableTest
    }

    def testFileCopy(): Unit = {
        import java.io._

        val fis = new FileInputStream(new File("C:\\data\\我的坚果云\\Bigdata\\Spark\\DEPLOY_MODE.sh"))
        val fos = new FileOutputStream(new File("C:\\Users\\yushuanghe\\Desktop\\testwrite.txt"))

        val buf = new Array[Byte](1024)
        fis.read(buf)
        //        println(buf.length)
        fos.write(buf, 0, 1024)

        fis.close()
        fos.close()
    }

    def testWriteFile(): Unit = {
        val pw = new PrintWriter("C:\\Users\\yushuanghe\\Desktop\\testwrite2.txt")
        pw.println("Hello World")
        pw.close()
    }

    def recursiveTraversalSubDir(): Unit = {
        def getSubdirIterator(dir: File): Iterator[File] = {
            //拿到子目录
            val childDirs = dir.listFiles.filter(_.isDirectory)

            //子目录+每一个子目录的子目录
            childDirs.toIterator ++ childDirs.toIterator.flatMap(getSubdirIterator _)
        }

        val iterator = getSubdirIterator(new File("C:\\Users\\yushuanghe\\Desktop\\SparkInternals"))

        for (d <- iterator) println(d)
    }

    def serializableTest(): Unit = {
        //如果要序列化，那么就必须让类，有一个@SerialVersionUID，定义一个版本号要让类继承一个Serializable trait

        @SerialVersionUID(42L) class Person(val name: String) extends Serializable
        val leo = new Person("leo")

        import java.io._

        val oos = new ObjectOutputStream(new FileOutputStream("C:\\Users\\yushuanghe\\Desktop\\test.obj"))
        oos.writeObject(leo)
        oos.close()

        val ois = new ObjectInputStream(new FileInputStream("C:\\Users\\yushuanghe\\Desktop\\test.obj"))
        val restoredLeo = ois.readObject().asInstanceOf[Person]
        println(restoredLeo.name)
    }
}
