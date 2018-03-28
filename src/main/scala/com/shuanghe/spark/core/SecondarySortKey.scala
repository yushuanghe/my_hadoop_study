package com.shuanghe.spark.core

/**
  * scala 二次排序自定义key
  *
  * @param first
  * @param second
  */
class SecondarySortKey(val first: Int, val second: Int) extends Ordered[SecondarySortKey] with Serializable {
    override def compare(that: SecondarySortKey): Int = {
        if (this.first - that.first != 0) {
            this.first - that.first
        } else {
            this.second - that.second
        }
    }
}
