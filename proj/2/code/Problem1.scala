package comp9313.proj2

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object Problem1 {
  def main(args: Array[String]) {
    // assign argument to variables
    val topK = args(0)
    val stopWordFile = args(1)
    val inputFile = args(2)
    val outputFolder = args(3)
    val conf = new SparkConf().setAppName("Problem1").setMaster("local")
    val sc = new SparkContext(conf)
    // 
    val stopWordList = sc.textFile(stopWordFile).collect.toList

    val tf = sc.textFile(inputFile)
    val validWordListList = tf.map(_.substring(9)).map(_.split(" ").toList.filter(word => !(stopWordList.contains(word) && word.charAt(0).isLetter))).collect.toList
    
    var coTermTupList = List[Tuple2[String,String]]()
    
    for (lst <- validWordListList) {
    	for (u <- 0 until lst.size) {
				for (w <- u+1 until lst.size) {
					if (lst(u) <= lst(w)) {
						coTermTupList = coTermTupList :+ (lst(u), lst(w));
					} else {
						coTermTupList = coTermTupList :+ (lst(w), lst(u));
					}
				}
			}
		}
		
		val rdd = sc.parallelize(coTermTupList.map(pair => (pair, 1)))
		
		val res = rdd.reduceByKey(_+_).collect.toList.sortBy(x => (-x._2, x._1.toString)).take(topK.toInt)
		
		val resRDD = sc.parallelize(res)
		
    resRDD.map(x => s"${x._1._1},${x._1._2}\t${x._2}").saveAsTextFile(outputFolder)

    sc.stop()
  }
}