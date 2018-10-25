package comp9313.proj2

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.collection.mutable.ArrayBuffer

object Problem1 {
  def main(args: Array[String]) {
    val inputFile = args(0)//input path
    val outputFolder = args(1)//output path
    val conf = new SparkConf().setAppName("Problem1").setMaster("local")//set conf
    val sc = new SparkContext(conf)
    val input =  sc.textFile(inputFile)//read file
    val reg = "[a-zA-Z]+"//regular input format(start with a-z or A-Z)
    val lines = input.flatMap(_.split("[\n]+")).collect()//read lines from file
    val list = ArrayBuffer[String]()
    //convert words to array
    for (z<-0 to (lines.length-1)){
      val words = lines(z).split("[\\s*$&#/\"'\\,.:;?!\\[\\](){}<>~\\-_]+").filter(x=>x.split("")(0).matches(reg))//split words
      for (i<-0 to (words.length-2)){
        for (j<-i+1 to (words.length-1)){
          //read w(i) and w(j) from lines 
          //and convert them to lowercase 
          //finally,add data into array(list)
          list+=words(i).toLowerCase+' '+words(j).toLowerCase
        }
      }
    }
    val list2 = list.map(x=>(x.split(" ")(0),x.split(" ")(1))).toArray//map to Array
    val list3 = sc.parallelize(list2)//get RDD
    //Group by key to calculate rf
    val re2 = list3.groupByKey.mapValues{ arr => arr.groupBy(identity).mapValues(_.size.toDouble / arr.size).toSeq}.flatMap{ case (k, vs) => vs.map(v => (k,v._1, v._2))}.collect
    val out = re2.sortBy(x=>(x._1,-x._3,x._2))//sort
    val out2 = out.map(x=>x._1+" "+x._2+" "+x._3)//output format
    val out3 = sc.parallelize(out2)
    out3.saveAsTextFile(outputFolder)
  }
}
