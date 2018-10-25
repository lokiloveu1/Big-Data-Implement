package comp9313.proj2

import org.apache.spark._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf  
import org.apache.spark.SparkContext 
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import Array._


object Problem2 { 
  
    def main(args: Array[String]) {
        //ignore logs  
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)   
        val inputFile = args(0)        
        val k = args(1).toInt
        val conf = new SparkConf().setAppName("Problem2").setMaster("local")  
        val sc = new SparkContext(conf)          
        val input = sc.textFile(inputFile) 
 
        /*
         * ------------------------------1. vertexs------------------------------------------
         * Create vertex[(VertexId,1)]
         * */
        val vertexs: RDD[(VertexId, Int)] = input.map{line =>{
            val nodes: Array[String] = line.split("\t| ")
            (nodes(1).toLong,1)                   
        }
        }.reduceByKey((a,b)=>1)        
        val vertexArray = vertexs.collect()
      

        /*
         * ------------------------------2. edges------------------------------------------ 
         * Create edge[(VertexId,VertexId),1]
         * */
        val edges:RDD[Edge[(Int)]] = input.map{line =>{
            val nodes: Array[String]  = line.split("\t| ")
            Edge(nodes(1).toLong,nodes(2).toLong,1)        
        }}
        val edgeArray = edges.collect()
          
        /*
         * ------------------------------3. graph------------------------------------------
         * 1. Create vertexRDD and edgeRdd
         * 2. Create a graph based on vertexRDD and edgeRDD, Graph[VD,ED] 
         */            
        val vertexRDD: RDD[(VertexId,Int)] = sc.parallelize(vertexArray) 
        val edgeRDD: RDD[Edge[Int]] = sc.parallelize(edgeArray) 
        val graph = Graph(vertexRDD, edgeRDD)
        //graph.triplets.collect().foreach(println)

        /*
         * ------------------------------4. cycle------------------------------------------
         * 1. Create a cycleGraph 
         */  

         val circleGraph = Pregel(
            graph.mapVertices((id,attr)=>List[List[VertexId]]()),// set initialgraph
            List[List[VertexId]](), // set initial value to null
            k, //k - hop
            EdgeDirection.Out)( //direction of sending msg
                (id,attr,msg)=>(attr++msg), //vprog
                triplet=>{
                  if (triplet.srcAttr.length==0){ //the first msg
                    val results = List(List(triplet.srcId))
                    Iterator((triplet.dstId,results))
                  }else{ //sending msg with (scrAttr ++ srcId)
                      Iterator((triplet.dstId,triplet.srcAttr.map(e=>(e++List(triplet.srcId)))))
                       }
                        },
                (a,b)=>(a++b)//merge msg
                )
                         
        /*
         * ------------------------------5. final output------------------------------------------
         */ 
                
        //filter result of list length==k and the cycle startwith its own id
        val out1 = circleGraph.vertices.map{case(id,attr)=>(id,attr.filter(e=>e.length==k).filter(e=>e(0)==id))}
        //convert list to set and fliter same result
        val out2 = out1.map(a=>a._2).flatMap(a=>a).map(e=>e.toSet).distinct()
        //convert set to list and filter the list which length not equal to k
        val out3 = out2.map(a=>a.toList).filter(e=>e.length==k)
        //calculate the number of cycle
        val num = out3.count
        

//        circleGraph.vertices.collect().foreach(println)
//        out1.foreach(println)
//        println()
//        out2.foreach(println)
//        println()
//        out3.foreach(println)
        println(num)
    }
}