package stream.clustering

import org.apache.flink.streaming.api.scala._
import stream.clustering.StreamClustering._
import org.apache.flink.api.common.functions.RichMapFunction
import scala.util.Random
import java.util.Arrays
import java.util.ArrayList
import scala.collection.mutable.MutableList
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.api.common.functions.MapFunction

object SimpleStreamClustering {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val input1 = readInput("/Users/gyfora/OneDrive/LIU/streamclustering/data1.csv", env).map(x=>{Thread.sleep(2);x})
    val input2 = readInput("/Users/gyfora/OneDrive/LIU/streamclustering/data2.csv", env).map(x=>{Thread.sleep(2);x})
    val input3 = readInput("/Users/gyfora/OneDrive/LIU/streamclustering/data3.csv", env).map(x=>{Thread.sleep(2);x})

    

    clusterNonParallel(input1, new SimpleKMeans(4), "/Users/gyfora/OneDrive/LIU/streamclustering/data1_nonParallelNB.csv")
    clusterNonParallel(input1, new SimpleKMeans(4, timeBiasedMean(0.1)), "/Users/gyfora/OneDrive/LIU/streamclustering/data1_nonParallelTB.csv")

    clusterParallel(input1, new ParallelKMeans(4, timeBiasedMean(0.1),100), "/Users/gyfora/OneDrive/LIU/streamclustering/data1_parallelTB.csv")
    clusterParallel(input1, new ParallelKMeans(4, timeBiasedMean(0.1),100, distanceBased(0.5)), "/Users/gyfora/OneDrive/LIU/streamclustering/data1_parallelTB_D.csv")

    env.execute
    

  }

  
}