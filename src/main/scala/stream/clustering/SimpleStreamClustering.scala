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

//	runExperiments("data1")
//	runExperiments("data2")
	runExperiments("data3")
    
  }
  
  def runExperiments(inputName: String) = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    
    val input = readInput("/Users/gyfora/OneDrive/LIU/streamclustering/"+inputName+".csv", env).map(x=>{Thread.sleep(2);x})
    
    clusterNonParallel(input, new SimpleKMeans(4), "/Users/gyfora/OneDrive/LIU/streamclustering/"+inputName+"_nonParallelNB.csv")
    clusterNonParallel(input, new SimpleKMeans(4, timeBiasedMean(0.1)), "/Users/gyfora/OneDrive/LIU/streamclustering/"+inputName+"_nonParallelTB.csv")

    clusterParallel(input, new ParallelKMeans(4, timeBiasedMean(0.1),100), "/Users/gyfora/OneDrive/LIU/streamclustering/"+inputName+"_parallelTB.csv")
    clusterParallel(input, new ParallelKMeans(4, timeBiasedMean(0.1),100, distanceBased(0.5)), "/Users/gyfora/OneDrive/LIU/streamclustering/"+inputName+"_parallelTB_D.csv")

    env.execute
  }

  
}