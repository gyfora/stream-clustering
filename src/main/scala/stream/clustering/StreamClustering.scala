package stream.clustering

import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.util.Collector

object StreamClustering {

  case class Point(id: Int, x: Double, y: Double)
  type Centers = List[(Point, Long)]
  type LabeledPoint = (Point, Int)
  type PointWithCount = (Point, Long)

  def readInput(inputPath: String, env: StreamExecutionEnvironment) = env.readTextFile(inputPath).map(line => {
    val split = line.split(",")
    Point(split(0).toInt, split(1).toDouble, split(2).toDouble)
  })

  def clusterNonParallel(input: DataStream[Point], clusterer: MapFunction[Point, LabeledPoint], output: String = ""): Unit = {
    val clustered = input.map(clusterer).setParallelism(1)
    if (output.equals("")) clustered.print else clustered.map(cp => (cp._1.id, cp._1.x, cp._1.y, cp._2)).writeAsCsv(output, writeMode = FileSystem.WriteMode.OVERWRITE).setParallelism(1)
  }

  def clusterParallel(input: DataStream[Point], clusterer: FlatMapFunction[Either[Point, Centers], Either[LabeledPoint, Centers]], output: String = ""): Unit = {
    val transformedInput: DataStream[Either[Point, Centers]] = input.map(Left(_))

    val clustered = transformedInput.iterate(2000)(clusterStepFunction(clusterer), true)

    if (output.equals("")) clustered.print else clustered.map(cp => (cp._1.id, cp._1.x, cp._1.y, cp._2)).writeAsCsv(output, writeMode = FileSystem.WriteMode.OVERWRITE).setParallelism(1)
  }

  def eucDist(p1: Point, p2: Point): Double = Math.sqrt((p2.x - p1.x) * (p2.x - p1.x) + (p2.y - p1.y) * (p2.y - p1.y))

  def nonBiasedMean(currentCenter: PointWithCount, point: Point): PointWithCount = {
    val n = currentCenter._2
    (Point(0, (currentCenter._1.x * n + point.x) / (n + 1), (currentCenter._1.y * n + point.y) / (n + 1)), n + 1)
  }

  def clusterStepFunction(clusterer: FlatMapFunction[Either[Point, Centers], Either[LabeledPoint, Centers]])(input: DataStream[Either[Point, Centers]]): (DataStream[Either[Point, Centers]], DataStream[LabeledPoint]) = {

    val transformed = input.flatMap(clusterer)

    val split = transformed.split(x => x match {
      case Left(lp) => Array("output")
      case Right(c) => Array("centers")
    })

    val centers: DataStream[Either[Point, Centers]] = split.select("centers").map(x => Right(x.right.get))
    val output: DataStream[LabeledPoint] = split.select("output").map(_.left.get)

    (centers.broadcast, output)
  }

  def timeBiasedMean(weightToNew: Double)(currentCenter: PointWithCount, point: Point): PointWithCount =
    (combineWithWeight(0.5, currentCenter._1, point), 1)

  def combineWithWeight(weightToNew: Double, oldP: Point, newP: Point): Point =
    Point(0, (1 - weightToNew) * oldP.x + weightToNew * newP.x, (1 - weightToNew) * oldP.y + weightToNew * newP.y)

  def clusterAndUpdateCenters(updateCenter: (PointWithCount, Point) => PointWithCount)(centers: Centers)(point: Point): (Int, Centers) = {
    val dists = for (i <- 0 to centers.size - 1) yield (i, eucDist(centers(i)._1, point))
    val c = dists.reduce((x, y) => if (x._2 < y._2) x else y)._1
    (c, centers.updated(c, updateCenter(centers(c), point)))
  }
  
  def simpleCombiner(combineWeight: Double = 0.5)(oldCenters: Centers, newCenters:Centers): Centers = {
    for ((oldC, newC) <- oldCenters.zip(newCenters)) yield (combineWithWeight(combineWeight, oldC._1, newC._1), oldC._2)
  }
  
  def distanceBased(combineWeight: Double = 0.5)(oldCenters: Centers, newCenters:Centers): Centers = {
    
    var oCenters = oldCenters;
    
    for (center <- newCenters) yield {
      val dists = for (i <- 0 to oCenters.size - 1) yield (i, eucDist(oCenters(i)._1,center._1))
      val c = dists.reduce((x, y) => if (x._2 < y._2) x else y)._1
      val updated: PointWithCount = (combineWithWeight(combineWeight, oCenters(c)._1,center._1),1)
      oCenters = removeIndex(oCenters, c)
      updated
    }
  }
  
  def removeIndex[A](list: List[A], ix: Int) = if (list.size < ix) list
                           else list.take(ix) ++ list.drop(ix+1)

  class SimpleKMeans(numClusters: Int, updateCenter: (PointWithCount, Point) => PointWithCount = nonBiasedMean)
        extends MapFunction[Point, LabeledPoint] {

    var centers: Centers = List()
    val cluster = clusterAndUpdateCenters(updateCenter) _ 

    def map(point: Point): LabeledPoint = {
      if (centers.size < numClusters) {
        centers = centers :+ (point, centers.size.toLong)
        (point, centers.size - 1)
      } else {
        val (label, newCenters) = cluster(centers)(point)
        centers = newCenters
        (point, label)
      }
    }
  }

  class ParallelKMeans(numClusters: Int, updateCenter: (PointWithCount, Point) => PointWithCount = nonBiasedMean, broadcastEvery: Int = 100, combiner: (Centers,Centers) => Centers = simpleCombiner(0.5)) extends FlatMapFunction[Either[Point, Centers], Either[LabeledPoint, Centers]] {

    var centers: Centers = List()
    var countSinceLastBroadcast = 0
    var updates = 0
    val cluster = clusterAndUpdateCenters(updateCenter) _

    def flatMap(pointOrCenters: Either[Point, Centers], out: Collector[Either[LabeledPoint, Centers]]): Unit = {

      pointOrCenters match {
        case Left(point) => {
          if (centers.size < numClusters) {
            centers = centers :+ (point, centers.size.toLong)
            out.collect(Left(point, centers.size - 1))
          } else {
            val (label, newCenters) = cluster(centers)(point)
            centers = newCenters
            out.collect(Left((point, label)))
          }

          countSinceLastBroadcast = countSinceLastBroadcast + 1;
          if (countSinceLastBroadcast >= broadcastEvery) {
            countSinceLastBroadcast = 0
            out.collect(Right(centers))
          }

        }
        case Right(newCenters) => {
          if (centers.size == numClusters) {
            centers = combiner(centers,newCenters)
          }
        }
      }

    }
  }

}