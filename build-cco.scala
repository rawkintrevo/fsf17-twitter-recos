/**
  * Created by rawkintrevo on 4/7/17.
  */

// Only need these to intelliJ doesn't whine


import org.apache.mahout.drivers.ItemSimilarityDriver.parser
import org.apache.mahout.math._
import org.apache.mahout.math.scalabindings._
import org.apache.mahout.math.drm._
import org.apache.mahout.math.scalabindings.RLikeOps._
import org.apache.mahout.math.drm.RLikeDrmOps._
import org.apache.mahout.math.indexeddataset.BiDictionary
import org.apache.mahout.sparkbindings._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
val conf = new SparkConf().setAppName("Simple Application")
val sc = new SparkContext(conf)

implicit val sdc: org.apache.mahout.sparkbindings.SparkDistributedContext = sc2sdc(sc)


// </pandering to intellij>


// http://files.grouplens.org/datasets/hetrec2011/hetrec2011-lastfm-2k.zip
// start mahout shell like this: $MAHOUT_HOME/bin/mahout spark-shell

import org.apache.mahout.sparkbindings.indexeddataset.IndexedDatasetSpark

val baseDir = "/home/rawkintrevo/gits/ffsf17-twitter-recos/data"
// We need to turn our raw text files into RDD[(String, String)]
val userLocalRDD = sc.textFile(baseDir + "/user-local.csv")
  .map(line => line.split(",")).filter(_.length == 2).map(a => (a(0), a(1)))
val userLocalIDS = IndexedDatasetSpark.apply(userLocalRDD)(sc)

val userFriendsRDD = sc.textFile(baseDir + "/user-friends.csv")
  .map(line => line.split(",")).filter(_.length == 2).map(a => (a(0), a(1)))
val userFriendsIDS = IndexedDatasetSpark.apply(userFriendsRDD)(sc)

val userHashtagsRDD = sc.textFile(baseDir + "/user-ht.csv")
  .map(line => line.split(",")).filter(_.length == 2).map(a => (a(0), a(1)))
val userHashtagsIDS = IndexedDatasetSpark.apply(userHashtagsRDD)(sc)

val userWordsRDD = sc.textFile(baseDir + "/user-words.csv")
  .map(line => line.split(",")).filter(_.length == 2).map(a => (a(0), a(1)))
val userWordsIDS = IndexedDatasetSpark.apply(userWordsRDD)(sc)

import org.apache.mahout.math.cf.SimilarityAnalysis


val hashtagReccosLlrDrmListByUser = SimilarityAnalysis.cooccurrencesIDSs(
  Array(userHashtagsIDS, userWordsIDS, userFriendsIDS),
  maxInterestingItemsPerThing = 100,
  maxNumInteractions = 500,
  randomSeed = 1234)

import java.io._

def readBiDictionary(path: String): org.apache.mahout.math.indexeddataset.BiDictionary = {
  val ois = new ObjectInputStream(new FileInputStream(path))
  val map = ois.readObject.asInstanceOf[org.apache.mahout.math.indexeddataset.BiDictionary]
  ois.close
  map
}

def writeBiDictionary(m: org.apache.mahout.math.indexeddataset.BiDictionary, path: String): Unit = {
  val oos = new ObjectOutputStream(new FileOutputStream(path))
  oos.writeObject(m)
  oos.close
}

def matrixWriter(m: Matrix, path: String): Unit = {
  import MahoutCollections._
  import scala.collection.JavaConversions._

  val oos = new ObjectOutputStream(new FileOutputStream(path))
  oos.writeObject(m.toArray.map(v => v.toMap).toList)
  oos.close
}

def matrixReader(path: String): Matrix ={
  val ois = new ObjectInputStream(new FileInputStream(path))
  // for row in matrix
  val m = ois.readObject.asInstanceOf[List[Map[Int,Double]]]
  ois.close
  sparse(m.map(m => svec(m)):_*)
}


matrixWriter(hashtagReccosLlrDrmListByUser(0).matrix.collect, baseDir + "/hashtags.matrix")
matrixWriter(hashtagReccosLlrDrmListByUser(1).matrix.collect, baseDir + "/words.matrix")
matrixWriter(hashtagReccosLlrDrmListByUser(2).matrix.collect, baseDir + "/friends.matrix")

writeBiDictionary(hashtagReccosLlrDrmListByUser(0).columnIDs, baseDir + "/hashtags.bidictionary")
writeBiDictionary(hashtagReccosLlrDrmListByUser(1).columnIDs, baseDir + "/words.bidictionary")
writeBiDictionary(hashtagReccosLlrDrmListByUser(2).columnIDs, baseDir + "/friends.bidictionary")
// write all matrices

val localMat = matrixReader(baseDir + "/local.matrix")
val hashtagsMat = matrixReader(baseDir + "/hashtags.matrix")
val wordsMat = matrixReader(baseDir + "/words.matrix")
val friendsMat = matrixReader(baseDir + "/friends.matrix")

val localBiDict = readBiDictionary(baseDir + "/local.bidictionary")
val hashtagsBiDict = readBiDictionary(baseDir + "/hashtags.bidictionary")
val wordsBiDict = readBiDictionary(baseDir + "/words.bidictionary")
val friendsBiDict = readBiDictionary(baseDir + "/friends.bidictionary")

//// Frome here on it was just me playing around with stuff. You cna go flink now if you like.

val userWords = "I am a very silly squirrel I eat dirty pizza".toLowerCase().split(" ")

def listOfStringsToSVec(strings: List[String], bidict: org.apache.mahout.math.indexeddataset.BiDictionary): RandomAccessSparseVector = {
  svec(strings.map(w => bidict.toMap.getOrElse[Int](w, -1)).filter(_ >= 0).map(i => (i, 1.0)), cardinality = bidict.size)
}

listOfStringsToSVec(List("foo", "america", "pizza", "dirty"), wordsBiDict)
val hashtagsVec = svec(userWords.map(w => wordsBiDict.toMap.getOrElse[Int](w, -1)).filter(_ >= 0).map(i => (i, 1.0)), cardinality = hashtagsBiDict.size)

val wordReccs = hashtagsMat %*% hashtagsVec

import org.apache.mahout.math.scalabindings.MahoutCollections._
val recs = wordReccs.toMap.toList.sortWith(_._2 > _._2).take(10)
for (item <- recs.toMap.keys.filterNot(userWords.toSet)){
  println(s"${wordsBiDict.inverse(item)} : ${recs.toMap.get(item)}")
}





