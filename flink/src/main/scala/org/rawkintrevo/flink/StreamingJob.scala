package org.rawkintrevo.flink

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import java.io.{FileInputStream, ObjectInputStream}

import com.twitter.hbc.core.endpoint.{Location, StatusesFilterEndpoint, StreamingEndpoint}
import java.util.Properties

import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.twitter.TwitterSource
import org.apache.mahout.math._
import org.apache.mahout.math.indexeddataset.DefaultIndexedDatasetReadSchema
import org.apache.mahout.math.scalabindings.{sparse, svec}
import org.apache.mahout.math.scalabindings.RLikeOps._

import org.apache.mahout.math.scalabindings.MahoutCollections._

import scala.collection.JavaConverters._
import scala.io.Source
import scala.util.parsing.json._

object StreamingJob {
  def matrixReader(path: String): List[Map[Int, Double]] ={
    val ois = new ObjectInputStream(new FileInputStream(path))
    // for row in matrix
    val m = ois.readObject.asInstanceOf[List[Map[Int,Double]]]
    ois.close
    m
  }

  def readBiDictionary(path: String): org.apache.mahout.math.indexeddataset.BiDictionary = {
    val ois = new ObjectInputStream(new FileInputStream(path))
    val map = ois.readObject.asInstanceOf[org.apache.mahout.math.indexeddataset.BiDictionary]
    ois.close
    map
  }

  def listOfStringsToSVec(strings: List[String], bidict: org.apache.mahout.math.indexeddataset.BiDictionary): RandomAccessSparseVector = {
    svec(strings.map(w => bidict.toMap.getOrElse[Int](w, -1)).filter(_ >= 0).map(i => (i, 1.0)), cardinality = bidict.size)
  }

  def main(args: Array[String]) {
    val baseDir = args(0)

    /** Load CCO Data From Files *****************************************************************/
    val localProtoMat = matrixReader(baseDir + "/data/local.matrix")
    val hashtagsProtoMat = matrixReader(baseDir + "/data/hashtags.matrix")
    val wordsProtoMat = matrixReader(baseDir + "/data/words.matrix")
    val friendsProtoMat = matrixReader(baseDir + "/data/friends.matrix")

    val localBiDict = readBiDictionary("/home/rawkintrevo/gits/ffsf17-twitter-recos/data/local.bidictionary")
    val hashtagsBiDict = readBiDictionary("/home/rawkintrevo/gits/ffsf17-twitter-recos/data/hashtags.bidictionary")
    val wordsBiDict = readBiDictionary("/home/rawkintrevo/gits/ffsf17-twitter-recos/data/words.bidictionary")
    val friendsBiDict = readBiDictionary("/home/rawkintrevo/gits/ffsf17-twitter-recos/data/friends.bidictionary")

    /**********************************************************************************************/
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val creds = JSON.parseFull(Source.fromFile(baseDir + "/conf/credentials.json").getLines.mkString).get.asInstanceOf[Map[String,Map[String, String]]]("twitter")

    val p = new Properties();
    p.setProperty(TwitterSource.CONSUMER_KEY, creds("consumerKey"));
    p.setProperty(TwitterSource.CONSUMER_SECRET, creds("consumerSecret"));
    p.setProperty(TwitterSource.TOKEN, creds("token"));
    p.setProperty(TwitterSource.TOKEN_SECRET, creds("tokenSecret"));

    //////////////////////////////////////////////////////
    // Create an Endpoint to Track our terms
    class myFilterEndpoint extends TwitterSource.EndpointInitializer with Serializable {
      @Override
      def createEndpoint(): StreamingEndpoint = {
        val endpoint = new StatusesFilterEndpoint()
        //endpoint.locations(List(chicago).asJava)
        endpoint.trackTerms(List("pizza").asJava)
        return endpoint
      }
    }

    val source = new TwitterSource(p)
    val epInit = new myFilterEndpoint()

    source.setCustomEndpointInitializer( epInit )

    val streamSource = env.addSource( source );

    streamSource.map(jsonString => {
      val result = JSON.parseFull(jsonString)

      val output = result match {
        case Some(e) => {
          /*****************************************************************************************
            * Some pretty lazy tweet handling
            */
          val tweet: Map[String, Any] = e.asInstanceOf[Map[String, Any]]
          val text: String = tweet("text").asInstanceOf[String]
          val words: Array[String] = text.split("\\s+").map(word => word.replaceAll("[^A-Za-z0-9]", "").toLowerCase())
          // could filter stop word here- but i don't care.

          val entities = tweet("entities").asInstanceOf[Map[String, List[Map[String, String]]]]
          val hashtags: List[String] = entities("hashtags").toArray.map(m => m.getOrElse("text","").toLowerCase()).toList
          val mentions: List[String] = entities("user_mentions").toArray.map(m => m.getOrElse("id_str", "")).toList
          //val local = tweet("place").asInstanceOf[Map[String, Any]]("name").asInstanceOf[String]
          val local = ""

          //val localMat = sparse(localProtoMat.map(m => svec(m)):_*)
          //val userLocalVec = listOfStringsToSVec(List(output.local), localBiDict)

          /****************************************************************************************
            * Mahout CCO
            */
          val hashtagsMat = sparse(hashtagsProtoMat.map(m => svec(m, cardinality = hashtagsBiDict.size)):_*)
          val wordsMat = sparse(wordsProtoMat.map(m => svec(m, cardinality= wordsBiDict.size)):_*)
          val friendsMat = sparse(friendsProtoMat.map(m => svec(m, cardinality = friendsBiDict.size)):_*)

          val userWordsVec = listOfStringsToSVec(words.toList, wordsBiDict)
          val userHashtagsVec = listOfStringsToSVec(hashtags, hashtagsBiDict)
          val userMentionsVec = listOfStringsToSVec(mentions, friendsBiDict)

          // localMat %*% userLocalVec +
          val reccos = hashtagsMat %*% userHashtagsVec + wordsMat %*% userWordsVec + friendsMat %*% userMentionsVec

          /*****************************************************************************************
            * Sort and Pretty Print
            */
          val recs = reccos.toMap.toList.sortWith(_._2 > _._2).take(4)
          val recString = recs.toMap.keys.filterNot(hashtags.toSet).map(item => s"(${hashtagsBiDict.inverse(item)} : ${recs.toMap.getOrElse(item, "")})").mkString("\n")
          val wordsString = userWordsVec.toMap.keys.map(item => s"${wordsBiDict.inverse(item)}").mkString(" ")
//          for (item <- recs.toMap.keys.filterNot(hashtags.toSet)){
//            println(s"${hashtagsBiDict.inverse(item)} : ${recs.toMap.get(item)}")
//          }
          var printWords = ""
          if (words.length > 0){
            printWords = words.mkString(" ")
          } else {
            printWords = ""
          }

          val output_string= s"""
               |---------------------------------------------------------
               |full tweet: ${jsonString}
               |text: ${printWords}
               |userWordsVec: ${wordsString}
               |hashtags used: ${hashtags}
               |hashtags reccomended:
               |${recString}
               |---------------------------------------------------------
             """.stripMargin
          output_string
        }
        case None => ""
      }

      output
    }).writeAsText(baseDir + "/tweets.txt", WriteMode.OVERWRITE)

    env.execute("Flink Streaming Twitter CCO")
  }
}
