package org.rawkintrevo

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


import com.twitter.hbc.core.endpoint.{StatusesFilterEndpoint, StreamingEndpoint, Location}

import java.util.Properties

import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.twitter.TwitterSource

import org.apache.mahout.math._
import org.apache.mahout.sparkbindings._

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf


import scala.collection.JavaConverters._
import scala.util.parsing.json._

/**
 * Skeleton for a Flink Streaming Job.
 *
 * For a full example of a Flink Streaming Job, see the SocketTextStreamWordCount.java
 * file in the same package/directory or have a look at the website.
 *
 * You can also generate a .jar file that you can submit on your Flink
 * cluster. Just type
 * {{{
 *   mvn clean package
 * }}}
 * in the projects root directory. You will find the jar in
 * target/ffsf17-twitter-recos-1.0-SNAPSHOT.jar
 * From the CLI you can then run
 * {{{
 *    ./bin/flink run -c org.rawkintrevo.StreamingJob target/ffsf17-twitter-recos-1.0-SNAPSHOT.jar
 * }}}
 *
 * For more information on the CLI see:
 *
 * http://flink.apache.org/docs/latest/apis/cli.html
 */
object StreamingJob {
  def main(args: Array[String]) {
    // do spark stuff first...
    // yea.. spark stuff. so what- fuck you.
    val conf = new SparkConf().setAppName("Compute CCO")
    val sc = new SparkContext(conf)

    implicit val sdc: org.apache.mahout.sparkbindings.SparkDistributedContext = sc2sdc(sc)

    import org.apache.mahout.sparkbindings.indexeddataset.IndexedDatasetSpark

    val baseDir = "/home/rawkintrevo/gits/ffsf17-twitter-recos/data"
    // We need to turn our raw text files into RDD[(String, String)]
    val userLocalRDD = sc.textFile(baseDir + "/user-local.csv").map(line => line.split(",")).filter(_.length == 2).map(a => (a(0), a(1)))
    val userLocalIDS = IndexedDatasetSpark.apply(userLocalRDD)(sc)

    val userFriendsRDD = sc.textFile(baseDir + "/user-friends.csv").map(line => line.split(",")).filter(_.length == 2).map(a => (a(0), a(1)))
    val userFriendsIDS = IndexedDatasetSpark.apply(userFriendsRDD)(sc)

    val userHashtagsRDD = sc.textFile(baseDir + "/user-ht.csv").map(line => line.split(",")).filter(_.length == 2).map(a => (a(0), a(1)))
    val userHashtagsIDS = IndexedDatasetSpark.apply(userHashtagsRDD)(sc)

    val userWordsRDD = sc.textFile(baseDir + "/user-words.csv").map(line => line.split(",")).filter(_.length == 2).map(a => (a(0), a(1)))
    val userWordsIDS = IndexedDatasetSpark.apply(userWordsRDD)(sc)

    import org.apache.mahout.math.cf.SimilarityAnalysis

    val hashtagReccosLlrDrmListByUser = SimilarityAnalysis.cooccurrencesIDSs(Array(userHashtagsIDS, userWordsIDS, userFriendsIDS, userLocalIDS), maxInterestingItemsPerThing = 100, maxNumInteractions = 500, randomSeed = 1234)


    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val htReccosLlrDrmListByUserStream = env.fromElements(hashtagReccosLlrDrmListByUser(0).matrix.collect)

    htReccosLlrDrmListByUserStream.broadcast // this matrix and a few Maps are what I'm trying to serialize.

    /**
      * todo: make the following parameters:
      * twitter-
      *   consumer_key
      *   consumer_secret
      *   token
      *   token_secret
      *   query_words (currently pizza
      */

    // From my Zeppelin Demo at FF Berlin 2016
    val p = new Properties();
    p.setProperty(TwitterSource.CONSUMER_KEY, "yVR3KnJ44DxhtiGnX60TqQ");
    p.setProperty(TwitterSource.CONSUMER_SECRET, "QsZMhnSPJNXbSDoc0l5pbTLKmwCcOsnvMCdUQYNR34");
    p.setProperty(TwitterSource.TOKEN, "1566016094-gzgtihGpXmYq3oEZ00LWOwSnaEOZwspCQkU0i3k");
    p.setProperty(TwitterSource.TOKEN_SECRET, "Qpo3AM2mfbOgaYJFLRpfRsrJeBe2CxkfzS7KX28vSQE");

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
          val tweet: Map[String, Any] = e.asInstanceOf[Map[String, Any]]
          val text: String = tweet("text").asInstanceOf[String]
          val words: Array[String] = text.split("\\s+").map(word => word.replaceAll("[^A-Za-z0-9]", "").toLowerCase())
          // could filter stop word here- but i don't care.
          words.mkString(" ")
        }
        case None => "failed"
      }

      htReccosLlrDrmListByUserStream // maybe this will work? lol.

      output
    }).writeAsText("/home/rawkintrevo/gits/ffsf17-twitter-recos/tweets.txt", WriteMode.OVERWRITE)

    env.execute("Flink Streaming Scala API Skeleton")
  }
}