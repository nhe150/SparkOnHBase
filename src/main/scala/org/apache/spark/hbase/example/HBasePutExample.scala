/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Author Norman He 2014
 */

package org.apache.spark.hbase.example

import org.apache.spark.SparkContext
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Put
import org.apache.spark.hbase.HBaseContext
import org.apache.spark.SparkConf

object HBasePutExample {
  /**
   *setup a table with column family called 'ft'
   */
  def main(args: Array[String]) {

	  if (args.length < 2) {
    		System.out.println("HBasePutExample {tableName} {value} ");
    		return
      }
    	
      val tableName = args(0)
      val value = args(1)

    	
      val sparkConf = new SparkConf().setAppName("HBaseBulkPutExample " + tableName)
      val sc = new SparkContext(sparkConf)

      val conf = HBaseConfiguration.create();
	    conf.addResource(new Path("/etc/hbase/conf/core-site.xml"));
	    conf.addResource(new Path("/etc/hbase/conf/hbase-site.xml"));
    	
      val hbaseContext = new HBaseContext(sc, conf)
      val connection = hbaseContext.getConnection

      val myObj =  (Bytes.toBytes("1"), Array((Bytes.toBytes("ft"), Bytes.toBytes("1"), Bytes.toBytes(value))))
      hbaseContext.Put[(Array[Byte], Array[(Array[Byte], Array[Byte], Array[Byte])])](connection,
          tableName,
          myObj,
          (putRecord) => {
            val put = new Put(putRecord._1)
            putRecord._2.foreach((putValue) => put.add(putValue._1, putValue._2, putValue._3))
            put
          },
          false);
	}
}