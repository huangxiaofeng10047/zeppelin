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
 */
package org.apache.zeppelin.rest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.interpreter.InterpreterSetting;
import org.apache.zeppelin.notebook.Note;
import org.apache.zeppelin.notebook.Paragraph;
import org.apache.zeppelin.scheduler.Job.Status;
import org.apache.zeppelin.server.ZeppelinServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.gson.Gson;

/**
 * Test against spark cluster.
 * Spark cluster is started by CI server using testing/startSparkCluster.sh
 */
public class ZeppelinSparkClusterTest extends AbstractTestRestApi {
    Gson gson = new Gson();

    @BeforeClass
    public static void init() throws Exception {
        AbstractTestRestApi.startUp();
    }

    @AfterClass
    public static void destroy() throws Exception {
        AbstractTestRestApi.shutDown();
    }

    private void waitForFinish(Paragraph p) {
        while (p.getStatus() != Status.FINISHED
                && p.getStatus() != Status.ERROR
                && p.getStatus() != Status.ABORT) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                LOG.error("Exception in WebDriverManager while getWebDriver ", e);
            }
        }
    }

    @Test
    public void basicRDDTransformationAndActionTest() throws IOException {
        // create new note
        Note note = ZeppelinServer.notebook.createNote(null);

        // run markdown paragraph, again
        Paragraph p = note.addParagraph();
        Map config = p.getConfig();
        config.put("enabled", true);
        p.setConfig(config);
        p.setText("%spark print(sc.parallelize(1 to 10).reduce(_ + _))");
        note.run(p.getId());
        waitForFinish(p);
        assertEquals(Status.FINISHED, p.getStatus());
        assertEquals("55", p.getResult().message());
        ZeppelinServer.notebook.removeNote(note.getId(), null);
    }

    @Test
    public void sparkSQLTest() throws IOException {
        // create new note
        Note note = ZeppelinServer.notebook.createNote(null);
        int sparkVersion = getSparkVersionNumber(note);
        // DataFrame API is available from spark 1.3
        if (sparkVersion >= 13) {
            // test basic dataframe api
            Paragraph p = note.addParagraph();
            Map config = p.getConfig();
            config.put("enabled", true);
            p.setConfig(config);
            p.setText("%spark val df=sqlContext.createDataFrame(Seq((\"hello\",20)))\n" +
                    "df.collect()");
            note.run(p.getId());
            waitForFinish(p);
            assertEquals(Status.FINISHED, p.getStatus());
            assertTrue(p.getResult().message().contains(
                    "Array[org.apache.spark.sql.Row] = Array([hello,20])"));

            // test display DataFrame
            p = note.addParagraph();
            config = p.getConfig();
            config.put("enabled", true);
            p.setConfig(config);
            p.setText("%spark val df=sqlContext.createDataFrame(Seq((\"hello\",20)))\n" +
                    "z.show(df)");
            note.run(p.getId());
            waitForFinish(p);
            assertEquals(Status.FINISHED, p.getStatus());
            assertEquals(InterpreterResult.Type.TABLE, p.getResult().type());
            assertEquals("_1\t_2\nhello\t20\n", p.getResult().message());

            // test display DataSet
            if (sparkVersion >= 20) {
                p = note.addParagraph();
                config = p.getConfig();
                config.put("enabled", true);
                p.setConfig(config);
                p.setText("%spark val ds=spark.createDataset(Seq((\"hello\",20)))\n" +
                        "z.show(ds)");
                note.run(p.getId());
                waitForFinish(p);
                assertEquals(Status.FINISHED, p.getStatus());
                assertEquals(InterpreterResult.Type.TABLE, p.getResult().type());
                assertEquals("_1\t_2\nhello\t20\n", p.getResult().message());
            }
            ZeppelinServer.notebook.removeNote(note.getId(), null);
        }
    }

    @Test
    public void sparkRTest() throws IOException {
      // create new note
      Note note = ZeppelinServer.notebook.createNote(null);
      int sparkVersion = getSparkVersionNumber(note);

      if (isSparkR() && sparkVersion >= 14) {   // sparkr supported from 1.4.0
        // restart spark interpreter
        List<InterpreterSetting> settings =
          ZeppelinServer.notebook.getBindedInterpreterSettings(note.getId());

        for (InterpreterSetting setting : settings) {
          if (setting.getName().equals("spark")) {
            ZeppelinServer.notebook.getInterpreterFactory().restart(setting.getId());
            break;
          }
        }

        String sqlContextName = "sqlContext";
        if (sparkVersion >= 20) {
          sqlContextName = "spark";
        }
        Paragraph p = note.addParagraph();
        Map config = p.getConfig();
        config.put("enabled", true);
        p.setConfig(config);
        p.setText("%r localDF <- data.frame(name=c(\"a\", \"b\", \"c\"), age=c(19, 23, 18))\n" +
            "df <- createDataFrame(" + sqlContextName + ", localDF)\n" +
            "count(df)"
        );
        note.run(p.getId());
        waitForFinish(p);
        System.err.println("sparkRTest=" + p.getResult().message());
        assertEquals(Status.FINISHED, p.getStatus());
        assertEquals("[1] 3", p.getResult().message());
      }
      ZeppelinServer.notebook.removeNote(note.getId(), null);
    }

    @Test
    public void pySparkTest() throws IOException {
        // create new note
        Note note = ZeppelinServer.notebook.createNote(null);
        note.setName("note");
        int sparkVersion = getSparkVersionNumber(note);

        if (isPyspark() && sparkVersion >= 12) {   // pyspark supported from 1.2.1
            // run markdown paragraph, again
            Paragraph p = note.addParagraph();
            Map config = p.getConfig();
            config.put("enabled", true);
            p.setConfig(config);
            p.setText("%pyspark print(sc.parallelize(range(1, 11)).reduce(lambda a, b: a + b))");
            note.run(p.getId());
            waitForFinish(p);
            assertEquals(Status.FINISHED, p.getStatus());
            assertEquals("55\n", p.getResult().message());
            if (sparkVersion >= 13) {
                // run sqlContext test
                p = note.addParagraph();
                config = p.getConfig();
                config.put("enabled", true);
                p.setConfig(config);
                p.setText("%pyspark from pyspark.sql import Row\n" +
                        "df=sqlContext.createDataFrame([Row(id=1, age=20)])\n" +
                        "df.collect()");
                note.run(p.getId());
                waitForFinish(p);
                assertEquals(Status.FINISHED, p.getStatus());
                assertEquals("[Row(age=20, id=1)]\n", p.getResult().message());

                // test display Dataframe
                p = note.addParagraph();
                config = p.getConfig();
                config.put("enabled", true);
                p.setConfig(config);
                p.setText("%pyspark from pyspark.sql import Row\n" +
                        "df=sqlContext.createDataFrame([Row(id=1, age=20)])\n" +
                        "z.show(df)");
                note.run(p.getId());
                waitForFinish(p);
                assertEquals(Status.FINISHED, p.getStatus());
                assertEquals(InterpreterResult.Type.TABLE, p.getResult().type());
                // TODO (zjffdu), one more \n is appended, need to investigate why.
                assertEquals("age\tid\n20\t1\n\n", p.getResult().message());

                // test udf
                p = note.addParagraph();
                config = p.getConfig();
                config.put("enabled", true);
                p.setConfig(config);
                p.setText("%pyspark sqlContext.udf.register(\"f1\", lambda x: len(x))\n" +
                       "sqlContext.sql(\"select f1(\\\"abc\\\") as len\").collect()");
                note.run(p.getId());
                waitForFinish(p);
                assertEquals(Status.FINISHED, p.getStatus());
                assertEquals("[Row(len=u'3')]\n", p.getResult().message());
            }
            if (sparkVersion >= 20) {
                // run SparkSession test
                p = note.addParagraph();
                config = p.getConfig();
                config.put("enabled", true);
                p.setConfig(config);
                p.setText("%pyspark from pyspark.sql import Row\n" +
                        "df=sqlContext.createDataFrame([Row(id=1, age=20)])\n" +
                        "df.collect()");
                note.run(p.getId());
                waitForFinish(p);
                assertEquals(Status.FINISHED, p.getStatus());
                assertEquals("[Row(age=20, id=1)]\n", p.getResult().message());

                // test udf
                p = note.addParagraph();
                config = p.getConfig();
                config.put("enabled", true);
                p.setConfig(config);
                // use SQLContext to register UDF but use this UDF through SparkSession
                p.setText("%pyspark sqlContext.udf.register(\"f1\", lambda x: len(x))\n" +
                        "spark.sql(\"select f1(\\\"abc\\\")\").collect()");
                note.run(p.getId());
                waitForFinish(p);
                assertEquals(Status.FINISHED, p.getStatus());
                assertEquals("[Row(f1(abc)=u'3')]\n", p.getResult().message());
            }
        }
        ZeppelinServer.notebook.removeNote(note.getId(), null);
    }

    @Test
    public void pySparkAutoConvertOptionTest() throws IOException {
        // create new note
        Note note = ZeppelinServer.notebook.createNote(null);
        note.setName("note");

        int sparkVersionNumber = getSparkVersionNumber(note);

        if (isPyspark() && sparkVersionNumber >= 14) {   // auto_convert enabled from spark 1.4
            // run markdown paragraph, again
            Paragraph p = note.addParagraph();
            Map config = p.getConfig();
            config.put("enabled", true);
            p.setConfig(config);

            String sqlContextName = "sqlContext";
            if (sparkVersionNumber >= 20) {
                sqlContextName = "spark";
            }

            p.setText("%pyspark\nfrom pyspark.sql.functions import *\n"
                    + "print(" + sqlContextName + ".range(0, 10).withColumn('uniform', rand(seed=10) * 3.14).count())");
            note.run(p.getId());
            waitForFinish(p);
            assertEquals(Status.FINISHED, p.getStatus());
            assertEquals("10\n", p.getResult().message());
        }
        ZeppelinServer.notebook.removeNote(note.getId(), null);
    }

    @Test
    public void zRunTest() throws IOException {
        // create new note
        Note note = ZeppelinServer.notebook.createNote(null);
        Paragraph p0 = note.addParagraph();
        Map config0 = p0.getConfig();
        config0.put("enabled", true);
        p0.setConfig(config0);
        p0.setText("%spark z.run(1)");
        Paragraph p1 = note.addParagraph();
        Map config1 = p1.getConfig();
        config1.put("enabled", true);
        p1.setConfig(config1);
        p1.setText("%spark val a=10");
        Paragraph p2 = note.addParagraph();
        Map config2 = p2.getConfig();
        config2.put("enabled", true);
        p2.setConfig(config2);
        p2.setText("%spark print(a)");

        note.run(p0.getId());
        waitForFinish(p0);
        assertEquals(Status.FINISHED, p0.getStatus());

        note.run(p2.getId());
        waitForFinish(p2);
        assertEquals(Status.FINISHED, p2.getStatus());
        assertEquals("10", p2.getResult().message());

        ZeppelinServer.notebook.removeNote(note.getId(), null);
    }

    @Test
    public void pySparkDepLoaderTest() throws IOException {
        // create new note
        Note note = ZeppelinServer.notebook.createNote(null);
        int sparkVersionNumber = getSparkVersionNumber(note);

        if (isPyspark() && sparkVersionNumber >= 14) {
            // restart spark interpreter
            List<InterpreterSetting> settings =
                    ZeppelinServer.notebook.getBindedInterpreterSettings(note.getId());

            for (InterpreterSetting setting : settings) {
                if (setting.getName().equals("spark")) {
                    ZeppelinServer.notebook.getInterpreterFactory().restart(setting.getId());
                    break;
                }
            }

            // load dep
            Paragraph p0 = note.addParagraph();
            Map config = p0.getConfig();
            config.put("enabled", true);
            p0.setConfig(config);
            p0.setText("%dep z.load(\"com.databricks:spark-csv_2.11:1.2.0\")");
            note.run(p0.getId());
            waitForFinish(p0);
            assertEquals(Status.FINISHED, p0.getStatus());

            // write test csv file
            File tmpFile = File.createTempFile("test", "csv");
            FileUtils.write(tmpFile, "a,b\n1,2");

            // load data using libraries from dep loader
            Paragraph p1 = note.addParagraph();
            p1.setConfig(config);

            String sqlContextName = "sqlContext";
            if (sparkVersionNumber >= 20) {
                sqlContextName = "spark";
            }
            p1.setText("%pyspark\n" +
                    "from pyspark.sql import SQLContext\n" +
                    "print(" + sqlContextName + ".read.format('com.databricks.spark.csv')" +
                    ".load('"+ tmpFile.getAbsolutePath() +"').count())");
            note.run(p1.getId());

            waitForFinish(p1);
            assertEquals(Status.FINISHED, p1.getStatus());
            assertEquals("2\n", p1.getResult().message());
        }
        ZeppelinServer.notebook.removeNote(note.getId(), null);
    }

    /**
     * Get spark version number as a numerical value.
     * eg. 1.1.x => 11, 1.2.x => 12, 1.3.x => 13 ...
     */
    private int getSparkVersionNumber(Note note) {
        Paragraph p = note.addParagraph();
        note.setName("note");
        Map config = p.getConfig();
        config.put("enabled", true);
        p.setConfig(config);
        p.setText("%spark print(sc.version)");
        note.run(p.getId());
        waitForFinish(p);
        assertEquals(Status.FINISHED, p.getStatus());
        String sparkVersion = p.getResult().message();
        System.out.println("Spark version detected " + sparkVersion);
        String[] split = sparkVersion.split("\\.");
        int version = Integer.parseInt(split[0]) * 10 + Integer.parseInt(split[1]);
        return version;
    }
}
