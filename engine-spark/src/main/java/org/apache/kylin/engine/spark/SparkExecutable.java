/*
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
package org.apache.kylin.engine.spark;

import java.io.File;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.CliCommandExecutor;
import org.apache.kylin.job.common.PatternedLogger;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecuteResult;
import org.slf4j.LoggerFactory;

/**
 * 执行org.apache.kylin.common.util.SparkEntry类作为kylin的spark入口,里面具体执行具体的main class以及其他重要参数
 */
public class SparkExecutable extends AbstractExecutable {

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(SparkExecutable.class);

    private static final String CLASS_NAME = "className";//main class 全路径
    private static final String JARS = "jars";//执行的jar包路径

    public void setClassName(String className) {
        this.setParam(CLASS_NAME, className);
    }

    public void setJars(String jars) {
        this.setParam(JARS, jars);
    }

    //组装成执行的类
    private String formatArgs() {
        StringBuilder stringBuilder = new StringBuilder();
        for (Map.Entry<String, String> entry : getParams().entrySet()) {
            StringBuilder tmp = new StringBuilder();
            tmp.append("-").append(entry.getKey()).append(" ").append(entry.getValue()).append(" ");//添加参数-key value
            if (entry.getKey().equals(CLASS_NAME)) {//className要放在stringBuilder的最前面
                stringBuilder.insert(0, tmp);
            } else if (entry.getKey().equals(JARS)) {
                // JARS is for spark-submit, not for app
                continue;
            } else {
                stringBuilder.append(tmp);
            }
        }
        if (stringBuilder.length() > 0) {
            return stringBuilder.substring(0, stringBuilder.length() - 1).toString();
        } else {
            return StringUtils.EMPTY;
        }
    }

    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        final KylinConfig config = context.getConfig();
        if (KylinConfig.getSparkHome() == null) {
            throw new NullPointerException();
        }
        if (config.getKylinJobJarPath() == null) {
            throw new NullPointerException();
        }
        String jars = this.getParam(JARS);//获取jar包

        //获取hadoop、hive、hbase的配置文件路径
        //hadoop conf dir
        String hadoopConf = null;
        hadoopConf = System.getProperty("kylin.hadoop.conf.dir");

        if (StringUtils.isEmpty(hadoopConf)) {
            throw new RuntimeException("kylin_hadoop_conf_dir is empty, check if there's error in the output of 'kylin.sh start'");
        }

        File hiveConfFile = new File(hadoopConf, "hive-site.xml");
        if (!hiveConfFile.exists()) {
            throw new RuntimeException("Cannot find hive-site.xml in kylin_hadoop_conf_dir: " + hadoopConf + //
                    ". In order to enable spark cubing, you must set kylin.env.hadoop-conf-dir to a dir which contains at least core-site.xml, hdfs-site.xml, hive-site.xml, mapred-site.xml, yarn-site.xml");
        }
        logger.info("Using " + hadoopConf + " as HADOOP_CONF_DIR");

        //hbase-site.xml
        String hbaseConf = ClassLoader.getSystemClassLoader().getResource("hbase-site.xml").getFile().toString();
        logger.info("Get hbase-site.xml location from classpath: " + hbaseConf);
        File hbaseConfFile = new File(hbaseConf);
        if (hbaseConfFile.exists() == false) {
            throw new IllegalArgumentException("Couldn't find hbase-site.xml from classpath.");
        }

        String jobJar = config.getKylinJobJarPath();
        if (StringUtils.isEmpty(jars)) {
            jars = jobJar;
        }

        /**
         * 创建字符串内容
         * 1.export HADOOP_CONF_DIR= ${hadoopConf} && ${sparkHome}/bin/spark-submit --class org.apache.kylin.common.util.SparkEntry
         * 2.添加spark参数 --conf key=value --conf key=value
         * 3.添加提交的文件(hbase的配置文件)和jar包
         * --files ${hbaseConfFile.getAbsolutePath()} --jars ${jars} ${jobJar} ${执行main class的各种参数}
         */
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("export HADOOP_CONF_DIR=%s && %s/bin/spark-submit --class org.apache.kylin.common.util.SparkEntry ");

        Map<String, String> sparkConfs = config.getSparkConfigOverride();//获取spark的配置信息
        for (Map.Entry<String, String> entry : sparkConfs.entrySet()) {
            stringBuilder.append(" --conf ").append(entry.getKey()).append("=").append(entry.getValue()).append(" ");//组装spark的配置信息
        }

        stringBuilder.append("--files %s --jars %s %s %s");
        try {
            String cmd = String.format(stringBuilder.toString(), hadoopConf, KylinConfig.getSparkHome(), hbaseConfFile.getAbsolutePath(), jars, jobJar, formatArgs());
            logger.info("cmd: " + cmd);
            CliCommandExecutor exec = new CliCommandExecutor();
            PatternedLogger patternedLogger = new PatternedLogger(logger);
            exec.execute(cmd, patternedLogger);
            getManager().addJobInfo(getId(), patternedLogger.getInfo());
            return new ExecuteResult(ExecuteResult.State.SUCCEED, patternedLogger.getBufferedLog());
        } catch (Exception e) {
            logger.error("error run spark job:", e);
            return new ExecuteResult(ExecuteResult.State.ERROR, e.getLocalizedMessage());
        }
    }


}
