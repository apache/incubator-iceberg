# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

FROM liaojiayidocker/hadoop-base:latest

ENV SPARK_VERSION=2.4.5
ENV SPARK_HADOOP_VERSION=2.7
ENV ICEBERG_VERSION=0.8.0

ARG SPARK_URL=https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${SPARK_HADOOP_VERSION}.tgz

# download spark
RUN set -x \
    && curl -fSL ${SPARK_URL} -o /tmp/spark.tar.gz \
    && tar -xvf /tmp/spark.tar.gz -C /opt/ \
    && rm /tmp/spark.tar.gz \
    && ln -s /opt/spark-${SPARK_VERSION}-bin-hadoop${SPARK_HADOOP_VERSION} /opt/spark


# download iceberg-spark-runtime
ARG ICEBERG_SPARK_URL=https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime/${ICEBERG_VERSION}-incubating/iceberg-spark-runtime-${ICEBERG_VERSION}-incubating.jar
RUN set -x \
    && curl -fSL ${ICEBERG_SPARK_URL} -o /opt/spark/jars/iceberg-spark-runtime-${ICEBERG_VERSION}-incubating.jar


ENV SPARK_HOME=/opt/spark
ENV PATH=${SPARK_HOME}/bin:${PATH}

EXPOSE 4040

RUN mkdir /opt/data
ADD logs.json /opt/data/logs.json

ADD run_spark.sh /opt/run_spark.sh
RUN chmod a+x /opt/run_spark.sh
CMD ["/opt/run_spark.sh"]
