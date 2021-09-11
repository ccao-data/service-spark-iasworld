# Multi-stage Dockerfile to build the dependencies required for Sqoop and Hive

##### CREATE MAIN HADOOP IMAGE #####

# Fresh base image 
FROM centos:7 AS hadoop
SHELL ["/bin/bash", "-euo", "pipefail", "-c"]

# Development tools
RUN yum install -y curl which tar bunzip2 openssh-server openssh-clients rsync && \
    yum clean all && \
    yum update -y libselinux

# JDK8
ARG JAVA_VER=1.8.0
ENV JAVA_HOME /usr/lib/jvm/java-"$JAVA_VER"-openjdk
ENV PATH "$PATH":"$JAVA_HOME"/bin
RUN yum install -y java-"$JAVA_VER"-openjdk-devel && \
    yum clean all

# Hadoop 
ARG HADOOP_VER=3.1.0
ENV HADOOP_HOME /usr/local/hadoop
ENV HDFS_NAMENODE_USER root
ENV HDFS_DATANODE_USER root
ENV HDFS_SECONDARYNAMENODE_USER root
ENV YARN_RESOURCEMANAGER_USER root
ENV YARN_NODEMANAGER_USER root
ENV HADOOP_COMMON_HOME "$HADOOP_HOME"
ENV HADOOP_HDFS_HOME "$HADOOP_HOME"
ENV HADOOP_MAPRED_HOME "$HADOOP_HOME"
ENV HADOOP_YARN_HOME "$HADOOP_HOME"
ENV HADOOP_CONF_DIR "$HADOOP_HOME"/etc/hadoop
ENV PATH "$PATH":"$HADOOP_HOME"/bin
RUN curl -s https://archive.apache.org/dist/hadoop/common/hadoop-"$HADOOP_VER"/hadoop-"$HADOOP_VER".tar.gz \
    | tar -xz -C /usr/local/ && \
    cd /usr/local && \
    ln -s ./hadoop-"$HADOOP_VER" hadoop && \
    echo "JAVA_HOME=${JAVA_HOME}" >> "$HADOOP_HOME"/etc/hadoop/hadoop-env.sh && \
    echo "HADOOP_HOME=${HADOOP_HOME}" >> "$HADOOP_HOME"/etc/hadoop/hadoop-env.sh

# Copy config files. Initially run in pseudo-distributed mode
# for namenode setup/formatting
RUN mkdir "$HADOOP_HOME"/input && \
    cp "$HADOOP_HOME"/etc/hadoop/*.xml "$HADOOP_HOME"/input
COPY docker-config/site/ "$HADOOP_HOME"/etc/hadoop/
RUN sed s/HOSTNAME/localhost/ \
    "$HADOOP_HOME"/etc/hadoop/core-site.xml.template \
    > "$HADOOP_HOME"/etc/hadoop/core-site.xml && \
    sed s/HOSTNAME/localhost/ \
    "$HADOOP_HOME"/etc/hadoop/yarn-site.xml.template \
    > /usr/local/hadoop/etc/hadoop/yarn-site.xml && \
    sed s/HOSTNAME/localhost/ \
    "$HADOOP_HOME"/etc/hadoop/mapred-site.xml.template \
    > /usr/local/hadoop/etc/hadoop/mapred-site.xml

# ssh setup for node comms
COPY docker-config/ssh_config /root/.ssh/config
RUN chmod 600 /root/.ssh/config && \
    chown root:root /root/.ssh/config && \
    ssh-keygen -q -N "" -t rsa -f /etc/ssh/ssh_host_rsa_key && \
    ssh-keygen -q -N "" -t dsa -f /etc/ssh/ssh_host_dsa_key && \
    ssh-keygen -q -N "" -t ecdsa -f /etc/ssh/ssh_host_ecdsa_key && \
    ssh-keygen -q -N "" -t ed25519 -f /etc/ssh/ssh_host_ed25519_key && \
    ssh-keygen -q -N "" -t ed25519 -f /root/.ssh/id_ed25519 && \
    cp /root/.ssh/id_ed25519.pub /root/.ssh/authorized_keys

# Fix 254 error code
RUN sed -i "/^[^#]*UsePAM/ s/.*/#&/" /etc/ssh/sshd_config && \
    echo "UsePAM no" >> /etc/ssh/sshd_config && \
    echo "Port 2122" >> /etc/ssh/sshd_config

# Add docker startup files and initialize/format namenode
COPY docker-config/init.sh /etc/docker-config/init.sh
RUN chown -R root:root /etc/docker-config/ && \
    chmod -R 700 /etc/docker-config/ && \
    /etc/docker-config/init.sh


##### CREATE MAIN SQOOP IMAGE #####

# Use hadoop image as base 
FROM hadoop AS sqoop

# Sqoop
ARG SQOOP_VER=1.4.7
ARG SQOOP_HADOOP_VER=2.6.0
ENV SQOOP_HOME /usr/local/sqoop
ENV SQOOP_CONF_DIR "$SQOOP_HOME"/conf
ENV PATH "$PATH":"$HADOOP_HOME"/bin:"$SQOOP_HOME"/bin
RUN curl -s https://archive.apache.org/dist/sqoop/"$SQOOP_VER"/sqoop-"$SQOOP_VER".bin__hadoop-"$SQOOP_HADOOP_VER".tar.gz \
    | tar -xz -C /usr/local && \
    ln -s /usr/local/sqoop-"$SQOOP_VER".bin__hadoop-"$SQOOP_HADOOP_VER" "$SQOOP_HOME"

# Make dirs for class files and jars
RUN mkdir -p /tmp/bindir /tmp/target && \
    chmod +x "$HADOOP_HOME"/etc/hadoop/hadoop-env.sh && \
    chown root:root "$HADOOP_HOME"/etc/hadoop/hadoop-env.sh

# Download and install Apache Hive
ARG HIVE_VER=3.1.2
ENV HIVE_HOME /usr/local/hive/apache-hive-"$HIVE_VER"-bin
ENV HIVE_CONF_DIR "$HIVE_HOME"/conf
ENV HCAT_HOME "$HIVE_HOME"/hcatalog
ENV PATH "$PATH":"$HIVE_HOME"/bin
ENV PATH "$PATH":"$HCAT_HOME"/bin
RUN mkdir -p /usr/local/hive && \
    curl -s https://dlcdn.apache.org/hive/hive-"$HIVE_VER"/apache-hive-"$HIVE_VER"-bin.tar.gz \
    | tar -xz -C /usr/local/hive && \
    rm "$HIVE_HOME"/lib/log4j-slf4j-impl-2.10.0.jar
COPY docker-config/hive/hive-site.xml "$HIVE_CONF_DIR"

# Install the postgresql connector driver
ARG PSQL_JDBC_VER=42.2.24
RUN curl -o /usr/share/java/postgresql-jdbc.jar https://jdbc.postgresql.org/download/postgresql-"$PSQL_JDBC_VER".jar && \
    chmod 644 /usr/share/java/postgresql-jdbc.jar && \
    ln -s /usr/share/java/postgresql-jdbc.jar "$HIVE_HOME"/lib/postgresql-jdbc.jar

# Entrypoint/startup for sqoop and hive
ADD https://repo1.maven.org/maven2/org/json/json/20090211/json-20090211.jar "$SQOOP_HOME"/lib
COPY docker-config/bootstrap.sh docker-config/entrypoint.sh /etc/docker-config/
RUN chown -R root:root /etc/docker-config/ && \
    chmod -R 755 /etc/docker-config/ && \
    chmod +x /etc/docker-config/entrypoint.sh

# Define script to run at startup
ENTRYPOINT ["/etc/docker-config/entrypoint.sh"]
