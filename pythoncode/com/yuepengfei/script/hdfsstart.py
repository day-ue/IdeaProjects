#!/usr/bin/python
import os
os.system("service hadoop-hdfs-namenode start")
os.system("service hadoop-hdfs-datanode start")
os.system("service hadoop-hdfs-secondarynamenode start")