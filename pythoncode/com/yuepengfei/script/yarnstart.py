#!/usr/bin/python
import os
os.system("service hadoop-yarn-resourcemanager start")
os.system("service hadoop-yarn-nodemanager start")
os.system("service hadoop-mapreduce-historyserver start")