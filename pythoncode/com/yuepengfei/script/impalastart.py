#!/usr/bin/python
import os
os.system("service impala-state-store start")
os.system("service impala-catalog start")
os.system("service impala-server start")