#!/bin/bash
spark-submit --driver-class-path /jdbc/ojdbc8.jar --jars /jdbc/ojdbc8.jar --master spark://spark-node-master:7077 submit.py
