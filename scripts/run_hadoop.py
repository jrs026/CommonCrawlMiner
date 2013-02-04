#!/usr/bin/python

# run_hadoop.py:
# 
# A script to assist in running Hadoop jobs locally or on Amazon's Elastic
# MapReduce. In order to run Hadoop locally, you must have a pseudo-distributed 
# Hadoop cluster running on your local machine. To run jobs on Amazon, you must
# have:
#
#   aws - A command line tool for accessing Amazon Web Services:
#     http://timkay.com/aws/
#
#   elastic-mapreduce - An Elastic MapReduce Ruby client:
#     http://aws.amazon.com/developertools/2264
#
#   ~/.awssecret - A plain text file with your AWS access key and secret key in
#   plain text, also needed by the "aws" tool.
# 
# This script was adapted from bin/ccRunExample from the commoncrawl-examples
# library:
#   https://github.com/commoncrawl/commoncrawl-examples

import optparse
import os
import re
import sys
import time

def main():
  user = os.environ["USER"]
  home_dir = os.environ["HOME"]

  # Setting up some of the old defaults
  current_dir = os.getcwd()
  local_jar_path = current_dir + "/jar/ccparc.jar";
  local_output_path = current_dir + "/output/result.tsv.gz"

  parser = optparse.OptionParser()
  parser.add_option("--use-aws", dest="use_aws", default=False,
      action="store_true",
      help="Execute a job on Elastic MapReduce instead of running locally")
  parser.add_option("--dryrun", dest="dryrun", default=False,
      action="store_true",
      help="If true, the script will only print commands, and not execute them")
  parser.add_option("-j", "--jar", dest="jar_location", default=local_jar_path,
      help="Location of the .jar file to be executed")

  parser.add_option("-b", "--bucket", dest="s3_user_bucket",
      default="jrsmith-commoncrawl-test",
      help="Name of the Amazon S3 bucket where data (.jar, logs and output) will be stored")
  parser.add_option("--emr-jar-path", dest="emr_jar_path", default="/emr/jars",
      help="S3 location where the .jar file will be stored")
  parser.add_option("--emr-log-path", dest="emr_log_path", default="/emr/logs",
      help="S3 location where the logs will be stored")
  parser.add_option("--emr-output-path", dest="emr_output_path",
      default="/emr/output",
      help="S3 location where output will be stored")

  parser.add_option("--emr-input-path", dest="emr_input_path",
      default="aws-publicdatasets/common-crawl/crawl-002/2010/01/06/*/*.arc.gz",
      help="File pattern for the input data. You must specify the S3 bucket in the address")

  parser.add_option("--job-name", dest="job_name", default="MiningTest",
      help="A descriptive name of the job")

  parser.add_option("--local_output", dest="local_output",
      default=local_output_path,
      help="Location on the local filesystem where output will be stored")

  # Elastic Map-reduce options
  parser.add_option("--master-type", dest="master_type", default="m1.large",
      help="Instance type for the master node")
  parser.add_option("--core-type", dest="core_type", default="m1.large",
      help="Instance type for the core node")
  #parser.add_option("--task-type", dest="task_type", default="c1.xlarge",
  #    help="Instance type for the task nodes") # m2.2xlarge
  parser.add_option("--instances", dest="instances", default="4", type="string",
      help="Number of instances to reserve")

  (opts, args) = parser.parse_args()
  last_slash = opts.jar_location.rfind("/")
  jar_name = opts.jar_location[last_slash+1:]
  timestamp = str(time.time())
  job_name = opts.job_name + "__" + timestamp

  full_jar_path = opts.s3_user_bucket + opts.emr_jar_path
  full_log_path = opts.s3_user_bucket + opts.emr_log_path
  full_output_path = opts.s3_user_bucket + opts.emr_output_path + "/" + job_name

  if not opts.use_aws:
    hdfs_output_path = "hdfs://localhost/user/" + user + "/output/" + opts.job_name
    hdfs_input_path = "hdfs://localhost/user/" + user + "/input/*"
    local_output_path = current_dir + "/output/" + opts.job_name + ".tsv"
    exec_str = "hadoop jar %s" % opts.jar_location
    exec_str += " %s %s %s/conf/mapred.xml" % (hdfs_input_path, hdfs_output_path, current_dir)
    print_exec(exec_str, opts.dryrun)
    retrieve_data_str = "hadoop fs -getmerge %s %s" % (hdfs_output_path, opts.local_output)
    print_exec(retrieve_data_str, opts.dryrun)
  else:
    aws_file = home_dir + "/.awssecret"
    [access_key, secret_key] = [line.strip() for line in open(aws_file, 'r')]
    put_data_str = "aws put %s/%s %s" % (full_jar_path, jar_name, opts.jar_location)
    print_exec(put_data_str, opts.dryrun)
    exec_str = "elastic-mapreduce --create --plain-output --name " + job_name
    exec_str += " --ami-version=\"latest\" --hadoop-version=\"1.0.3\""
    exec_str += " --jar \"s3n://%s/%s\" --step-name \"Run_%s\"" % (full_jar_path, jar_name, opts.job_name)
    exec_str += " --log-uri \"s3n://%s\"" % (full_log_path)
    exec_str += " --access-id \"%s\" --private-key \"%s\"" % (access_key, secret_key)
    exec_str += " --arg \"-Dmapreduce.job.split.metainfo.maxsize=-1\""
    exec_str += " --arg \"-Dmapred.max.map.failures.percent=10\""
    exec_str += " --arg \"-Dmapred.max.reduce.failures.percent=10\""
    # TODO : These options would enable a workaround to the OutOfMemory bug, but
    # would force shuffling to be on disk.
    exec_str += " --arg \"s3n://" + opts.emr_input_path + "\""
    exec_str += " --arg \"s3n://" + full_output_path + "\""
    exec_str += " --instance-group master --instance-type \"%s\" --instance-count 1" % (opts.master_type)
    #exec_str += " --instance-group task   --instance-type \"%s\"" % (opts.task_type)
    exec_str += " --instance-group core --instance-type \"%s\"" % (opts.core_type)
    exec_str += " --instance-count %s" % (opts.instances)
    #exec_str += " --bootstrap-action s3://elasticmapreduce/bootstrap-actions/configurations/latest/memory-intensive"
    print_exec(exec_str, opts.dryrun)

def print_exec(str, dryrun):
  print str
  if (not dryrun):
    os.system(str)

if __name__ == '__main__':
  main()
