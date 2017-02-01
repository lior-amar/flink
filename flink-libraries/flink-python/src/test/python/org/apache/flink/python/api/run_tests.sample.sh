#!/bin/bash
# ###############################################################################
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
################################################################################

SCRIPT_NAME=`basename ${BASH_SOURCE[0]}`
SCRIPT_PATH=`dirname ${BASH_SOURCE[0]}`

USAGE="Usage: $SCRIPT_NAME <python_test_file> [arg1 [arg2 .. argn]]"

if [ "$#" -lt 1 ]; then
	echo "$USAGE"
	exit 1
fi

# Set this variable to point to the flink python src dir
FLINK_PYTHON_ROOT=$SCRIPT_PATH/../../../../../../..

# The PYTHONPATH that will be used
PYTHONPATH=$PYTHONPATH:$FLINK_PYTHON_ROOT/main/python/org/apache/flink/python/api/
PYTHONPATH=$PYTHONPATH:$FLINK_PYTHON_ROOT/test/python/org/apache/flink/python/api


# The PYTHONPATH that will be used
PYTHONPATH="$FLINK_PYTHON_ROOT/main/python/org/apache/flink/python/api/:$FLINK_PYTHON_ROOT/test/python/org/apache/flink/python/api"

# Set this variable to point to the PYFLINK command to use
PYFLINK="/net/home/lior/src/flink/flink/flink-dist/target/flink-1.3-SNAPSHOT-bin/flink-1.3-SNAPSHOT/bin/pyflink2.sh"

TEST_FILE=$1
shift

env PYTHONPATH=$PYTHONPATH python2 $TEST_FILE  --pyfl $PYFLINK $@
 
