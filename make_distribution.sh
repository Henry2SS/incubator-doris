#!/usr/bin/env bash
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

##############################################################
# This script is used to compile Apache Doris(incubating)
# Usage:
#    sh build.sh        build both Backend and Frontend.
#    sh build.sh -clean clean previous output and build.
#
# You need to make sure all thirdparty libraries have been
# compiled and installed correctly.
##############################################################

set -eo pipefail
ROOT=`dirname "$0"`
ROOT=`cd "$ROOT"; pwd`
export DORIS_HOME=${ROOT}

DORIS_OUTPUT=${DORIS_HOME}/output
rm -rf ${DORIS_OUTPUT}
rm -rf ${DORIS_HOME}/output-${build_version}

USE_JEMALLOC=1 WITH_MYSQL=1 WITH_LZO=1 sh build.sh
BROKER_DIR=${DORIS_HOME}/fs_brokers/apache_hdfs_broker/
rm -rf ${BROKER_DIR}/output
sh ${BROKER_DIR}/build.sh
cp -r ${BROKER_DIR}/output/apache_hdfs_broker ${DORIS_OUTPUT}/.
build_version=`grep build_version ${DORIS_HOME}/gensrc/script/gen_build_version.sh | head -n 1 | cut -d = -f 2 | awk -F "[\"\"]" '{print $2}'`
mv ${DORIS_OUTPUT} ${DORIS_HOME}/output-${build_version}
rm -rf ${DORIS_HOME}/EASY-Doris-${build_version}.tar
cd ${DORIS_HOME}
tar -czvf EASY-Doris-${build_version}.tar output-${build_version}

echo "***************************************"
echo "Successfully package Doris"
echo "***************************************"
