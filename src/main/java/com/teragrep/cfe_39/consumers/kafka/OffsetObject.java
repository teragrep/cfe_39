/*
   HDFS Data Ingestion for PTH_06 use CFE-39
   Copyright (C) 2022  Fail-Safe IT Solutions Oy

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */
package com.teragrep.cfe_39.consumers.kafka;

public abstract class OffsetObject {
    protected String topic;
    protected Integer partition;
    protected Long offset;
    protected byte[] record;
    public abstract boolean isNull();
    public abstract String getTopic();
    public abstract Integer getPartition();
    public abstract Long getOffset();
    public abstract byte[] getRecord();
}
