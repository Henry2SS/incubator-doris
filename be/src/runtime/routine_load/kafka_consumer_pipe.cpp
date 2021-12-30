// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "runtime/routine_load/kafka_consumer_pipe.h"
#include "rapidjson/rapidjson.h"
#include "rapidjson/document.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"
#include "avro/Decoder.hh"
#include "avro/Generic.hh"
#include "avro/Specific.hh"
#include "avro/jdwdata.hh"

namespace doris {
// now avro json schema is hard code
Status KafkaConsumerPipe::append_avro_bytes(const char* data, size_t size) {
    std::unique_ptr<avro::InputStream> json_in = avro::memoryInputStream(reinterpret_cast<const uint8_t*>(data), size);
    avro::DecoderPtr d = avro::binaryDecoder();
    doris::JdwData jdw_data;
    try {
        d->init(*json_in);
        avro::decode(*d, jdw_data);
    } catch (avro::Exception& e) {
        return Status::InternalError(std::string("avro message deserialize error : ") + e.what());
    }
    return append_map(jdw_data);
}

Status KafkaConsumerPipe::append_map(const doris::JdwData& jdw_data) {
    rapidjson::Document document;
    rapidjson::Document::AllocatorType& allocator = document.GetAllocator();
    rapidjson::Value root(rapidjson::kObjectType);
    rapidjson::Value key(rapidjson::kStringType);
    rapidjson::Value value(rapidjson::kStringType);

    std::unordered_map<std::string, std::string> result_map;
    if (!jdw_data.src.is_null()) {
        const auto& src_map = jdw_data.src.get_map();
        for (const auto& it : src_map) {
            result_map[it.first.c_str()] = (it.second.is_null() ? "" : it.second.get_string());
        }
    }
    if (!jdw_data.cur.is_null()) {
        const auto& cur_map = jdw_data.cur.get_map();
        for (const auto& it : cur_map) {
            result_map[it.first.c_str()] = (it.second.is_null() ? "" : it.second.get_string());
        }
    }
    for (const auto& it : result_map) {
        key.SetString(it.first.c_str(), allocator);
        value.SetString(it.second.c_str(), allocator);
        root.AddMember(key, value, allocator);
    }
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    root.Accept(writer);
    return append_json(buffer.GetString(), buffer.GetSize());
}
} // namespace doris