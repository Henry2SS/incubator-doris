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

#include <exec/avro_scanner.h>

#include <sstream>
#include <fstream>
#include <iostream>
#include "env/env.h"
#include "exec/broker_reader.h"
#include "exec/buffered_reader.h"
#include "exec/local_file_reader.h"
#include "exec/plain_binary_line_reader.h"
#include "exec/s3_reader.h"
#include "exprs/expr.h"
#include "exprs/json_functions.h"
#include "gutil/strings/split.h"
#include "runtime/exec_env.h"
#include "runtime/mem_tracker.h"
#include "runtime/runtime_state.h"
#include "common/config.h"

namespace doris {

void deserializeNoop(MemPool* , Tuple* , SlotDescriptor* , avro::Decoder & , int )
{
}

AvroScanner::AvroScanner(RuntimeState* state, RuntimeProfile* profile,
                         const TBrokerScanRangeParams& params,
                         const std::vector<TBrokerRangeDesc>& ranges,
                         const std::vector<TNetworkAddress>& broker_addresses,
                         const std::vector<TExpr>& pre_filter_texprs, ScannerCounter* counter)
    : BaseScanner(state, profile, params, pre_filter_texprs, counter),
      _ranges(ranges),
      _broker_addresses(broker_addresses),
      _cur_file_reader(nullptr),
      _cur_line_reader(nullptr),
      _cur_avro_reader(nullptr),
      _next_range(0),
      _cur_reader_eof(false),
      _scanner_eof(false) {
    if (params.__isset.line_delimiter_length && params.line_delimiter_length > 1) {
        _line_delimiter = params.line_delimiter_str;
        _line_delimiter_length = params.line_delimiter_length;
    } else {
        _line_delimiter.push_back(static_cast<char>(params.line_delimiter));
        _line_delimiter_length = 1;
    }      
}


AvroScanner::~AvroScanner() {
    close();
}

Status AvroScanner::open() {
    return BaseScanner::open();
}


// do decode in get_next;
Status AvroScanner::get_next(Tuple* tuple, MemPool* tuple_pool, bool* eof) {
    SCOPED_TIMER(_read_timer);

    // read one line
    while (!_scanner_eof) {
        if (_cur_file_reader == nullptr || _cur_reader_eof) {
            RETURN_IF_ERROR(open_next_reader());
             // If there isn't any more reader, break this
            if (_scanner_eof) {
                break;
            }
        }

        if (_skip_next_line) {
            size_t size = 0;
            const uint8_t* object_ptr = nullptr;
            RETURN_IF_ERROR(_cur_line_reader->read_line(&object_ptr, &size, &_cur_reader_eof));
            _skip_next_line = false;
            continue;
        }

        bool is_empty_row = false;

        RETURN_IF_ERROR(_cur_avro_reader->read_avro_row(_src_tuple, _src_slot_descs, tuple_pool,
                                                        &is_empty_row, &_cur_reader_eof));
        if (is_empty_row) {
            continue;
        }
        COUNTER_UPDATE(_rows_read_counter, 1);
        SCOPED_TIMER(_materialize_timer);
        if (fill_dest_tuple(tuple, tuple_pool)) {
            break; // break if true
        }
    }
    if (_scanner_eof) {
        *eof = true;
    } else {
        *eof = false;
    }
    WHZ_LOG << "_scanner_eof " << _scanner_eof << std::endl;
    return Status::OK();
}

void AvroScanner::close() {
    BaseScanner::close();
    if (_cur_avro_reader != nullptr) {
        delete _cur_avro_reader;
        _cur_avro_reader = nullptr;
    }
    if (_cur_line_reader != nullptr) {
        delete _cur_line_reader;
        _cur_line_reader = nullptr;
    }
    if (_cur_file_reader != nullptr) {
        if (_stream_load_pipe != nullptr) {
            _stream_load_pipe.reset();
        } else {
            delete _cur_file_reader;
        }
        _cur_file_reader = nullptr;
    }
}

Status AvroScanner::open_file_reader() {
    if (_cur_file_reader != nullptr) {
        if (_stream_load_pipe != nullptr) {
            _stream_load_pipe.reset();
            _cur_file_reader = nullptr;
        } else {
            delete _cur_file_reader;
            _cur_file_reader = nullptr;
        }
    }

    const TBrokerRangeDesc& range = _ranges[_next_range];
    //int64_t start_offset = range.start_offset;
    // if (range.__isset.read_avro_by_line) {
    //     _read_avro_by_line = range.read_avro_by_line;
    // }
    // current only support line reader, double check here.
    // if (!_read_avro_by_line) {
    //     return Status::InvalidArgument("Only support read avro by line. Set `read_avro_by_line` = \"true\".");
    // }

    switch (range.file_type) {
    
    // case TFileType::FILE_LOCAL: {
    //     WHZ_LOG << "range.path = " << range.path << std::endl;
    //     LocalFileReader* file_reader = new LocalFileReader(range.path, start_offset);
    //     RETURN_IF_ERROR(file_reader->open());
    //     _cur_file_reader = file_reader;
    //     break;
    // }
    // case TFileType::FILE_BROKER: {
    //     BrokerReader* broker_reader =
    //             new BrokerReader(_state->exec_env(), _broker_addresses, _params.properties,
    //                              range.path, start_offset);
    //     RETURN_IF_ERROR(broker_reader->open());
    //     _cur_file_reader = broker_reader;
    //     break;
    // }
    // case TFileType::FILE_S3: {
    //     BufferedReader* s3_reader =
    //             new BufferedReader(_profile, new S3Reader(_params.properties, range.path, start_offset));
    //     RETURN_IF_ERROR(s3_reader->open());
    //     _cur_file_reader = s3_reader;
    //     break;
    
    case TFileType::FILE_STREAM: {
        _stream_load_pipe = _state->exec_env()->load_stream_mgr()->get(range.load_id);
        if (_stream_load_pipe == nullptr) {
            VLOG_NOTICE << "unknown stream load id: " << UniqueId(range.load_id);
            return Status::InternalError("unknown stream load id");
        }
        _cur_file_reader = _stream_load_pipe.get();
        break;
    }
    case TFileType::FILE_LOCAL:
    case TFileType::FILE_BROKER:
    case TFileType::FILE_S3:
    default: {
        std::stringstream ss;
        ss << "Only support stream type. Unsupport file type, type=" << range.file_type;
        return Status::InternalError(ss.str());
    }
    }
    _cur_reader_eof = false;
    return Status::OK();
}


Status AvroScanner::open_line_reader() {
    if (_cur_line_reader != nullptr) {
        delete _cur_line_reader;
        _cur_line_reader = nullptr;
    }

    const TBrokerRangeDesc& range = _ranges[_next_range];
    int64_t size = range.size;
    if (range.start_offset != 0) {
        size += 1;
        _skip_next_line = true;
    } else {
        _skip_next_line = false;
    }
    // _cur_line_reader = new PlainBiaLineReader(_profile, _cur_file_reader, nullptr,
    //                                           size, _line_delimiter, _line_delimiter_length);
    _cur_line_reader = new PlainBinaryLineReader(_cur_file_reader);
    _cur_reader_eof = false;
    return Status::OK();
}


Status AvroScanner::open_avro_reader() {
    if (_cur_avro_reader != nullptr) {
        delete _cur_avro_reader;
        _cur_avro_reader = nullptr;
    }

    const TBrokerRangeDesc& range = _ranges[_next_range];
    if (range.start_offset != 0) {
        _skip_next_line = true;
    } else {
        _skip_next_line = false;
    }

    // std::string avropath = "";
    // std::string avro_root = "";

    // if (range.__isset.jsonpaths) {
    //     avropath = range.jsonpaths;
    // }
    // if (range.__isset.json_root) {
    //     //avro_root = range.json_root;
    //     return Status::InvalidArgument("json_root is not support currently, while using AVRO.");
    // }
    

    // if (_read_avro_by_line) {
    //     WHZ_LOG << "go _cur_line_reader" << std::endl;
    //     _cur_avro_reader =
    //             new AvroReader(_state, _counter, _profile, nullptr, _cur_line_reader);
    //} else {
        // current case never reached
    WHZ_LOG << "go _cur_file_reader" << std::endl;
    _cur_avro_reader = new AvroReader(_state, _counter, _profile, nullptr, _cur_line_reader);
    //}
    WHZ_LOG << "new AvroReader" << std::endl;
    // RETURN_IF_ERROR(_cur_avro_reader->init(avropath, avro_root));
    RETURN_IF_ERROR(_cur_avro_reader->init());
    return Status::OK();
}

Status AvroScanner::open_next_reader() {
    if (_next_range >= _ranges.size()) {
        _scanner_eof = true;
        return Status::OK();
    }

    RETURN_IF_ERROR(open_file_reader());
    RETURN_IF_ERROR(open_line_reader());
    RETURN_IF_ERROR(open_avro_reader());
    _next_range++;

    return Status::OK();
}




////// class AvroReader
AvroReader::AvroReader(RuntimeState* state, ScannerCounter* counter, RuntimeProfile* profile,
                       FileReader* file_reader, LineReader* line_reader) 
        : _next_line(0), 
          _total_lines(0),
          _state(state),
          _counter(counter),
          _profile(profile),
          _file_reader(file_reader),
          _line_reader(line_reader),
          _closed(false),
          _decoder(avro::binaryDecoder()),
          _in(nullptr) {
    _bytes_read_counter = ADD_COUNTER(_profile, "BytesRead", TUnit::BYTES);
    _read_timer = ADD_TIMER(_profile, "ReadTime");
    _file_read_timer = ADD_TIMER(_profile, "FileReadTime");
}

AvroReader::~AvroReader() {
    _close();
}

Status AvroReader::init() {
    //WHZ_LOG << avropath.empty() << std::endl;
    // if (!avropath.empty()) {
    //     Status st = _get_avro_paths(avropath, &_parsed_avropaths);
    //     RETURN_IF_ERROR(st);
    //     for (int i = 0; i < _parsed_avropaths.size(); i++) {
    //         for (int j = 0; j < _parsed_avropaths[i].size(); j++) {
    //             WHZ_TEST << _parsed_avropaths[i][j].to_string() << " ";
    //             WHZ_TEST << _parsed_avropaths[i][j].key << std::endl;
    //         }
    //         std::cout << " " << std::endl;
    //     }
    // } else {
    //     return Status::InvalidArgument("Please set json_path to specify the field.");
    // }
    // if (!avro_root.empty()) {
    //     JsonFunctions::parse_json_paths(avro_root, &_parsed_avro_root);
    // }

    // for(auto vect_row : _parsed_avropaths) {
    //     _print_avro_path(vect_row);
    // }
    // process avro_path
    //Status path_valid;
    //path_valid = _validate_and_generate_avro_path(&_parsed_avropaths);
    //RETURN_IF_ERROR(path_valid);
    // get avro schema

    // test
    bool exist = FileUtils::check_exist(config::avro_schema_file_path);
    //bool exist = FileUtils::check_exist("/tmp/jdolap/output/be/conf/avro_schema.json");
    if (!exist) {
        return Status::InternalError("there is no avro schema file at " + config::avro_schema_file_path + ". Please put an schema file in json format.");
    } else {
        //std::string s = "/tmp/jdolap/output/be/conf/avro_schema.json";
        try {
            _schema = avro::compileJsonSchemaFromFile(config::avro_schema_file_path.c_str());
            //_datum = avro::GenericDatum(_schema);
        } catch (avro::Exception &e) {
            return Status::InternalError(std::string("schema get from json failed.") + e.what());
        }
    }

    if (_schema.root()->type() != avro::AVRO_RECORD) {
        return Status::DataQualityError("Root schema must be a record");
    }
    
    // TODO: check if path is in shcema
    // path_valid = _validate_path_and_schema(&_schema, &_parsed_avropaths);
    // RETURN_IF_ERROR(path_valid);
    return Status::OK();
}

// Status AvroReader::_validate_path_and_schema(avro::ValidSchema* schema, std::vector<std::vector<JsonPath>>* vect) {
//     std::vector<std::string> fields_vect;
//     for (int i = 0; i < (*schema).root()->leaves(); i++) {
//         fields_vect.push_back((*schema).root()->nameAt(i));
//         WHZ_LOG << "field " << i << " : " << (*schema).root()->nameAt(i) << std::endl;
//     }
//     for (auto vect_row : *vect) {
//         if (std::find(fields_vect.begin(), fields_vect.end(), vect_row[1].to_string()) == fields_vect.end()) {
//             WHZ_LOG << "can't find path `" << vect_row[1].to_string() <<  "` in schema." << std::endl;
//             return Status::InvalidArgument("avro path `" + vect_row[1].to_string() + "` can't be found in schema.");
//         }
//     }
//     return Status::OK();
// }

// Status AvroReader::_validate_and_generate_avro_path(std::vector<std::vector<JsonPath>>* vect) {
//     for (auto vect_row : *vect) {
//         if (!vect_row[0].is_valid) {
//             return Status::InvalidArgument("avro path only support 2-level format. It should be like [\"$.key_1\", \"$.key_2\"]");
//         }
//         if (vect_row.size() != 2) {
//             return Status::InvalidArgument("avro path only support 2-level format. It should be like [\"$.key_1\", \"$.key_2\"]");
//         } 
//         if (vect_row[1].to_string() == "*") {
//             return Status::InvalidArgument("avro path only support 2-level format. It should be like [\"$.key_1\", \"$.key_2\"]");
//         } 
//     }
//     return Status::OK();
// }


// Status AvroReader::_get_avro_paths(const std::string& avropath,
//                            std::vector<std::vector<JsonPath>>* vect) {
//     rapidjson::Document avropaths_doc;
//     if (!avropaths_doc.Parse(avropath.c_str(), avropath.length()).HasParseError()) {
//         // TODO: should check if path is no more than 2-level
//         if (!avropaths_doc.IsArray()) {
//             return Status::InvalidArgument("Invalid avro path: " + avropath);
//         } else {
//             for (int i = 0; i < avropaths_doc.Size(); i++) {
//                 const rapidjson::Value& path = avropaths_doc[i];
//                 if (!path.IsString()) {
//                     return Status::InvalidArgument("Invalid avro path: " + avropath);
//                 }
//                 std::vector<JsonPath> parsed_paths;
//                 JsonFunctions::parse_json_paths(path.GetString(), &parsed_paths);
//                 vect->push_back(std::move(parsed_paths));
//             }
//             return Status::OK();
//         }
//     } else {
//         return Status::InvalidArgument("Invalid avro path: " + avropath);
//     }
// }

void AvroReader::_close() {
    if (_closed) {
        return;
    }
    _closed = true;
}

// std::string AvroReader::_print_avro_value(avro::NodePtr root_node) {
//     std::ostringstream ss;      // STYLE_CHECK_ALLOW_STD_STRING_STREAM
//     ss.exceptions(std::ios::failbit);
//     root_node->printJson(ss, 0);
//     return ss.str();
// }

// std::string AvroReader::_print_avro_path(const std::vector<JsonPath>& path) {
//     std::stringstream ss;
//     for (auto& p : path) {
//         ss << p.to_string() << ".";
//     }
//     return ss.str();
// }

// read one avro object from line reader or file reader, and decode it
Status AvroReader::_get_avro_doc(size_t* size, bool* eof, MemPool* tuple_pool,
                                   Tuple* tuple, const std::vector<SlotDescriptor*>& slot_descs) {
    SCOPED_TIMER(_file_read_timer);
    const uint8_t* avro_str = nullptr;
    std::unique_ptr<uint8_t[]> avro_str_ptr;

    if (_line_reader != nullptr) {
        WHZ_LOG << "_line_reader is not null " << std::endl;
        RETURN_IF_ERROR(_line_reader->read_line(&avro_str, size, eof));
    } else {
        WHZ_LOG << "_file_reader == nullptr " << (_file_reader == nullptr) << std::endl;
        int64_t length = 0;
        RETURN_IF_ERROR(_file_reader->read_one_message(&avro_str_ptr, &length));
        avro_str = avro_str_ptr.get();
        *size = length;
        if (length == 0) {
            *eof = true;
        }
    }
    
    _bytes_read_counter += *size;
    if (*eof) {
        return Status::OK();
    }
    WHZ_LOG << "length = " << *size << std::endl;

    WHZ_LOG << std::string("avro_str in _parse_avro_doc = ") << (const char *) avro_str << std::endl;
    
    // just allow one row in one object
    try {
        
        auto schema_root = _schema.root();
        //avro::DataFileReaderBase file_reader(avro::memoryInputStream(avro_str, *size));
        //WHZ_LOG << "file_reader.hasMore()" << file_reader.hasMore() << std::endl;
        _in = avro::memoryInputStream(avro_str, *size);
        _decoder->init(*_in);

    } catch  (avro::Exception &e) {
        return Status::DataQualityError(std::string("data quality is not good.") + e.what());
    }


        
        //_file_reader_ptr = std::make_unique<avro::DataFileReader<avro::GenericDatum>>(avro::memoryInputStream(avro_str, *size));
        
        // if (_file_reader_ptr.get()->read(_datum)) {
        //     // TODO : skip row here ?

        //     if (_datum.type() != avro::AVRO_RECORD) {
        //         return Status::DataQualityError("Root schema must be a record");
        //     }

        //     if (!_write_values_by_avropath(_datum, tuple_pool, tuple, slot_descs)) {
        //         _counter->num_rows_filtered++;
        //         return Status::DataQualityError("Data quality is not good.");
        //     }
        // }
    // } catch (avro::Exception &e) {
    //     WHZ_LOG << "data quality is not good." << std::endl;
    //     return Status::DataQualityError(std::string("data quality is not good.") + e.what());
    // }
    return Status::OK();
}

void AvroReader::_fill_slot(Tuple* tuple, SlotDescriptor* slot_desc, MemPool* mem_pool,
                            const uint8_t* value, int32_t len) {
    WHZ_LOG << "into fill slot" << std::endl;
    tuple->set_not_null(slot_desc->null_indicator_offset());
    void* slot = tuple->get_slot(slot_desc->tuple_offset());
    StringValue* str_slot = reinterpret_cast<StringValue*>(slot);
    // WHZ_LOG << str_slot->to_string() << std::endl;
    WHZ_LOG << "value = " << value << std::endl;
    str_slot->ptr = reinterpret_cast<char*>(mem_pool->allocate(len));
    WHZ_LOG << "is ok allocate " << std::endl;

    memcpy(str_slot->ptr, value, len);
    // WHZ_LOG << "str_slot->ptr " << str_slot->to_string() << std::endl;
    str_slot->len = len;
    // WHZ_LOG << "str_slot->len " << str_slot->len << std::endl;
}

// size_t AvroReader::_get_column_index(std::string path_name, const std::vector<SlotDescriptor*>& slot_descs) {
//     for (size_t i = 0; i < slot_descs.size(); i++) {
//         if (slot_descs[i]->col_name() == path_name) {
//             return i;
//         }
//     }
//     return -1;
// }

// bool AvroReader::_write_values_by_avropath(avro::GenericDatum datum, MemPool* tuple_pool,
//                                    Tuple* tuple, const std::vector<SlotDescriptor*>& slot_descs) {
//     WHZ_LOG << "into _write_values_by_avropath " << std::endl;
//     int nullcount = 0;
//     bool valid = true;
//     // uint8_t tmp_buf[128] = {0};
//     // int32_t wbytes = 0;
    
//     // avro_path should be different each one 
//     for (int i = 0; i < _parsed_avropaths.size(); i++) {
        
//         std::string path_name = _parsed_avropaths[i][1].to_string();
//         auto type = datum.value<avro::GenericRecord>().field(path_name).type();
//         avro::GenericDatum record_datum = datum.value<avro::GenericRecord>().field(path_name);

//         switch (type)
//         {
//         case avro::AVRO_LONG:
//         case avro::AVRO_INT:
//         case avro::AVRO_FLOAT:
//         case avro::AVRO_DOUBLE:
//         case avro::AVRO_BYTES:
//         case avro::AVRO_STRING:
//         case avro::AVRO_BOOL:
//             {
//                 valid = _process_simple_type(type, path_name, record_datum, nullcount, tuple_pool, tuple, slot_descs);
//             }
//             break;
//         case avro::AVRO_MAP:
//             {
//                 avro::GenericMap map = record_datum.value<avro::GenericMap>();
//                 auto map_type = map.schema()->leafAt(0)->type();
//                 for (auto pair : map.value()) {
//                     std::string map_key = pair.first;
//                     avro::GenericDatum map_datum = pair.second;
//                     for (size_t j = 0; j < slot_descs.size(); j++) {
//                         if (map_key == slot_descs[j]->col_name()) {
//                             valid = _process_simple_type(map_type, map_key, map_datum, nullcount, tuple_pool, tuple, slot_descs);
//                         }
//                     }
//                 }
//             }
//             break;
//         case avro::AVRO_ARRAY:
//             {
//                 WHZ_LOG << "Not supported " << type << " yet. Coming soon.";
//                 valid = false;
//             }
//             break;
//         default:
//             WHZ_LOG << "Not supported " << type << " yet. Coming soon.";
//             valid = false;
//             break;
//         }

//     }
//     if (nullcount == slot_descs.size()) {
//         WHZ_LOG << "empty row" << std::endl;
//         _counter->num_rows_filtered++;
//         valid = false;
//     }
//     return valid;

// }


// bool AvroReader::_process_simple_type(avro::Type type, std::string& path_name, avro::GenericDatum datum, int nullcount, MemPool* tuple_pool,
//                                       Tuple* tuple, const std::vector<SlotDescriptor*>& slot_descs) {
//     //int nullcount = 0;
//     bool valid = true;
//     uint8_t tmp_buf[128] = {0};
//     int32_t wbytes = 0;

//     size_t index_of_slot = _get_column_index(path_name, slot_descs);
//     WHZ_LOG << "into _process_simple_type type : " << type << std::endl;
//     WHZ_LOG << "into _process_simple_type index_of_slot : " << index_of_slot << std::endl;
//     switch (type)
//     {
//         case avro::AVRO_LONG:
//             {
//                 int64_t val_long = datum.value<int64_t>();
//                 if (index_of_slot == -1) {
//                     WHZ_LOG << "there is no such column named " << path_name << ". Please check the input of json_path." << std::endl; 
//                     valid = false;
//                 } else {
//                     wbytes = sprintf((char*)tmp_buf, "%ld", val_long);
//                      _fill_slot(tuple, slot_descs[index_of_slot], tuple_pool, tmp_buf, wbytes);
//                 }
//             }
//             break;
//         case avro::AVRO_INT:
//             {
//                 int32_t val_int = datum.value<int32_t>();
//                 if (index_of_slot == -1) {
//                     WHZ_LOG << "there is no such column named " << path_name << ". Please check the input of json_path." << std::endl; 
//                     valid = false;
//                 } else {
//                     wbytes = sprintf((char*)tmp_buf, "%d", val_int);
//                     _fill_slot(tuple, slot_descs[index_of_slot], tuple_pool, tmp_buf, wbytes);
//                 }
//             }
//             break;
//         case avro::AVRO_FLOAT:
//             {
//                 float val_float = datum.value<float>();
//                 if (index_of_slot == -1) {
//                     WHZ_LOG << "there is no such column named " << path_name << ". Please check the input of json_path." << std::endl; 
//                     valid = false;
//                 } else {
//                     WHZ_LOG << "val_float = " << val_float << std::endl;
//                     wbytes = sprintf((char*)tmp_buf, "%f", val_float);
//                     _fill_slot(tuple, slot_descs[index_of_slot], tuple_pool, tmp_buf, wbytes);
//                 }
//             }
//             break;
//         case avro::AVRO_DOUBLE:
//             {
//                 double val_double = datum.value<double>();
//                 if (index_of_slot == -1) {
//                     WHZ_LOG << "there is no such column named " << path_name << ". Please check the input of json_path." << std::endl; 
//                     valid = false;
//                 } else {
//                     WHZ_LOG << "val_double = " << val_double << std::endl;
//                     wbytes = sprintf((char*)tmp_buf, "%lf", val_double);
//                     _fill_slot(tuple, slot_descs[index_of_slot], tuple_pool, tmp_buf, wbytes);
//                 }
                
//             }
//             break;
//         case avro::AVRO_BYTES: [[fallthrough]];
//         case avro::AVRO_STRING:
//             {
//                 std::string val_string = datum.value<std::string>();
//                 if (index_of_slot == -1) {
//                     WHZ_LOG << "there is no such column named " << path_name << ". Please check the input of json_path." << std::endl;
//                     valid = false; 
//                 } else if (val_string.empty()) {
//                     if (slot_descs[index_of_slot]->is_nullable()) {
//                         tuple->set_null(slot_descs[index_of_slot]->null_indicator_offset());
//                         nullcount++;
//                     } else {
//                         WHZ_LOG << "column " << slot_descs[index_of_slot] << " is not nullable, but the value parsed from avro is null." << std::endl;
//                     }
//                 } else {
//                     _fill_slot(tuple, slot_descs[index_of_slot], tuple_pool, (uint8_t*)val_string.c_str(), strlen(val_string.c_str()));

//                 }   
//             }
//             break;
//         case avro::AVRO_BOOL:
//             {
//                 bool val_bool = datum.value<bool>();
//                 if (index_of_slot == -1) {
//                     WHZ_LOG << "there is no such column named " << path_name << ". Please check the input of json_path." << std::endl; 
//                     valid = false;
//                 } else {
//                     WHZ_LOG << "val_bool = " << val_bool << std::endl;
//                     wbytes = sprintf((char*)tmp_buf, "%d", val_bool);
//                     _fill_slot(tuple, slot_descs[index_of_slot], tuple_pool, tmp_buf, wbytes);
//                 }
                
//             }
//             break;
//     default:
//         WHZ_LOG << "not supported yet. Only support simple type or 2-level complex type.";
//         valid = false; // current row is invalid
//         break;
//     }
//     return valid;
// }

// using DeserializeFn = std::function<void(MemPool* tuple_pool,Tuple* tuple, const SlotDescriptor* slot_desc, avro::Decoder & decoder)>;

AvroReader::DeserializeFn AvroReader::createDeserializeFn(avro::NodePtr root_node, SlotDescriptor* slot_desc)
{
    //auto logical_type = root_node->logicalType().type();
    uint8_t tmp_buf[128] = {0};
    int32_t wbytes = 0;

    switch (root_node->type())
    {
        case avro::AVRO_BYTES: [[fallthrough]];
        case avro::AVRO_STRING:
            {
                return [val_string = std::string(), this](MemPool* tuple_pool, Tuple* tuple, SlotDescriptor* slot_desc, avro::Decoder & decoder, int nullcount) mutable {
                    decoder.decodeString(val_string);
                    if (val_string.empty()) {
                        if (slot_desc->is_nullable()) {
                            tuple->set_null(slot_desc->null_indicator_offset());
                            nullcount++;
                        } else {
                            throw avro::Exception("NULL data for non-nullable column" + slot_desc->col_name());
                        }
                    } else {
                        _fill_slot(tuple, slot_desc, tuple_pool, (uint8_t*)val_string.c_str(), strlen(val_string.c_str()));
                    }
                };
            }
            break;
        case avro::AVRO_INT:
            {
                return [tmp_buf, wbytes, this](MemPool* tuple_pool, Tuple* tuple, SlotDescriptor* slot_desc, avro::Decoder & decoder, int nullcount)  mutable {
                    int32_t val_int = decoder.decodeInt();
                    wbytes = sprintf((char*)tmp_buf, "%d", val_int);
                    _fill_slot(tuple, slot_desc, tuple_pool, tmp_buf, wbytes);
                }; 
            }
            break;
        case avro::AVRO_LONG:
            {
                return [tmp_buf, wbytes, this](MemPool* tuple_pool, Tuple* tuple, SlotDescriptor* slot_desc, avro::Decoder & decoder, int nullcount) mutable {
                    int64_t val_long = decoder.decodeLong();
                    wbytes = sprintf((char*)tmp_buf, "%ld", val_long);
                    _fill_slot(tuple, slot_desc, tuple_pool, tmp_buf, wbytes);
                };
            }
            break;
        case avro::AVRO_FLOAT:
            {
                return [tmp_buf, wbytes, this](MemPool* tuple_pool, Tuple* tuple, SlotDescriptor* slot_desc, avro::Decoder & decoder, int nullcount) mutable {
                    float val_float = decoder.decodeFloat();
                    wbytes = sprintf((char*)tmp_buf, "%f", val_float);
                    _fill_slot(tuple, slot_desc, tuple_pool, tmp_buf, wbytes);
                };
            }
            break;
        case avro::AVRO_DOUBLE:
            {
                return [tmp_buf, wbytes, this](MemPool* tuple_pool, Tuple* tuple, SlotDescriptor* slot_desc, avro::Decoder & decoder, int nullcount) mutable {
                    double val_double = decoder.decodeDouble();
                    wbytes = sprintf((char*)tmp_buf, "%lf", val_double);
                    _fill_slot(tuple, slot_desc, tuple_pool, tmp_buf, wbytes);
                };
            }
            break;
        case avro::AVRO_BOOL:
            {
                return [tmp_buf, wbytes, this](MemPool* tuple_pool, Tuple* tuple, SlotDescriptor* slot_desc, avro::Decoder & decoder, int nullcount) mutable {
                    bool val_bool = decoder.decodeBool();
                    wbytes = sprintf((char*)tmp_buf, "%d", val_bool);
                    _fill_slot(tuple, slot_desc, tuple_pool, tmp_buf, wbytes);
                };
            }
            break;
        case avro::AVRO_ARRAY:
            {
                // should have array type info
                // TODO :  
            }
            break;
        case avro::AVRO_UNION:
            {
                auto nullable_deserializer = [root_node, slot_desc, this](size_t non_null_union_index) {
                    auto nested_deserialize = createDeserializeFn(root_node->leafAt(non_null_union_index), slot_desc);
                    return [non_null_union_index, nested_deserialize, this] (MemPool* tuple_pool, Tuple* tuple, SlotDescriptor* slot_desc, avro::Decoder & decoder, int nullcount) mutable {
                        size_t union_index = decoder.decodeUnionIndex();
                        if (union_index == non_null_union_index)
                        {
                            nested_deserialize(tuple_pool, tuple, slot_desc, decoder, nullcount);
                        }
                    };
                };
                if (root_node->leaves() == 2 && slot_desc->is_nullable())
                {
                    if (root_node->leafAt(0)->type() == avro::AVRO_NULL)
                        return nullable_deserializer(1);
                    if (root_node->leafAt(1)->type() == avro::AVRO_NULL)
                        return nullable_deserializer(0);
                }
            }
            break;
        case avro::AVRO_NULL:
        {
            if (slot_desc->is_nullable())
            {
                return [this](MemPool* tuple_pool, Tuple* tuple, SlotDescriptor* slot_desc, avro::Decoder & decoder, int nullcount) mutable {
                    tuple->set_null(slot_desc->null_indicator_offset());
                    nullcount++;
                };
            }
        } 
            break;
        case avro::AVRO_ENUM: [[fallthrough]];
        case avro::AVRO_FIXED: [[fallthrough]];
        case avro::AVRO_MAP: [[fallthrough]];
        case avro::AVRO_RECORD: [[fallthrough]];
        default:
            {
                WHZ_LOG << "Not support " << root_node->type() << "type yet." << std::endl;
            }
            break;
    }
    throw avro::Exception("Type " + slot_desc->type().type + std::string("is not compatible with Avro ") + avro::ValidSchema(root_node).toJson(false));
}


AvroReader::SkipFn AvroReader::createSkipFn(avro::NodePtr root_node)
{
    switch (root_node->type())
    {
        case avro::AVRO_STRING:
            return [](avro::Decoder & decoder) { decoder.skipString(); };
        case avro::AVRO_BYTES:
            return [](avro::Decoder & decoder) { decoder.skipBytes(); };
        case avro::AVRO_INT:
            return [](avro::Decoder & decoder) { decoder.decodeInt(); };
        case avro::AVRO_LONG:
            return [](avro::Decoder & decoder) { decoder.decodeLong(); };
        case avro::AVRO_FLOAT:
            return [](avro::Decoder & decoder) { decoder.decodeFloat(); };
        case avro::AVRO_DOUBLE:
            return [](avro::Decoder & decoder) { decoder.decodeDouble(); };
        case avro::AVRO_BOOL:
            return [](avro::Decoder & decoder) { decoder.decodeBool(); };
        case avro::AVRO_ARRAY: {
            auto nested_skip_fn = createSkipFn(root_node->leafAt(0));
            return [nested_skip_fn](avro::Decoder & decoder) { 
                for (size_t n = decoder.arrayStart(); n != 0; n = decoder.arrayNext())
                {
                    for (size_t i = 0; i < n; ++i)
                    {
                        nested_skip_fn(decoder);
                    }
                }
            };
        }
        case avro::AVRO_UNION: {
            std::vector<SkipFn> union_skip_fns;
            for (size_t i = 0; i < root_node->leaves(); i++)
            {
                union_skip_fns.push_back(createSkipFn(root_node->leafAt(i)));
            }
            return [union_skip_fns](avro::Decoder & decoder) { union_skip_fns[decoder.decodeUnionIndex()](decoder); };
        }
        case avro::AVRO_NULL:
            return [](avro::Decoder & decoder) { decoder.decodeNull(); };
        case avro::AVRO_ENUM:
            return [](avro::Decoder & decoder) { decoder.decodeEnum(); };
        case avro::AVRO_FIXED: {
            auto fixed_size = root_node->fixedSize();
            return [fixed_size](avro::Decoder & decoder) { decoder.skipFixed(fixed_size); };
        }
        case avro::AVRO_MAP: {
            auto value_skip_fn = createSkipFn(root_node->leafAt(1));
            return [value_skip_fn](avro::Decoder & decoder) {
                for (size_t n = decoder.mapStart(); n != 0; n = decoder.mapNext())
                {
                    for (size_t i = 0; i < n; ++i)
                    {
                        decoder.skipString();
                        value_skip_fn(decoder);
                    }
                }
            };
        }
        case avro::AVRO_RECORD: {
            std::vector<SkipFn> field_skip_fns;
            for (size_t i = 0; i < root_node->leaves(); i++)
            {
                field_skip_fns.push_back(createSkipFn(root_node->leafAt(i)));
            }
            return [field_skip_fns](avro::Decoder & decoder) {
                for (auto & skip_fn : field_skip_fns) {
                    skip_fn(decoder);
                }
            };
        }
        default:
            throw avro::Exception("Unsupported Avro type");
    }
}
// one avro object only one row
// Status AvroReader::_handle_nested_complex_avro(Tuple* tuple,
//                                  const std::vector<SlotDescriptor*>& slot_descs,
//                                  MemPool* tuple_pool, bool* is_empty_row, bool* eof) {
//     WHZ_LOG << "into _handle_nested_complex_avro"  << std::endl;
//     size_t size = 0;

//     //Status st = _parse_avro_doc(&size, eof, tuple_pool, tuple, slot_descs);
//     //WHZ_LOG << "after parse_avro_doc, the st is :" << st.to_string() << std::endl;
//     // if (st.is_data_quality_error()) {
//     //     return Status::DataQualityError("avro data quality bad.");
//     // }
//     // if (st.is_data_quality_error()) {
//     //             //continue;
//     //     Status::DataQualityError("data quality is not good");
//     // }
//     // RETURN_IF_ERROR(st);
//     if (size == 0 || *eof) {
//         *is_empty_row = true;
//         return Status::OK();
//     }
//     // if (size == 0 || *eof) {
//     //     *is_empty_row = true;
//     // }
//     // *is_empty_row = false;

//     // TODO : how to decide that the row is empty in _parse_avro_doc ?

//     WHZ_LOG << "_handle_nested_complex_avro will return : " << std::endl; 
//     return Status::OK();
// }

Status AvroReader::_get_field_mapping(const std::vector<SlotDescriptor*>& slot_descs)
{
    _field_mapping.resize(_schema.root()->leaves(), -1);
    for (size_t i = 0; i < _schema.root()->leaves(); ++i)
    {
        _skip_fns.push_back(createSkipFn(_schema.root()->leafAt(i)));
        _deserialize_fns.push_back(&deserializeNoop);
    }

    size_t field_index;
    for (size_t i = 0; i < slot_descs.size(); ++i)
    {
        if(!_schema.root()->nameIndex(slot_descs[i]->col_name(), field_index)) 
        {
            return Status::DataQualityError("column " + slot_descs[i]->col_name() + " is not found in schema.");
        }
        auto field_schema = _schema.root()->leafAt(field_index);
        try {
            _deserialize_fns[field_index] = createDeserializeFn(field_schema, slot_descs[i]);
        } catch (...)
        {
            return Status::DataQualityError("column " + slot_descs[i]->col_name() + " failed to create deserialize function.");
        }
        _field_mapping[field_index] = i;
    }
    

    return Status::OK();
}

Status AvroReader::deserialize_row(Tuple* tuple, const std::vector<SlotDescriptor*>& slot_descs,
                                       MemPool* tuple_pool, bool* is_empty_row, bool* eof) 
{
    size_t size = 0;
    // get data, init _decoder
    Status st = _get_avro_doc(&size, eof, tuple_pool, tuple, slot_descs);
    if (st.is_data_quality_error()) {
        return Status::DataQualityError("avro data quality bad.");
    }
    RETURN_IF_ERROR(st);
        if (size == 0 || *eof) {
        *is_empty_row = true;
        return Status::OK();
    }

    int nullcount = 0;
    // do deserialize
    for (size_t i = 0; i < _field_mapping.size(); i++) 
    {
        if (_field_mapping[i] >= 0)
        {
            _deserialize_fns[i](tuple_pool, tuple, slot_descs[_field_mapping[i]], *_decoder, nullcount);
        }
        else
        {
            _skip_fns[i](*_decoder);
        }
    }
    
    if (nullcount == slot_descs.size()) {
        _counter->num_rows_filtered++;
        // valid = false;
    }
    return Status::OK();
}
Status AvroReader::read_avro_row(Tuple* tuple, const std::vector<SlotDescriptor*>& slot_descs, MemPool* tuple_pool,
                                 bool* is_empty_row, bool* eof) {
    Status st = _get_field_mapping(slot_descs);
    RETURN_IF_ERROR(st);                     
    return AvroReader::deserialize_row(tuple, slot_descs, tuple_pool, is_empty_row, eof);
}




} // namespace doris
