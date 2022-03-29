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
#include "exec/s3_reader.h"
#include "exprs/expr.h"
#include "exprs/json_functions.h"
#include "gutil/strings/split.h"
#include "runtime/exec_env.h"
#include "runtime/mem_tracker.h"
#include "runtime/runtime_state.h"
#include "common/config.h"

namespace doris {

AvroScanner::AvroScanner(RuntimeState* state, RuntimeProfile* profile,
                         const TBrokerScanRangeParams& params,
                         const std::vector<TBrokerRangeDesc>& ranges,
                         const std::vector<TNetworkAddress>& broker_addresses,
                         const std::vector<TExpr>& pre_filter_texprs, ScannerCounter* counter)
    : BaseScanner(state, profile, params, pre_filter_texprs, counter),
      _ranges(ranges),
      _broker_addresses(broker_addresses),
      _cur_file_reader(nullptr),
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

    // read one object
    while (!_scanner_eof) {
        if (_cur_file_reader == nullptr || _cur_reader_eof) {
            RETURN_IF_ERROR(open_next_reader());
             // If there isn't any more reader, break this
            if (_scanner_eof) {
                break;
            }
        }

        // if (_read_avro_by_line && _skip_next_line) {
        //     size_t size = 0;
        //     const uint8_t* object_ptr = nullptr;
        //     RETURN_IF_ERROR(_cur_line_reader->read_line(&object_ptr, &size, &_cur_reader_eof));
        //     _skip_next_line = false;
        //     continue;
        // }

        //bool is_empty_row = false;
        int32_t readed_rows = 0;
        // 
        RETURN_IF_ERROR(_cur_avro_reader->read_avro_row(_src_tuple, _src_slot_descs, tuple_pool,
                                                    readed_rows, &_cur_reader_eof));
        WHZ_LOG << "read_avro_row = " << readed_rows << " rows." << std::endl;

        // 

        COUNTER_UPDATE(_rows_read_counter, readed_rows);
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
    // if (_cur_line_reader != nullptr) {
    //     delete _cur_line_reader;
    //     _cur_line_reader = nullptr;
    // }
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
    int64_t start_offset = range.start_offset;
    // if (range.__isset.read_json_by_line) {
    //     _read_avro_by_line = range.read_json_by_line;
    // }
    switch (range.file_type) {
    
    case TFileType::FILE_LOCAL: {
        WHZ_LOG << "range.path = " << range.path << std::endl;
        LocalFileReader* file_reader = new LocalFileReader(range.path, start_offset);
        RETURN_IF_ERROR(file_reader->open());
        _cur_file_reader = file_reader;
        break;
    }
    case TFileType::FILE_BROKER: {
        BrokerReader* broker_reader =
                new BrokerReader(_state->exec_env(), _broker_addresses, _params.properties,
                                 range.path, start_offset);
        RETURN_IF_ERROR(broker_reader->open());
        _cur_file_reader = broker_reader;
        break;
    }
    case TFileType::FILE_S3: {
        BufferedReader* s3_reader =
                new BufferedReader(_profile, new S3Reader(_params.properties, range.path, start_offset));
        RETURN_IF_ERROR(s3_reader->open());
        _cur_file_reader = s3_reader;
        break;
    }
    case TFileType::FILE_STREAM: {
        _stream_load_pipe = _state->exec_env()->load_stream_mgr()->get(range.load_id);
        if (_stream_load_pipe == nullptr) {
            VLOG_NOTICE << "unknown stream load id: " << UniqueId(range.load_id);
            return Status::InternalError("unknown stream load id");
        }
        _cur_file_reader = _stream_load_pipe.get();
        break;
    }
    default: {
        std::stringstream ss;
        ss << "Unknown file type, type=" << range.file_type;
        return Status::InternalError(ss.str());
    }
    }
    _cur_reader_eof = false;
    return Status::OK();
}


// Status AvroScanner::open_line_reader() {
//     if (_cur_line_reader != nullptr) {
//         delete _cur_line_reader;
//         _cur_line_reader = nullptr;
//     }

//     const TBrokerRangeDesc& range = _ranges[_next_range];
//     int64_t size = range.size;
//     if (range.start_offset != 0) {
//         size += 1;
//         _skip_next_line = true;
//     } else {
//         _skip_next_line = false;
//     }
//     _cur_line_reader = new PlainTextLineReader(_profile, _cur_file_reader, nullptr,
//                                                size, _line_delimiter, _line_delimiter_length);
//     //_cur_line_reader = new PlainBinaryLineReader(_cur_file_reader);
//     _cur_reader_eof = false;
//     return Status::OK();
// }


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

    std::string avropath = "";
    std::string avro_root = "";

    if (range.__isset.avropaths) {
        avropath = range.avropaths;
    }
    if (range.__isset.json_root) {
        avro_root = range.json_root;
    }

    // if (_read_avro_by_line) {
    //     _cur_avro_reader =
    //             new AvroReader(_state, _counter, _profile, nullptr, _cur_line_reader);
    // } else {
    _cur_avro_reader = new AvroReader(_state, _counter, _profile, _cur_file_reader);
    //}
    WHZ_LOG << "new AvroReader" << std::endl;
    RETURN_IF_ERROR(_cur_avro_reader->init(avropath, avro_root));
    return Status::OK();
}

Status AvroScanner::open_next_reader() {
    if (_next_range >= _ranges.size()) {
        _scanner_eof = true;
        return Status::OK();
    }

    RETURN_IF_ERROR(open_file_reader());
    // if (_read_avro_by_line) {
    //     RETURN_IF_ERROR(open_line_reader());
    // }
    //RETURN_IF_ERROR(open_line_reader());
    RETURN_IF_ERROR(open_avro_reader());
    _next_range++;

    return Status::OK();
}




////// class AvroReader
AvroReader::AvroReader(RuntimeState* state, ScannerCounter* counter, RuntimeProfile* profile,
                       FileReader* file_reader) 
        : _next_line(0), 
          _total_lines(0),
          _state(state),
          _counter(counter),
          _profile(profile),
          _file_reader(file_reader),
          _closed(false),
          _file_reader_ptr(nullptr) {
    _bytes_read_counter = ADD_COUNTER(_profile, "BytesRead", TUnit::BYTES);
    _read_timer = ADD_TIMER(_profile, "ReadTime");
    _file_read_timer = ADD_TIMER(_profile, "FileReadTime");
}


AvroReader::~AvroReader() {
    _close();
}

Status AvroReader::init(const std::string& avropath, const std::string& avro_root) {
    WHZ_LOG << avropath.empty() << std::endl;
    if (!avropath.empty()) {
        Status st = _get_avro_paths(avropath, &_parsed_avropaths);
        RETURN_IF_ERROR(st);
        for (int i = 0; i < _parsed_avropaths.size(); i++) {
            for (int j = 0; j < _parsed_avropaths[i].size(); j++) {
                WHZ_TEST << _parsed_avropaths[i][j].to_string() << " ";
                WHZ_TEST << _parsed_avropaths[i][j].key << std::endl;
            }
            std::cout << " " << std::endl;
        }
    }
    if (!avro_root.empty()) {
        JsonFunctions::parse_json_paths(avro_root, &_parsed_avro_root);
    }
    // get avro schema
    // test
    // bool exist = FileUtils::check_exist(config::avro_schema_file_path);
    bool exist = FileUtils::check_exist("/tmp/jdolap/output/be/conf/avro_schema.json");
    if (!exist) {
        return Status::InternalError("there is no avro schema file at " + config::avro_schema_file_path + ". Please put an schema file in json format.");
    } else {
        std::string s = "/tmp/jdolap/output/be/conf/avro_schema.json";
        _schema = avro::compileJsonSchemaFromFile(s.c_str());
        _datum = avro::GenericDatum(_schema);
    }
    
    // TODO: check if path is in shcema

    return Status::OK();
}

Status AvroReader::_get_avro_paths(const std::string& avropath,
                           std::vector<std::vector<JsonPath>>* vect) {
    rapidjson::Document avropaths_doc;
    if (!avropaths_doc.Parse(avropath.c_str(), avropath.length()).HasParseError()) {
        // TODO: should check if path is no more than 2-level
        if (!avropaths_doc.IsArray()) {
            return Status::InvalidArgument("Invalid avro path: " + avropath);
        } else {
            for (int i = 0; i < avropaths_doc.Size(); i++) {
                const rapidjson::Value& path = avropaths_doc[i];
                if (!path.IsString()) {
                    return Status::InvalidArgument("Invalid avro path: " + avropath);
                }
                std::vector<JsonPath> parsed_paths;
                JsonFunctions::parse_json_paths(path.GetString(), &parsed_paths);
                vect->push_back(std::move(parsed_paths));
            }
            return Status::OK();
        }
    } else {
        return Status::InvalidArgument("Invalid avro path: " + avropath);
    }
}

void AvroReader::_close() {
    if (_closed) {
        return;
    }
    _closed = true;
}

std::string AvroReader::_print_avro_value(avro::NodePtr root_node) {
    std::ostringstream ss;      // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    ss.exceptions(std::ios::failbit);
    root_node->printJson(ss, 0);
    return ss.str();
}

std::string AvroReader::_print_avro_path(const std::vector<JsonPath>& path) {
    std::stringstream ss;
    for (auto& p : path) {
        ss << p.to_string() << ".";
    }
    return ss.str();
}

// read one avro object from line reader or file reader, and decode it
Status AvroReader::_parse_avro_doc(int32_t readed_rows, size_t* size, bool* eof, MemPool* tuple_pool,
                                   Tuple* tuple, const std::vector<SlotDescriptor*>& slot_descs) {
    SCOPED_TIMER(_file_read_timer);
    const uint8_t* avro_str = nullptr;
    std::unique_ptr<uint8_t[]> avro_str_ptr;
    int64_t length = 0;
    RETURN_IF_ERROR(_file_reader->read_one_message(&avro_str_ptr, &length));
    avro_str = avro_str_ptr.get();
    *size = length;
    if (length == 0) {
        *eof = true;
    }

    if (*eof) {
        return Status::OK();
    }

    _bytes_read_counter += *size;
    
    _file_reader_ptr = std::make_unique<avro::DataFileReader<avro::GenericDatum>>(avro::memoryInputStream(avro_str, *size));

    if (_file_reader_ptr.get()->read(_datum)) {
        // TODO : skip row here ?

        while (_datum.type() != avro::AVRO_RECORD) {
            return Status::DataQualityError("Root schema must be a record");
        }
        // if (_datum.value<avro::GenericRecord>().fieldCount() < _parsed_avropaths.size()) {
        //     return Status::DataQualityError("avro path size is larger than schema size");
        // }
        // for (int i = 0; i < _parsed_avropaths.size(); i++) {
        //     WHZ_LOG << "check each type " << i << " " << _datum.value<avro::GenericRecord>().field(_parsed_avropaths[i][1].to_string()).type() << std::endl;
        //     if (_datum.value<avro::GenericRecord>().field(_parsed_avropaths[i][1].to_string()).type() != avro::AVRO_STRING 
        //         && _datum.value<avro::GenericRecord>().field(_parsed_avropaths[i][1].to_string()).type() != avro::AVRO_LONG) { 
        //         return Status::DataQualityError("only AVRO_STRING and AVRO_LONG supported.");
        //     } else {
        //         WHZ_LOG << "here" << std::endl; 
        //         WHZ_LOG << "fieldcount" << _datum.value<avro::GenericRecord>().fieldCount() << std::endl;
        //     }
        // } 
        if (!_write_values_by_avropath(_datum, tuple_pool, tuple, slot_descs)) {
            _counter->num_rows_filtered++;
        } else {
            readed_rows++;
        }
    }
    //WHZ_LOG << "file not base" << _reader << std::endl;
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
    WHZ_LOG << "str_slot->ptr " << str_slot->to_string() << std::endl;
    str_slot->len = len;
    WHZ_LOG << "str_slot->len " << str_slot->len << std::endl;
}

size_t AvroReader::_get_column_index(std::string path_name, const std::vector<SlotDescriptor*>& slot_descs) {
    for (size_t i = 0; i < slot_descs.size(); i++) {
        if (slot_descs[i]->col_name() == path_name) {
            return i;
        }
    }
    return -1;
}

bool AvroReader::_write_values_by_avropath(avro::GenericDatum datum, MemPool* tuple_pool,
                                   Tuple* tuple, const std::vector<SlotDescriptor*>& slot_descs) {
    WHZ_LOG << "into _write_values_by_avropath " << std::endl;
    int nullcount = 0;
    bool valid = true;
    // uint8_t tmp_buf[128] = {0};
    // int32_t wbytes = 0;
    
    // avro_path should be different each one 
    for (int i = 0; i < _parsed_avropaths.size(); i++) {
        // should modify
        std::string path_name = _parsed_avropaths[i][1].to_string();
        auto type = datum.value<avro::GenericRecord>().field(path_name).type();
        avro::GenericDatum record_datum = datum.value<avro::GenericRecord>().field(path_name);

        switch (type)
        {
        case avro::AVRO_LONG:
        case avro::AVRO_INT:
        case avro::AVRO_FLOAT:
        case avro::AVRO_DOUBLE:
        case avro::AVRO_BYTES:
        case avro::AVRO_STRING:
        case avro::AVRO_BOOL:
            {
                _process_simple_type(type, path_name, record_datum, nullcount, tuple_pool, tuple, slot_descs);
            }
            break;
        case avro::AVRO_MAP:
            {
                avro::GenericMap map = record_datum.value<avro::GenericMap>();
                auto map_type = map.schema()->leafAt(0)->type();
                for (auto pair : map.value()) {
                    std::string map_key = pair.first;
                    avro::GenericDatum map_datum = pair.second;
                    for (size_t j = 0; j < slot_descs.size(); j++) {
                        if (map_key == slot_descs[j]->col_name()) {
                            _process_simple_type(map_type, map_key, map_datum, nullcount, tuple_pool, tuple, slot_descs);
                        }
                    }
                }
            }
        default:
            valid = false;
            break;
        }

    }
    if (nullcount == slot_descs.size()) {
        WHZ_LOG << "empty row" << std::endl;
        _counter->num_rows_filtered++;
        valid = false;
    }
    return valid;

}


bool AvroReader::_process_simple_type(avro::Type type, std::string& path_name, avro::GenericDatum datum, int nullcount, MemPool* tuple_pool,
                                      Tuple* tuple, const std::vector<SlotDescriptor*>& slot_descs) {
    //int nullcount = 0;
    bool valid = true;
    uint8_t tmp_buf[128] = {0};
    int32_t wbytes = 0;

    size_t index_of_slot = _get_column_index(path_name, slot_descs);
    WHZ_LOG << "into _process_simple_type type : " << type << std::endl;
    WHZ_LOG << "into _process_simple_type index_of_slot : " << index_of_slot << std::endl;
    switch (type)
    {
        case avro::AVRO_LONG:
            {
                int64_t val_long = datum.value<int64_t>();
                if (index_of_slot == -1) {
                    WHZ_LOG << "there is no such column named " << path_name << ". Please check the input of json_path." << std::endl; 
                    valid = false;
                } else {
                    wbytes = sprintf((char*)tmp_buf, "%ld", val_long);
                     _fill_slot(tuple, slot_descs[index_of_slot], tuple_pool, tmp_buf, wbytes);
                }
            }
            break;
        case avro::AVRO_INT:
            {
                int32_t val_int = datum.value<int32_t>();
                if (index_of_slot == -1) {
                    WHZ_LOG << "there is no such column named " << path_name << ". Please check the input of json_path." << std::endl; 
                    valid = false;
                } else {
                    wbytes = sprintf((char*)tmp_buf, "%d", val_int);
                    _fill_slot(tuple, slot_descs[index_of_slot], tuple_pool, tmp_buf, wbytes);
                }
            }
            break;
        case avro::AVRO_FLOAT:
            {
                float val_float = datum.value<float>();
                if (index_of_slot == -1) {
                    WHZ_LOG << "there is no such column named " << path_name << ". Please check the input of json_path." << std::endl; 
                    valid = false;
                } else {
                    WHZ_LOG << "val_float = " << val_float << std::endl;
                    wbytes = sprintf((char*)tmp_buf, "%f", val_float);
                    _fill_slot(tuple, slot_descs[index_of_slot], tuple_pool, tmp_buf, wbytes);
                }
            }
            break;
        case avro::AVRO_DOUBLE:
            {
                double val_double = datum.value<double>();
                if (index_of_slot == -1) {
                    WHZ_LOG << "there is no such column named " << path_name << ". Please check the input of json_path." << std::endl; 
                    valid = false;
                } else {
                    WHZ_LOG << "val_double = " << val_double << std::endl;
                    wbytes = sprintf((char*)tmp_buf, "%lf", val_double);
                    _fill_slot(tuple, slot_descs[index_of_slot], tuple_pool, tmp_buf, wbytes);
                }
                
            }
            break;
        case avro::AVRO_BYTES: [[fallthrough]];
        case avro::AVRO_STRING:
            {
                std::string val_string = datum.value<std::string>();
                if (index_of_slot == -1) {
                    WHZ_LOG << "there is no such column named " << path_name << ". Please check the input of json_path." << std::endl;
                    valid = false; 
                } else if (val_string.empty()) {
                    if (slot_descs[index_of_slot]->is_nullable()) {
                        tuple->set_null(slot_descs[index_of_slot]->null_indicator_offset());
                        nullcount++;
                    } else {
                        WHZ_LOG << "column " << slot_descs[index_of_slot] << " is not nullable, but the value parsed from avro is null." << std::endl;
                    }
                } else {
                    _fill_slot(tuple, slot_descs[index_of_slot], tuple_pool, (uint8_t*)val_string.c_str(), strlen(val_string.c_str()));

                }   
            }
            break;
        case avro::AVRO_BOOL:
            {
                bool val_bool = datum.value<bool>();
                if (index_of_slot == -1) {
                    WHZ_LOG << "there is no such column named " << path_name << ". Please check the input of json_path." << std::endl; 
                    valid = false;
                } else {
                    WHZ_LOG << "val_bool = " << val_bool << std::endl;
                    wbytes = sprintf((char*)tmp_buf, "%d", val_bool);
                    _fill_slot(tuple, slot_descs[index_of_slot], tuple_pool, tmp_buf, wbytes);
                }
                
            }
            break;
    default:
        WHZ_LOG << "not supported yet. Only support simple type or 2-level complex type.";
        valid = false; // current row is invalid
        break;
    }
    return valid;
}

Status AvroReader::_handle_nested_complex_avro(Tuple* tuple,
                                 const std::vector<SlotDescriptor*>& slot_descs,
                                 MemPool* tuple_pool, int32_t readed_rows, bool* eof) {
    WHZ_LOG << "into _handle_nested_complex_avro"  << std::endl;
    size_t size = 0;
    //int32_t readed_rows = 0;
    Status st = _parse_avro_doc(readed_rows, &size, eof, tuple_pool, tuple, slot_descs);
    WHZ_LOG << "after parse_avro_doc, the st is :" << st.to_string() << std::endl;

    // if (st.is_data_quality_error()) {
    //             //continue;
    //     Status::DataQualityError("data quality is not good");
    // }
    RETURN_IF_ERROR(st);
    
    // if (size == 0 || *eof) {
    //     *is_empty_row = true;
    // }
    // *is_empty_row = false;

    // TODO : how to decide that the row is empty in _parse_avro_doc ?

    WHZ_LOG << "_handle_nested_complex_avro will return : " << std::endl; 
    return Status::OK();
}





Status AvroReader::read_avro_row(Tuple* tuple, const std::vector<SlotDescriptor*>& slot_descs, MemPool* tuple_pool,
                int32_t readed_rows, bool* eof) {
    return AvroReader::_handle_nested_complex_avro(tuple, slot_descs, tuple_pool, readed_rows, eof);
}




} // namespace doris
