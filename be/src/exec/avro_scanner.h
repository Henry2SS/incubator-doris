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

#ifndef BE_SRC_AVRO_SCANNER_H_
#define BE_SRC_AVRO_SCANNER_H_
#include <string>
#include <vector>

#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include "avro/Compiler.hh"
#include "avro/DataFile.hh"
#include "avro/Decoder.hh"
#include "avro/Encoder.hh"
#include "avro/Generic.hh"
#include "avro/GenericDatum.hh"
#include "avro/Node.hh"
#include "avro/NodeConcepts.hh"
#include "avro/NodeImpl.hh"
#include "avro/Reader.hh"
#include "avro/Schema.hh"
#include "avro/Specific.hh"
#include "avro/ValidSchema.hh"
#include "avro/Writer.hh"
#include "avro/Exception.hh"

#include "common/status.h"
#include "exec/base_scanner.h"
#include "gen_cpp/PlanNodes_types.h"
#include "gen_cpp/Types_types.h"
#include "runtime/descriptors.h"
#include "runtime/mem_pool.h"
#include "runtime/small_file_mgr.h"
#include "runtime/stream_load/load_stream_mgr.h"
#include "runtime/tuple.h"
#include "util/runtime_profile.h"
#include "util/slice.h"
#include "util/file_utils.h"

#define WHZ_LOG std::cout << "whz_log: "
#define WHZ_TEST std::cout << "whz_test: "
namespace doris {
//class AvroDeserializer;
class Tuple;
class SlotDescriptor;
class RuntimeState;
class TupleDescriptor;
class MemTracker;
class FileReader;
class LineReader;
class AvroReader;

class AvroScanner : public BaseScanner {
public:
    AvroScanner(RuntimeState* state, RuntimeProfile* profile, const TBrokerScanRangeParams& params,
                const std::vector<TBrokerRangeDesc>& ranges,
                const std::vector<TNetworkAddress>& broker_addresses,
                const std::vector<TExpr>& pre_filter_texprs,
                ScannerCounter* counter);
    ~AvroScanner();

    // Open this scanner, will initialize information needed
    Status open() override;

    // Get next tuple
    Status get_next(Tuple* tuple, MemPool* tuple_pool, bool* eof) override;

    // Close this scanner
    void close() override;

private:
    Status open_file_reader();
    Status open_line_reader();
    Status open_avro_reader();
    Status open_next_reader();

private:
    const std::vector<TBrokerRangeDesc>& _ranges;
    const std::vector<TNetworkAddress>& _broker_addresses;

    std::string _avropaths;

    std::string _line_delimiter;
    int _line_delimiter_length;
    
    // std::unique_ptr<avro::DataFileReaderBase> _cur_file_reader;
    FileReader* _cur_file_reader;
    LineReader* _cur_line_reader;
    AvroReader* _cur_avro_reader;
    int _next_range;
    bool _cur_reader_eof;
    bool _scanner_eof;
    // bool _read_avro_by_line;

    bool _skip_next_line;

    std::shared_ptr<StreamLoadPipe> _stream_load_pipe;
};

struct JsonPath;

class AvroReader {
public:
    // AvroReader(RuntimeState* state, ScannerCounter* counter, RuntimeProfile* profile,
    //                    FileReader* file_reader, LineReader* line_reader);
    AvroReader(RuntimeState* state, ScannerCounter* counter, RuntimeProfile* profile,
                          FileReader* file_reader, LineReader* line_reader);
    ~AvroReader();

    const char* get_filename() { return _filename; }

    Status init();

    Status read_avro_row(Tuple* tuple, const std::vector<SlotDescriptor*>& slot_descs, MemPool* tuple_pool,
                         bool* is_empty_row, bool* eof);
private:
    // Status _handle_nested_complex_avro(Tuple* tuple, const std::vector<SlotDescriptor*>& slot_descs,
    //                                    MemPool* tuple_pool, bool* is_empty_row, bool* eof);

    void _fill_slot(Tuple* tuple, SlotDescriptor* slot_desc, MemPool* mem_pool,
                    const uint8_t* value, int32_t len);

    // void _set_tuple_value(avro::GenericDatum datum, Tuple* tuple,
    //                       const std::vector<SlotDescriptor*>& slot_descs, MemPool* tuple_pool,
    //                       bool* valid);
    // void _write_data_to_tuple(avro::GenericDatum datum, SlotDescriptor* desc,
    //                           Tuple* tuple, MemPool* tuple_pool, bool* valid);

    // bool _write_values_by_avropath(avro::GenericDatum datum, MemPool* tuple_pool,
    //                                Tuple* tuple, const std::vector<SlotDescriptor*>& slot_descs);
    
    // bool _process_simple_type(avro::Type type, std::string& path_name, avro::GenericDatum datum, int nullcount, MemPool* tuple_pool,
    //                                Tuple* tuple, const std::vector<SlotDescriptor*>& slot_descs);

    // size_t _get_column_index(std::string path_name, const std::vector<SlotDescriptor*>& slot_descs);
    void _close();

    //Status _get_avro_paths(const std::string& avropath,
    //                       std::vector<std::vector<JsonPath>>* vect);

    Status _get_avro_doc(size_t* size, bool* eof, MemPool* tuple_pool,
                           Tuple* tuple, const std::vector<SlotDescriptor*>& slot_descs);

    // std::string _print_avro_value(avro::NodePtr root_node);
    // std::string _print_avro_path(const std::vector<JsonPath>& path);

    // Status _validate_and_generate_avro_path(std::vector<std::vector<JsonPath>>* vect);
    // Status _validate_path_and_schema(avro::ValidSchema* schema, std::vector<std::vector<JsonPath>>* vect);

    Status _get_field_mapping(const std::vector<SlotDescriptor*>& slot_descs);
    Status deserialize_row(Tuple* tuple, const std::vector<SlotDescriptor*>& slot_descs,
                                       MemPool* tuple_pool, bool* is_empty_row, bool* eof);

    using DeserializeFn = std::function<void(MemPool* tuple_pool, Tuple* tuple, SlotDescriptor* slot_desc, avro::Decoder & decoder, int nullcount)>;
    using SkipFn = std::function<void(avro::Decoder & decoder)>;
    DeserializeFn createDeserializeFn(avro::NodePtr root_node, SlotDescriptor* slot_desc);
    SkipFn createSkipFn(avro::NodePtr root_node);
private:
    int _next_line;
    int _total_lines;
    RuntimeState* _state;
    ScannerCounter* _counter;
    RuntimeProfile* _profile;
    FileReader* _file_reader;
    LineReader* _line_reader;
    bool _closed;
    RuntimeProfile::Counter* _bytes_read_counter;
    RuntimeProfile::Counter* _read_timer;
    RuntimeProfile::Counter* _file_read_timer;

    std::vector<std::vector<JsonPath>> _parsed_avropaths;
    std::vector<JsonPath> _parsed_avro_root;

    std::unordered_map<std::string, int> _name_map;



    std::vector<int> _field_mapping;
    std::vector<SkipFn> _skip_fns;
    std::vector<DeserializeFn> _deserialize_fns;

    const char* _filename;
    avro::ValidSchema _schema;
    avro::DecoderPtr _decoder;
    avro::InputStreamPtr _in;
    //std::unique_ptr<avro::DataFileReader<avro::GenericDatum>> _file_reader_ptr;
    //avro::DataFileReaderBase _file_reader;
    //avro::GenericDatum _datum;

    
};

} // namespace doris




#endif
