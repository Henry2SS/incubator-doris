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

#include <gtest/gtest.h>


#include "common/object_pool.h"
#include "exec/broker_scan_node.h"
#include "exec/local_file_reader.h"
#include "exprs/cast_functions.h"
#include "gen_cpp/Descriptors_types.h"
#include "gen_cpp/PlanNodes_types.h"
#include "exprs/decimalv2_operators.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"
#include "runtime/tuple.h"
#include "runtime/user_function_cache.h"
#include "exec/avro_scanner.h"

namespace doris {

class AvroScannerTestMap : public testing::Test {
public:
    AvroScannerTestMap() : _runtime_state(TQueryGlobals()) {
        init();
        _runtime_state._instance_mem_tracker.reset(new MemTracker());
        _runtime_state._exec_env = ExecEnv::GetInstance();
    }
    void init();
    static void SetUpTestCase() {
        UserFunctionCache::instance()->init(
                "./be/test/runtime/test_data/user_function_cache/normal");
        CastFunctions::init();
        DecimalV2Operators::init();
    }

protected:
    virtual void SetUp() {}
    virtual void TearDown() {}

private:
    int create_src_tuple(TDescriptorTable& t_desc_table, int next_slot_id);
    int create_dst_tuple(TDescriptorTable& t_desc_table, int next_slot_id);
    void create_expr_info();
    void init_desc_table();
    RuntimeState _runtime_state;
    ObjectPool _obj_pool;
    std::map<std::string, SlotDescriptor*> _slots_map;
    TBrokerScanRangeParams _params;
    DescriptorTbl* _desc_tbl;
    TPlanNode _tnode;
    ScannerCounter _counter;
};

#define TUPLE_ID_DST 0
#define TUPLE_ID_SRC 1
#define COLUMN_NUMBERS 6
#define DST_TUPLE_SLOT_ID_START 1
#define SRC_TUPLE_SLOT_ID_START 7

int AvroScannerTestMap::create_src_tuple(TDescriptorTable& t_desc_table, int next_slot_id) {
    const char *columnNames[] = {"k1", "k2", "k3", "mid", "db", "sch"};
    for (int i = 0; i < COLUMN_NUMBERS; i++) {
        TSlotDescriptor slot_desc;

        slot_desc.id = next_slot_id++;
        slot_desc.parent = 1;
        TTypeDesc type;
        {
            TTypeNode node;
            node.__set_type(TTypeNodeType::SCALAR);
            TScalarType scalar_type;
            scalar_type.__set_type(TPrimitiveType::VARCHAR);
            scalar_type.__set_len(65535);
            node.__set_scalar_type(scalar_type);
            type.types.push_back(node);
        }
        slot_desc.slotType = type;
        slot_desc.columnPos = i;
        slot_desc.byteOffset = i * 16 + 8;
        slot_desc.nullIndicatorByte = i / 8;
        slot_desc.nullIndicatorBit = i % 8;
        slot_desc.colName = columnNames[i];
        slot_desc.slotIdx = i + 1;
        slot_desc.isMaterialized = true;

        t_desc_table.slotDescriptors.push_back(slot_desc);
    }

    {
        // TTupleDescriptor source
        TTupleDescriptor t_tuple_desc;
        t_tuple_desc.id = TUPLE_ID_SRC;
        t_tuple_desc.byteSize = COLUMN_NUMBERS * 16 + 8;
        t_tuple_desc.numNullBytes = 0;
        t_tuple_desc.tableId = 0;
        t_tuple_desc.__isset.tableId = true;
        t_desc_table.tupleDescriptors.push_back(t_tuple_desc);
    }
    return next_slot_id;
}

int AvroScannerTestMap::create_dst_tuple(TDescriptorTable& t_desc_table, int next_slot_id) {
    int32_t byteOffset = 8;
    { //k4
        TSlotDescriptor slot_desc;
        slot_desc.id = next_slot_id++;
        slot_desc.parent = 0;
        TTypeDesc type;
        {
            TTypeNode node;
            node.__set_type(TTypeNodeType::SCALAR);
            TScalarType scalar_type;
            scalar_type.__set_type(TPrimitiveType::LARGEINT);
            scalar_type.__set_len(65535);
            node.__set_scalar_type(scalar_type);
            type.types.push_back(node);
        }
        slot_desc.slotType = type;
        slot_desc.columnPos = 0;
        slot_desc.byteOffset = byteOffset;
        slot_desc.nullIndicatorByte = 0;
        slot_desc.nullIndicatorBit = 0;
        slot_desc.colName = "k1";
        slot_desc.slotIdx = 1;
        slot_desc.isMaterialized = true;

        t_desc_table.slotDescriptors.push_back(slot_desc);
    }
    byteOffset += 16;
    { // k5
        TSlotDescriptor slot_desc;

        slot_desc.id = next_slot_id++;
        slot_desc.parent = 0;
        TTypeDesc type;
        {
            TTypeNode node;
            node.__set_type(TTypeNodeType::SCALAR);
            TScalarType scalar_type;
            scalar_type.__set_type(TPrimitiveType::BIGINT);
            scalar_type.__set_len(65535);
            node.__set_scalar_type(scalar_type);
            type.types.push_back(node);
        }
        slot_desc.slotType = type;
        slot_desc.columnPos = 1;
        slot_desc.byteOffset = byteOffset;
        slot_desc.nullIndicatorByte = 0;
        slot_desc.nullIndicatorBit = 1;
        slot_desc.colName = "k2";
        slot_desc.slotIdx = 2;
        slot_desc.isMaterialized = true;

        t_desc_table.slotDescriptors.push_back(slot_desc);
    }
    byteOffset += 16;
    { // k6
        TSlotDescriptor slot_desc;

        slot_desc.id = next_slot_id++;
        slot_desc.parent = 0;
        TTypeDesc type;
        {
            TTypeNode node;
            node.__set_type(TTypeNodeType::SCALAR);
            TScalarType scalar_type;
            scalar_type.__set_type(TPrimitiveType::SMALLINT);
            scalar_type.__set_len(65535);
            node.__set_scalar_type(scalar_type);
            type.types.push_back(node);
        }
        slot_desc.slotType = type;
        slot_desc.columnPos = 2;
        slot_desc.byteOffset = byteOffset;
        slot_desc.nullIndicatorByte = 0;
        slot_desc.nullIndicatorBit = 2;
        slot_desc.colName = "k3";
        slot_desc.slotIdx = 3;
        slot_desc.isMaterialized = true;

        t_desc_table.slotDescriptors.push_back(slot_desc);
    }
    byteOffset += 16;
    { // k1
        TSlotDescriptor slot_desc;

        slot_desc.id = next_slot_id++;
        slot_desc.parent = 0;
        TTypeDesc type;
        {
            TTypeNode node;
            node.__set_type(TTypeNodeType::SCALAR);
            TScalarType scalar_type;
            scalar_type.__set_type(TPrimitiveType::LARGEINT);
            scalar_type.__set_len(65535);
            node.__set_scalar_type(scalar_type);
            type.types.push_back(node);
        }
        slot_desc.slotType = type;
        slot_desc.columnPos = 3;
        slot_desc.byteOffset = byteOffset;
        slot_desc.nullIndicatorByte = 0;
        slot_desc.nullIndicatorBit = 3;
        slot_desc.colName = "mid";
        slot_desc.slotIdx = 4;
        slot_desc.isMaterialized = true;

        t_desc_table.slotDescriptors.push_back(slot_desc);
    }
    byteOffset += 8;
    {// k2
        TSlotDescriptor slot_desc;

        slot_desc.id = next_slot_id++;
        slot_desc.parent = 0;
        TTypeDesc type;
        {
            TTypeNode node;
            node.__set_type(TTypeNodeType::SCALAR);
            TScalarType scalar_type;
            scalar_type.__set_type(TPrimitiveType::VARCHAR);
            scalar_type.__set_len(65535);
            node.__set_scalar_type(scalar_type);
            type.types.push_back(node);
        }
        slot_desc.slotType = type;
        slot_desc.columnPos = 4;
        slot_desc.byteOffset = byteOffset;
        slot_desc.nullIndicatorByte = 0;
        slot_desc.nullIndicatorBit = 4;
        slot_desc.colName = "db";
        slot_desc.slotIdx = 5;
        slot_desc.isMaterialized = true;

        t_desc_table.slotDescriptors.push_back(slot_desc);
    }
    byteOffset += 16;
    {// k3
        TSlotDescriptor slot_desc;

        slot_desc.id = next_slot_id++;
        slot_desc.parent = 0;
        TTypeDesc type;
        {
            TTypeNode node;
            node.__set_type(TTypeNodeType::SCALAR);
            TScalarType scalar_type;
            scalar_type.__isset.precision = true;
            scalar_type.__isset.scale = true;
            scalar_type.__set_precision(-1);
            scalar_type.__set_scale(-1);
            scalar_type.__set_type(TPrimitiveType::VARCHAR);
            scalar_type.__set_len(65535);
            node.__set_scalar_type(scalar_type);
            type.types.push_back(node);
        }
        slot_desc.slotType = type;
        slot_desc.columnPos = 5;
        slot_desc.byteOffset = byteOffset;
        slot_desc.nullIndicatorByte = 0;
        slot_desc.nullIndicatorBit = 5;
        slot_desc.colName = "sch";
        slot_desc.slotIdx = 6;
        slot_desc.isMaterialized = true;

        t_desc_table.slotDescriptors.push_back(slot_desc);
    }

    t_desc_table.__isset.slotDescriptors = true;
    {
        // TTupleDescriptor dest
        TTupleDescriptor t_tuple_desc;
        t_tuple_desc.id = TUPLE_ID_DST;
        t_tuple_desc.byteSize = byteOffset + 8;
        t_tuple_desc.numNullBytes = 0;
        t_tuple_desc.tableId = 0;
        t_tuple_desc.__isset.tableId = true;
        t_desc_table.tupleDescriptors.push_back(t_tuple_desc);
    }
    return next_slot_id;
}

void AvroScannerTestMap::init_desc_table() {
    TDescriptorTable t_desc_table;

    // table descriptors
    TTableDescriptor t_table_desc;

    t_table_desc.id = 0;
    t_table_desc.tableType = TTableType::BROKER_TABLE;
    t_table_desc.numCols = 0;
    t_table_desc.numClusteringCols = 0;
    t_desc_table.tableDescriptors.push_back(t_table_desc);
    t_desc_table.__isset.tableDescriptors = true;

    int next_slot_id = 1;

    next_slot_id = create_dst_tuple(t_desc_table, next_slot_id);

    next_slot_id = create_src_tuple(t_desc_table, next_slot_id);

    DescriptorTbl::create(&_obj_pool, t_desc_table, &_desc_tbl);

    _runtime_state.set_desc_tbl(_desc_tbl);
}

void AvroScannerTestMap::create_expr_info() {
    TTypeDesc varchar_type;
    {
        TTypeNode node;
        node.__set_type(TTypeNodeType::SCALAR);
        TScalarType scalar_type;
        scalar_type.__set_type(TPrimitiveType::VARCHAR);
        scalar_type.__set_len(5000);
        node.__set_scalar_type(scalar_type);
        varchar_type.types.push_back(node);
    }
    // k4 VARCHAR --> VARCHAR
    {
        TExprNode slot_ref;
        slot_ref.node_type = TExprNodeType::SLOT_REF;
        slot_ref.type = varchar_type;
        slot_ref.num_children = 0;
        slot_ref.__isset.slot_ref = true;
        slot_ref.slot_ref.slot_id = SRC_TUPLE_SLOT_ID_START; // category id in src tuple
        slot_ref.slot_ref.tuple_id = 1;

        TExpr expr;
        expr.nodes.push_back(slot_ref);

        _params.expr_of_dest_slot.emplace(DST_TUPLE_SLOT_ID_START, expr);
        _params.src_slot_ids.push_back(SRC_TUPLE_SLOT_ID_START);
    }
    // k5 VARCHAR --> VARCHAR
    {
        TExprNode slot_ref;
        slot_ref.node_type = TExprNodeType::SLOT_REF;
        slot_ref.type = varchar_type;
        slot_ref.num_children = 0;
        slot_ref.__isset.slot_ref = true;
        slot_ref.slot_ref.slot_id = SRC_TUPLE_SLOT_ID_START + 1; // author id in src tuple
        slot_ref.slot_ref.tuple_id = 1;

        TExpr expr;
        expr.nodes.push_back(slot_ref);

        _params.expr_of_dest_slot.emplace(DST_TUPLE_SLOT_ID_START + 1, expr);
        _params.src_slot_ids.push_back(SRC_TUPLE_SLOT_ID_START + 1);
    }
    // k6 VARCHAR --> VARCHAR
    {
        TExprNode slot_ref;
        slot_ref.node_type = TExprNodeType::SLOT_REF;
        slot_ref.type = varchar_type;
        slot_ref.num_children = 0;
        slot_ref.__isset.slot_ref = true;
        slot_ref.slot_ref.slot_id = SRC_TUPLE_SLOT_ID_START + 2; // log_time id in src tuple
        slot_ref.slot_ref.tuple_id = 1;

        TExpr expr;
        expr.nodes.push_back(slot_ref);

        _params.expr_of_dest_slot.emplace(DST_TUPLE_SLOT_ID_START + 2, expr);
        _params.src_slot_ids.push_back(SRC_TUPLE_SLOT_ID_START + 2);
    }

    // k1 VARCHAR --> DOUBLE
    {
        TTypeDesc int_type;
        {
            TTypeNode node;
            node.__set_type(TTypeNodeType::SCALAR);
            TScalarType scalar_type;
            scalar_type.__set_type(TPrimitiveType::BIGINT);
            node.__set_scalar_type(scalar_type);
            int_type.types.push_back(node);
        }
        TExprNode cast_expr;
        cast_expr.node_type = TExprNodeType::CAST_EXPR;
        cast_expr.type = int_type;
        cast_expr.__set_opcode(TExprOpcode::CAST);
        cast_expr.__set_num_children(1);
        cast_expr.__set_output_scale(-1);
        cast_expr.__isset.fn = true;
        cast_expr.fn.name.function_name = "casttodouble";
        cast_expr.fn.binary_type = TFunctionBinaryType::BUILTIN;
        cast_expr.fn.arg_types.push_back(varchar_type);
        cast_expr.fn.ret_type = int_type;
        cast_expr.fn.has_var_args = false;
        cast_expr.fn.__set_signature("casttodouble(VARCHAR(*))");
        cast_expr.fn.__isset.scalar_fn = true;
        cast_expr.fn.scalar_fn.symbol = "doris::CastFunctions::cast_to_double_val";

        TExprNode slot_ref;
        slot_ref.node_type = TExprNodeType::SLOT_REF;
        slot_ref.type = varchar_type;
        slot_ref.num_children = 0;
        slot_ref.__isset.slot_ref = true;
        slot_ref.slot_ref.slot_id = SRC_TUPLE_SLOT_ID_START + 3; // price id in src tuple
        slot_ref.slot_ref.tuple_id = 1;

        TExpr expr;
        expr.nodes.push_back(cast_expr);
        expr.nodes.push_back(slot_ref);

        _params.expr_of_dest_slot.emplace(DST_TUPLE_SLOT_ID_START + 3, expr);
        _params.src_slot_ids.push_back(SRC_TUPLE_SLOT_ID_START + 3);
    }
    // k2 VARCHAR --> LargeInt
    {
        TTypeDesc int_type;
        {
            TTypeNode node;
            node.__set_type(TTypeNodeType::SCALAR);
            TScalarType scalar_type;
            scalar_type.__set_type(TPrimitiveType::LARGEINT);
            node.__set_scalar_type(scalar_type);
            int_type.types.push_back(node);
        }
        TExprNode cast_expr;
        cast_expr.node_type = TExprNodeType::CAST_EXPR;
        cast_expr.type = int_type;
        cast_expr.__set_opcode(TExprOpcode::CAST);
        cast_expr.__set_num_children(1);
        cast_expr.__set_output_scale(-1);
        cast_expr.__isset.fn = true;
        cast_expr.fn.name.function_name = "casttolargeint";
        cast_expr.fn.binary_type = TFunctionBinaryType::BUILTIN;
        cast_expr.fn.arg_types.push_back(varchar_type);
        cast_expr.fn.ret_type = int_type;
        cast_expr.fn.has_var_args = false;
        cast_expr.fn.__set_signature("casttolargeint(VARCHAR(*))");
        cast_expr.fn.__isset.scalar_fn = true;
        cast_expr.fn.scalar_fn.symbol = "doris::CastFunctions::cast_to_large_int_val";

        TExprNode slot_ref;
        slot_ref.node_type = TExprNodeType::SLOT_REF;
        slot_ref.type = varchar_type;
        slot_ref.num_children = 0;
        slot_ref.__isset.slot_ref = true;
        slot_ref.slot_ref.slot_id = SRC_TUPLE_SLOT_ID_START + 4; // price id in src tuple
        slot_ref.slot_ref.tuple_id = 1;

        TExpr expr;
        expr.nodes.push_back(cast_expr);
        expr.nodes.push_back(slot_ref);

        _params.expr_of_dest_slot.emplace(DST_TUPLE_SLOT_ID_START + 4, expr);
        _params.src_slot_ids.push_back(SRC_TUPLE_SLOT_ID_START + 4);
    }
    // k3 VARCHAR --> Decimal
    {
        TTypeDesc int_type;
        {
            TTypeNode node;
            node.__set_type(TTypeNodeType::SCALAR);
            TScalarType scalar_type;
            scalar_type.__isset.precision = true;
            scalar_type.__isset.scale = true;
            scalar_type.__set_precision(-1);
            scalar_type.__set_scale(-1);
            scalar_type.__set_type(TPrimitiveType::DECIMALV2);
            node.__set_scalar_type(scalar_type);
            int_type.types.push_back(node);
        }
        TExprNode cast_expr;
        cast_expr.node_type = TExprNodeType::CAST_EXPR;
        cast_expr.type = int_type;
        cast_expr.__set_opcode(TExprOpcode::CAST);
        cast_expr.__set_num_children(1);
        cast_expr.__set_output_scale(-1);
        cast_expr.__isset.fn = true;
        cast_expr.fn.name.function_name = "casttodecimalv2";
        cast_expr.fn.binary_type = TFunctionBinaryType::BUILTIN;
        cast_expr.fn.arg_types.push_back(varchar_type);
        cast_expr.fn.ret_type = int_type;
        cast_expr.fn.has_var_args = false;
        cast_expr.fn.__set_signature("casttodecimalv2(VARCHAR(*))");
        cast_expr.fn.__isset.scalar_fn = true;
        cast_expr.fn.scalar_fn.symbol = "doris::DecimalV2Operators::cast_to_decimalv2_val";

        TExprNode slot_ref;
        slot_ref.node_type = TExprNodeType::SLOT_REF;
        slot_ref.type = varchar_type;
        slot_ref.num_children = 0;
        slot_ref.__isset.slot_ref = true;
        slot_ref.slot_ref.slot_id = SRC_TUPLE_SLOT_ID_START + 5; // price id in src tuple
        slot_ref.slot_ref.tuple_id = 1;

        TExpr expr;
        expr.nodes.push_back(cast_expr);
        expr.nodes.push_back(slot_ref);

        _params.expr_of_dest_slot.emplace(DST_TUPLE_SLOT_ID_START + 5, expr);
        _params.src_slot_ids.push_back(SRC_TUPLE_SLOT_ID_START + 5);
    }
    // _params.__isset.expr_of_dest_slot = true;
    _params.__set_dest_tuple_id(TUPLE_ID_DST);
    _params.__set_src_tuple_id(TUPLE_ID_SRC);
}

void AvroScannerTestMap::init() {
    create_expr_info();
    init_desc_table();

    // Node Id
    _tnode.node_id = 0;
    _tnode.node_type = TPlanNodeType::SCHEMA_SCAN_NODE;
    _tnode.num_children = 0;
    _tnode.limit = -1;
    _tnode.row_tuples.push_back(0);
    _tnode.nullable_tuples.push_back(false);
    _tnode.broker_scan_node.tuple_id = 0;
    _tnode.__isset.broker_scan_node = true;
}

// test avro scanner read file
TEST_F(AvroScannerTestMap, read_avro_file_map) {
    BrokerScanNode scan_node(&_obj_pool, _tnode, *_desc_tbl);
    scan_node.init(_tnode);
    auto status = scan_node.prepare(&_runtime_state);
    ASSERT_TRUE(status.ok());

    // set scan range
    std::vector<TScanRangeParams> scan_ranges;
    {
        TScanRangeParams scan_range_params;

        TBrokerScanRange broker_scan_range;
        broker_scan_range.params = _params;
        TBrokerRangeDesc range;
        range.start_offset = 0;
        range.size = -1;
        range.format_type = TFileFormatType::FORMAT_AVRO;
        range.splittable = true;
        range.__isset.avropaths = true;
        range.__isset.read_json_by_line = true;
        range.read_json_by_line = true;
        range.path = "/tmp/jdolap/be/test/exec/test_data/avro_scanner/test2.avsc";
        //range.avropaths = "[\"$.mid\", \"$.src.k1\", \"$.src.k2\", \"$.src.k3\"]";
        //range.avropaths = "[\"$.src.k1\", \"$.src.k2\", \"$.src.k3\", \"$.cur.k4\", \"$.cur.k5\", \"$.cur.k6\"]";
        range.avropaths = "[\"$.src\", \"$.mid\", \"$.db\", \"$.sch\"]";
        range.file_type = TFileType::FILE_LOCAL;
        broker_scan_range.ranges.push_back(range);

        // TBrokerRangeDesc range1;
        // range1.start_offset = 0;
        // range1.size = -1;
        // range1.format_type = TFileFormatType::FORMAT_AVRO;
        // range1.splittable = true;
        // range1.__isset.avropaths = true;
        // range1.path = "/tmp/jdolap/be/test/exec/test_data/avro_scanner/test2.avsc";
        // range1.avropaths = "[\"$.src\", \"$.mid\", \"$.db\", \"$.sch\"]";
        // range1.file_type = TFileType::FILE_LOCAL;
        // broker_scan_range.ranges.push_back(range1);

        scan_range_params.scan_range.__set_broker_scan_range(broker_scan_range);
        scan_ranges.push_back(scan_range_params);
    }
    scan_node.set_scan_ranges(scan_ranges);
    status = scan_node.open(&_runtime_state);
    ASSERT_TRUE(status.ok());

    MemTracker tracker;
    RowBatch batch(scan_node.row_desc(), _runtime_state.batch_size(), &tracker);
    bool eof = false;
    status = scan_node.get_next(&_runtime_state, &batch, &eof);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(1, batch.num_rows());
    ASSERT_FALSE(eof);
    batch.reset();
    scan_node.close(&_runtime_state);
    {
        std::stringstream ss;
        scan_node.runtime_profile()->pretty_print(&ss);
        LOG(INFO) << ss.str();
    }

}
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    doris::CpuInfo::init();
    return RUN_ALL_TESTS();
}
