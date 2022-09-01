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

package org.apache.doris.analysis;


import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Table.TableType;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.common.VecNotImplException;
import org.apache.doris.common.util.VectorizedUtil;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

/**
 * Wraps a list of TableRef instances that form a FROM clause, allowing them to be
 * analyzed independently of the statement using them. To increase the flexibility of
 * the class it implements the Iterable interface.
 */
public class FromClause implements ParseNode, Iterable<TableRef> {

    private final ArrayList<TableRef> tableRefs_;

    private boolean analyzed_ = false;
    private boolean needToSql = false;

    // the tables positions may be changed by 'join reorder' optimization
    // after reset, the original order information is lost
    // in the next re-analyze phase, the mis-ordered tables may lead to 'unable to find column xxx' error
    // now we use originalTableRefOrders to keep track of table order information
    // so that in reset method, we can recover the original table orders.
    private final ArrayList<TableRef> originalTableRefOrders = new ArrayList<TableRef>();

    public FromClause(List<TableRef> tableRefs) {
        tableRefs_ = Lists.newArrayList(tableRefs);
        // Set left table refs to ensure correct toSql() before analysis.
        for (int i = 1; i < tableRefs_.size(); ++i) {
            tableRefs_.get(i).setLeftTblRef(tableRefs_.get(i - 1));
        }
        // save the tableRef's order, will use in reset method later
        originalTableRefOrders.clear();
        for (int i = 0; i < tableRefs_.size(); ++i) {
            originalTableRefOrders.add(tableRefs_.get(i));
        }
    }

    public FromClause() { tableRefs_ = Lists.newArrayList(); }
    public List<TableRef> getTableRefs() { return tableRefs_; }
    public void setNeedToSql(boolean needToSql) {
        this.needToSql = needToSql;
    }

    private void checkFromHiveTable(Analyzer analyzer) throws AnalysisException {
        for (TableRef tblRef : tableRefs_) {
            if (!(tblRef instanceof BaseTableRef)) {
                continue;
            }

            TableName tableName = tblRef.getName();
            String dbName = tableName.getDb();
            if (Strings.isNullOrEmpty(dbName)) {
                dbName = analyzer.getDefaultDb();
            } else {
                dbName = ClusterNamespace.getFullName(analyzer.getClusterName(), tblRef.getName().getDb());
            }
            if (Strings.isNullOrEmpty(dbName)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
            }

            Database db = analyzer.getCatalog().getDbOrAnalysisException(dbName);
            String tblName = tableName.getTbl();
            Table table = db.getTableOrAnalysisException(tblName);
        }
    }

    /**
     * In some cases, the reorder method of select stmt will incorrectly sort the tableRef with on clause.
     * The meaning of this function is to reset those tableRefs with on clauses.
     * For example:
     * Origin stmt: select * from t1 inner join t2 on t1.k1=t2.k1
     * After analyze: select * from t2 on t1.k1=t2.k1 inner join t1
     *
     * If this statement just needs to be reanalyze (query rewriter), an error will be reported
     * because the table t1 in the on clause cannot be recognized.
     */
    private void sortTableRefKeepSequenceOfOnClause() {
        Collections.sort(this.tableRefs_, new Comparator<TableRef>() {
            @Override
            public int compare(TableRef tableref1, TableRef tableref2) {
                int i1 = 0;
                int i2 = 0;
                if (tableref1.getOnClause() != null || tableref1.getUsingClause() != null) {
                    i1 = 1;
                }
                if (tableref2.getOnClause() != null || tableref2.getUsingClause() != null) {
                    i2 = 1;
                }
                return i1 - i2;
            }
        });
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
        if (analyzed_) return;

        if (tableRefs_.isEmpty()) {
            analyzed_ = true;
            return;
        }

        // The order of the tables may have changed during the previous analyzer process.
        // For example, a join b on xxx is changed to b on xxx join a.
        // This change will cause the predicate in on clause be adjusted to the front of the association table,
        // causing semantic analysis to fail. Unknown column 'column1' in 'table1'
        // So we need to readjust the order of the tables here.
        if (analyzer.enableStarJoinReorder()) {
            sortTableRefKeepSequenceOfOnClause();
        }

        // Start out with table refs to establish aliases.
        TableRef leftTblRef = null;  // the one to the left of tblRef
        for (int i = 0; i < tableRefs_.size(); ++i) {
            // Resolve and replace non-InlineViewRef table refs with a BaseTableRef or ViewRef.
            TableRef tblRef = tableRefs_.get(i);
            tblRef = analyzer.resolveTableRef(tblRef);
            tableRefs_.set(i, Preconditions.checkNotNull(tblRef));
            tblRef.setLeftTblRef(leftTblRef);
            if (tblRef instanceof InlineViewRef) {
                ((InlineViewRef) tblRef).setNeedToSql(needToSql);
            }
            tblRef.analyze(analyzer);
            leftTblRef = tblRef;
        }
        checkExternalTable(analyzer);

        // Fix the problem of column nullable attribute error caused by inline view + outer join
        changeTblRefToNullable(analyzer);

        // TODO: remove when query from hive table is supported
        checkFromHiveTable(analyzer);

        analyzed_ = true;

        // save the tableRef's order, will use in reset method later
        originalTableRefOrders.clear();
        for (int i = 0; i < tableRefs_.size(); ++i) {
            originalTableRefOrders.add(tableRefs_.get(i));
        }
    }

    private void checkExternalTable(Analyzer analyzer) throws UserException {
        for (TableRef tblRef : tableRefs_) {
            if (!(tblRef instanceof BaseTableRef)) {
                continue;
            }

            TableName tableName = tblRef.getName();
            String dbName = tableName.getDb();
            if (Strings.isNullOrEmpty(dbName)) {
                dbName = analyzer.getDefaultDb();
            } else {
                dbName = ClusterNamespace.getFullName(analyzer.getClusterName(), tblRef.getName().getDb());
            }
            if (Strings.isNullOrEmpty(dbName)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
            }

            Database db = analyzer.getCatalog().getDbOrAnalysisException(dbName);
            String tblName = tableName.getTbl();
            Table table = db.getTableOrAnalysisException(tblName);
            if (VectorizedUtil.isVectorized()) {
                if (table.getType() == TableType.BROKER || table.getType() == TableType.HIVE
                        || table.getType() == TableType.ICEBERG) {
                    throw new VecNotImplException("Not support table type " + table.getType() + " in vec engine");
                }
            }
        }
    }

    // set null-side inlinve view column
    // For example: select * from (select a as k1 from t) tmp right join b on tmp.k1=b.k1
    // The columns from tmp should be nullable.
    // The table ref tmp will be used by HashJoinNode.computeOutputTuple()
    private void changeTblRefToNullable(Analyzer analyzer) {
        for (TableRef tableRef : tableRefs_) {
            if (!(tableRef instanceof InlineViewRef)) {
                continue;
            }
            InlineViewRef inlineViewRef = (InlineViewRef) tableRef;
            if (analyzer.isOuterJoined(inlineViewRef.getId())) {
                for (SlotDescriptor slotDescriptor : inlineViewRef.getDesc().getSlots()) {
                    slotDescriptor.setIsNullable(true);
                }
            }
        }
    }

    public FromClause clone() {
        ArrayList<TableRef> clone = Lists.newArrayList();
        for (TableRef tblRef : tableRefs_) {
            clone.add(tblRef.clone());
        }

        FromClause result = new FromClause(clone);
        for (int i = 0; i < clone.size(); ++i) {
            result.originalTableRefOrders.add(clone.get(i));
        }
        return result;
    }

    public void reset() {
        for (int i = 0; i < size(); ++i) {
            TableRef origTblRef = get(i);
            // TODO(zc):
            // if (origTblRef.isResolved() && !(origTblRef instanceof InlineViewRef)) {
            //     // Replace resolved table refs with unresolved ones.
            //     TableRef newTblRef = new TableRef(origTblRef);
            //     // Use the fully qualified raw path to preserve the original resolution.
            //     // Otherwise, non-fully qualified paths might incorrectly match a local view.
            //     // TODO for 2.3: This full qualification preserves analysis state which is
            //     // contrary to the intended semantics of reset(). We could address this issue by
            //     // changing the WITH-clause analysis to register local views that have
            //     // fully-qualified table refs, and then remove the full qualification here.
            //     newTblRef.rawPath_ = origTblRef.getResolvedPath().getFullyQualifiedRawPath();
            //     set(i, newTblRef);
            // }
            get(i).reset();
        }
        // recover original table orders
        for (int i = 0; i < size(); ++i) {
            tableRefs_.set(i, originalTableRefOrders.get(i));
        }
        this.analyzed_ = false;
    }

    @Override
    public String toSql() {
        StringBuilder builder = new StringBuilder();
        if (!tableRefs_.isEmpty()) {
            builder.append(" FROM");
            for (int i = 0; i < tableRefs_.size(); ++i) {
                builder.append(" " + tableRefs_.get(i).toSql());
            }
        }
        return builder.toString();
    }

    public boolean isEmpty() { return tableRefs_.isEmpty(); }

    @Override
    public Iterator<TableRef> iterator() { return tableRefs_.iterator(); }
    public int size() { return tableRefs_.size(); }
    public TableRef get(int i) { return tableRefs_.get(i); }
    public void set(int i, TableRef tableRef) {
        tableRefs_.set(i, tableRef);
        originalTableRefOrders.set(i, tableRef);
    }
    public void add(TableRef t) { 
        tableRefs_.add(t); 
        // join reorder will call add method after call clear method.
        // we want to keep tableRefPositions unchanged in that case
        // in other cases, tableRefs_.size() would larger than tableRefPositions.size()
        // then we can update tableRefPositions. same logic in addAll method.
        if (tableRefs_.size() > originalTableRefOrders.size()) {
            originalTableRefOrders.add(t);
        }
    }
    public void addAll(List<TableRef> t) { 
        tableRefs_.addAll(t); 
        if (tableRefs_.size() > originalTableRefOrders.size()) {
            for (int i = originalTableRefOrders.size(); i < tableRefs_.size(); ++i) {
                originalTableRefOrders.add(tableRefs_.get(i));
            }
        }
    }
    public void clear() { tableRefs_.clear(); }
}
