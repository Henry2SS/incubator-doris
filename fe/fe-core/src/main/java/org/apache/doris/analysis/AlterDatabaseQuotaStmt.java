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

import org.apache.doris.catalog.Env;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.ParseUtil;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Strings;

public class AlterDatabaseQuotaStmt extends DdlStmt {
    private String dbName;
    private QuotaType quotaType;
    private String quotaValue;
    private long quota;
    private String quotaName;

    public enum QuotaType {
        NONE,
        DATA,
        REPLICA,
        TRANSACTION
    }

    public AlterDatabaseQuotaStmt(String dbName, QuotaType quotaType, String quotaValue) {
        this.dbName = dbName;
        this.quotaType = quotaType;
        this.quotaValue = quotaValue;
    }

    public String getDbName() {
        return dbName;
    }

    public long getQuota() {
        return quota;
    }

    public QuotaType getQuotaType() {
        return quotaType;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);

        if (!Env.getCurrentEnv().getAuth().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_DBACCESS_DENIED_ERROR,
                    analyzer.getQualifiedUser(), dbName);
        }

        if (Strings.isNullOrEmpty(dbName)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
        }
        dbName = ClusterNamespace.getFullName(getClusterName(), dbName);
        if (quotaType == QuotaType.DATA) {
            quota = ParseUtil.analyzeDataVolumn(quotaValue);
            quotaName = "DATA";
        } else if (quotaType == QuotaType.REPLICA) {
            quota = ParseUtil.analyzeReplicaNumber(quotaValue);
            quotaName = "REPLICA";
        } else if (quotaType == QuotaType.TRANSACTION) {
            quota = ParseUtil.analyzeTransactionNumber(quotaValue);
            quotaName = "TRANSACTION";
        }

    }

    @Override
    public String toSql() {
        return "ALTER DATABASE " + dbName + " SET "
                + quotaName
                + " QUOTA " + quotaValue;
    }
}
