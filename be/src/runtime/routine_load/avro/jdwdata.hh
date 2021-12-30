/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


#ifndef JDWDATA_HH_3142537978__H_
#define JDWDATA_HH_3142537978__H_


#include <sstream>
#include "boost/any.hpp"
#include "avro/Specific.hh"
#include "avro/Encoder.hh"
#include "avro/Decoder.hh"

namespace doris {
struct _jdwdata_json_Union__0__ {
private:
    size_t idx_;
    boost::any value_;
public:
    size_t idx() const { return idx_; }
    std::string get_string() const;
    void set_string(const std::string& v);
    bool is_null() const {
        return (idx_ == 1);
    }
    void set_null() {
        idx_ = 1;
        value_ = boost::any();
    }
    _jdwdata_json_Union__0__();
};

struct _jdwdata_json_Union__1__ {
private:
    size_t idx_;
    boost::any value_;
public:
    size_t idx() const { return idx_; }
    std::string get_string() const;
    void set_string(const std::string& v);
    bool is_null() const {
        return (idx_ == 1);
    }
    void set_null() {
        idx_ = 1;
        value_ = boost::any();
    }
    _jdwdata_json_Union__1__();
};

struct _jdwdata_json_Union__2__ {
private:
    size_t idx_;
    boost::any value_;
public:
    size_t idx() const { return idx_; }
    std::string get_string() const;
    void set_string(const std::string& v);
    bool is_null() const {
        return (idx_ == 1);
    }
    void set_null() {
        idx_ = 1;
        value_ = boost::any();
    }
    _jdwdata_json_Union__2__();
};

struct _jdwdata_json_Union__3__ {
private:
    size_t idx_;
    boost::any value_;
public:
    size_t idx() const { return idx_; }
    std::map<std::string, _jdwdata_json_Union__2__ > get_map() const;
    void set_map(const std::map<std::string, _jdwdata_json_Union__2__ >& v);
    bool is_null() const {
        return (idx_ == 1);
    }
    void set_null() {
        idx_ = 1;
        value_ = boost::any();
    }
    _jdwdata_json_Union__3__();
};

struct _jdwdata_json_Union__4__ {
private:
    size_t idx_;
    boost::any value_;
public:
    size_t idx() const { return idx_; }
    std::string get_string() const;
    void set_string(const std::string& v);
    bool is_null() const {
        return (idx_ == 1);
    }
    void set_null() {
        idx_ = 1;
        value_ = boost::any();
    }
    _jdwdata_json_Union__4__();
};

struct _jdwdata_json_Union__5__ {
private:
    size_t idx_;
    boost::any value_;
public:
    size_t idx() const { return idx_; }
    std::map<std::string, _jdwdata_json_Union__4__ > get_map() const;
    void set_map(const std::map<std::string, _jdwdata_json_Union__4__ >& v);
    bool is_null() const {
        return (idx_ == 1);
    }
    void set_null() {
        idx_ = 1;
        value_ = boost::any();
    }
    _jdwdata_json_Union__5__();
};

struct _jdwdata_json_Union__6__ {
private:
    size_t idx_;
    boost::any value_;
public:
    size_t idx() const { return idx_; }
    std::string get_string() const;
    void set_string(const std::string& v);
    bool is_null() const {
        return (idx_ == 1);
    }
    void set_null() {
        idx_ = 1;
        value_ = boost::any();
    }
    _jdwdata_json_Union__6__();
};

struct _jdwdata_json_Union__7__ {
private:
    size_t idx_;
    boost::any value_;
public:
    size_t idx() const { return idx_; }
    std::map<std::string, _jdwdata_json_Union__6__ > get_map() const;
    void set_map(const std::map<std::string, _jdwdata_json_Union__6__ >& v);
    bool is_null() const {
        return (idx_ == 1);
    }
    void set_null() {
        idx_ = 1;
        value_ = boost::any();
    }
    _jdwdata_json_Union__7__();
};

struct JdwData {
    typedef _jdwdata_json_Union__0__ ddl_t;
    typedef _jdwdata_json_Union__1__ err_t;
    typedef _jdwdata_json_Union__3__ src_t;
    typedef _jdwdata_json_Union__5__ cur_t;
    typedef _jdwdata_json_Union__7__ cus_t;
    int64_t mid;
    std::string db;
    std::string sch;
    std::string tab;
    std::string opt;
    int64_t ts;
    ddl_t ddl;
    err_t err;
    src_t src;
    cur_t cur;
    cus_t cus;
    JdwData() :
        mid(int64_t()),
        db(std::string()),
        sch(std::string()),
        tab(std::string()),
        opt(std::string()),
        ts(int64_t()),
        ddl(ddl_t()),
        err(err_t()),
        src(src_t()),
        cur(cur_t()),
        cus(cus_t())
        { }
};

inline
std::string _jdwdata_json_Union__0__::get_string() const {
    if (idx_ != 0) {
        throw avro::Exception("Invalid type for union");
    }
    return boost::any_cast<std::string >(value_);
}

inline
void _jdwdata_json_Union__0__::set_string(const std::string& v) {
    idx_ = 0;
    value_ = v;
}

inline
std::string _jdwdata_json_Union__1__::get_string() const {
    if (idx_ != 0) {
        throw avro::Exception("Invalid type for union");
    }
    return boost::any_cast<std::string >(value_);
}

inline
void _jdwdata_json_Union__1__::set_string(const std::string& v) {
    idx_ = 0;
    value_ = v;
}

inline
std::string _jdwdata_json_Union__2__::get_string() const {
    if (idx_ != 0) {
        throw avro::Exception("Invalid type for union");
    }
    return boost::any_cast<std::string >(value_);
}

inline
void _jdwdata_json_Union__2__::set_string(const std::string& v) {
    idx_ = 0;
    value_ = v;
}

inline
std::map<std::string, _jdwdata_json_Union__2__ > _jdwdata_json_Union__3__::get_map() const {
    if (idx_ != 0) {
        throw avro::Exception("Invalid type for union");
    }
    return boost::any_cast<std::map<std::string, _jdwdata_json_Union__2__ > >(value_);
}

inline
void _jdwdata_json_Union__3__::set_map(const std::map<std::string, _jdwdata_json_Union__2__ >& v) {
    idx_ = 0;
    value_ = v;
}

inline
std::string _jdwdata_json_Union__4__::get_string() const {
    if (idx_ != 0) {
        throw avro::Exception("Invalid type for union");
    }
    return boost::any_cast<std::string >(value_);
}

inline
void _jdwdata_json_Union__4__::set_string(const std::string& v) {
    idx_ = 0;
    value_ = v;
}

inline
std::map<std::string, _jdwdata_json_Union__4__ > _jdwdata_json_Union__5__::get_map() const {
    if (idx_ != 0) {
        throw avro::Exception("Invalid type for union");
    }
    return boost::any_cast<std::map<std::string, _jdwdata_json_Union__4__ > >(value_);
}

inline
void _jdwdata_json_Union__5__::set_map(const std::map<std::string, _jdwdata_json_Union__4__ >& v) {
    idx_ = 0;
    value_ = v;
}

inline
std::string _jdwdata_json_Union__6__::get_string() const {
    if (idx_ != 0) {
        throw avro::Exception("Invalid type for union");
    }
    return boost::any_cast<std::string >(value_);
}

inline
void _jdwdata_json_Union__6__::set_string(const std::string& v) {
    idx_ = 0;
    value_ = v;
}

inline
std::map<std::string, _jdwdata_json_Union__6__ > _jdwdata_json_Union__7__::get_map() const {
    if (idx_ != 0) {
        throw avro::Exception("Invalid type for union");
    }
    return boost::any_cast<std::map<std::string, _jdwdata_json_Union__6__ > >(value_);
}

inline
void _jdwdata_json_Union__7__::set_map(const std::map<std::string, _jdwdata_json_Union__6__ >& v) {
    idx_ = 0;
    value_ = v;
}

inline _jdwdata_json_Union__0__::_jdwdata_json_Union__0__() : idx_(0), value_(std::string()) { }
inline _jdwdata_json_Union__1__::_jdwdata_json_Union__1__() : idx_(0), value_(std::string()) { }
inline _jdwdata_json_Union__2__::_jdwdata_json_Union__2__() : idx_(0), value_(std::string()) { }
inline _jdwdata_json_Union__3__::_jdwdata_json_Union__3__() : idx_(0), value_(std::map<std::string, _jdwdata_json_Union__2__ >()) { }
inline _jdwdata_json_Union__4__::_jdwdata_json_Union__4__() : idx_(0), value_(std::string()) { }
inline _jdwdata_json_Union__5__::_jdwdata_json_Union__5__() : idx_(0), value_(std::map<std::string, _jdwdata_json_Union__4__ >()) { }
inline _jdwdata_json_Union__6__::_jdwdata_json_Union__6__() : idx_(0), value_(std::string()) { }
inline _jdwdata_json_Union__7__::_jdwdata_json_Union__7__() : idx_(0), value_(std::map<std::string, _jdwdata_json_Union__6__ >()) { }
}
namespace avro {
template<> struct codec_traits<doris::_jdwdata_json_Union__0__> {
    static void encode(Encoder& e, doris::_jdwdata_json_Union__0__ v) {
        e.encodeUnionIndex(v.idx());
        switch (v.idx()) {
        case 0:
            avro::encode(e, v.get_string());
            break;
        case 1:
            e.encodeNull();
            break;
        }
    }
    static void decode(Decoder& d, doris::_jdwdata_json_Union__0__& v) {
        size_t n = d.decodeUnionIndex();
        if (n >= 2) { throw avro::Exception("Union index too big"); }
        switch (n) {
        case 0:
            {
                std::string vv;
                avro::decode(d, vv);
                v.set_string(vv);
            }
            break;
        case 1:
            d.decodeNull();
            v.set_null();
            break;
        }
    }
};

template<> struct codec_traits<doris::_jdwdata_json_Union__1__> {
    static void encode(Encoder& e, doris::_jdwdata_json_Union__1__ v) {
        e.encodeUnionIndex(v.idx());
        switch (v.idx()) {
        case 0:
            avro::encode(e, v.get_string());
            break;
        case 1:
            e.encodeNull();
            break;
        }
    }
    static void decode(Decoder& d, doris::_jdwdata_json_Union__1__& v) {
        size_t n = d.decodeUnionIndex();
        if (n >= 2) { throw avro::Exception("Union index too big"); }
        switch (n) {
        case 0:
            {
                std::string vv;
                avro::decode(d, vv);
                v.set_string(vv);
            }
            break;
        case 1:
            d.decodeNull();
            v.set_null();
            break;
        }
    }
};

template<> struct codec_traits<doris::_jdwdata_json_Union__2__> {
    static void encode(Encoder& e, doris::_jdwdata_json_Union__2__ v) {
        e.encodeUnionIndex(v.idx());
        switch (v.idx()) {
        case 0:
            avro::encode(e, v.get_string());
            break;
        case 1:
            e.encodeNull();
            break;
        }
    }
    static void decode(Decoder& d, doris::_jdwdata_json_Union__2__& v) {
        size_t n = d.decodeUnionIndex();
        if (n >= 2) { throw avro::Exception("Union index too big"); }
        switch (n) {
        case 0:
            {
                std::string vv;
                avro::decode(d, vv);
                v.set_string(vv);
            }
            break;
        case 1:
            d.decodeNull();
            v.set_null();
            break;
        }
    }
};

template<> struct codec_traits<doris::_jdwdata_json_Union__3__> {
    static void encode(Encoder& e, doris::_jdwdata_json_Union__3__ v) {
        e.encodeUnionIndex(v.idx());
        switch (v.idx()) {
        case 0:
            avro::encode(e, v.get_map());
            break;
        case 1:
            e.encodeNull();
            break;
        }
    }
    static void decode(Decoder& d, doris::_jdwdata_json_Union__3__& v) {
        size_t n = d.decodeUnionIndex();
        if (n >= 2) { throw avro::Exception("Union index too big"); }
        switch (n) {
        case 0:
            {
                std::map<std::string, doris::_jdwdata_json_Union__2__ > vv;
                avro::decode(d, vv);
                v.set_map(vv);
            }
            break;
        case 1:
            d.decodeNull();
            v.set_null();
            break;
        }
    }
};

template<> struct codec_traits<doris::_jdwdata_json_Union__4__> {
    static void encode(Encoder& e, doris::_jdwdata_json_Union__4__ v) {
        e.encodeUnionIndex(v.idx());
        switch (v.idx()) {
        case 0:
            avro::encode(e, v.get_string());
            break;
        case 1:
            e.encodeNull();
            break;
        }
    }
    static void decode(Decoder& d, doris::_jdwdata_json_Union__4__& v) {
        size_t n = d.decodeUnionIndex();
        if (n >= 2) { throw avro::Exception("Union index too big"); }
        switch (n) {
        case 0:
            {
                std::string vv;
                avro::decode(d, vv);
                v.set_string(vv);
            }
            break;
        case 1:
            d.decodeNull();
            v.set_null();
            break;
        }
    }
};

template<> struct codec_traits<doris::_jdwdata_json_Union__5__> {
    static void encode(Encoder& e, doris::_jdwdata_json_Union__5__ v) {
        e.encodeUnionIndex(v.idx());
        switch (v.idx()) {
        case 0:
            avro::encode(e, v.get_map());
            break;
        case 1:
            e.encodeNull();
            break;
        }
    }
    static void decode(Decoder& d, doris::_jdwdata_json_Union__5__& v) {
        size_t n = d.decodeUnionIndex();
        if (n >= 2) { throw avro::Exception("Union index too big"); }
        switch (n) {
        case 0:
            {
                std::map<std::string, doris::_jdwdata_json_Union__4__ > vv;
                avro::decode(d, vv);
                v.set_map(vv);
            }
            break;
        case 1:
            d.decodeNull();
            v.set_null();
            break;
        }
    }
};

template<> struct codec_traits<doris::_jdwdata_json_Union__6__> {
    static void encode(Encoder& e, doris::_jdwdata_json_Union__6__ v) {
        e.encodeUnionIndex(v.idx());
        switch (v.idx()) {
        case 0:
            avro::encode(e, v.get_string());
            break;
        case 1:
            e.encodeNull();
            break;
        }
    }
    static void decode(Decoder& d, doris::_jdwdata_json_Union__6__& v) {
        size_t n = d.decodeUnionIndex();
        if (n >= 2) { throw avro::Exception("Union index too big"); }
        switch (n) {
        case 0:
            {
                std::string vv;
                avro::decode(d, vv);
                v.set_string(vv);
            }
            break;
        case 1:
            d.decodeNull();
            v.set_null();
            break;
        }
    }
};

template<> struct codec_traits<doris::_jdwdata_json_Union__7__> {
    static void encode(Encoder& e, doris::_jdwdata_json_Union__7__ v) {
        e.encodeUnionIndex(v.idx());
        switch (v.idx()) {
        case 0:
            avro::encode(e, v.get_map());
            break;
        case 1:
            e.encodeNull();
            break;
        }
    }
    static void decode(Decoder& d, doris::_jdwdata_json_Union__7__& v) {
        size_t n = d.decodeUnionIndex();
        if (n >= 2) { throw avro::Exception("Union index too big"); }
        switch (n) {
        case 0:
            {
                std::map<std::string, doris::_jdwdata_json_Union__6__ > vv;
                avro::decode(d, vv);
                v.set_map(vv);
            }
            break;
        case 1:
            d.decodeNull();
            v.set_null();
            break;
        }
    }
};

template<> struct codec_traits<doris::JdwData> {
    static void encode(Encoder& e, const doris::JdwData& v) {
        avro::encode(e, v.mid);
        avro::encode(e, v.db);
        avro::encode(e, v.sch);
        avro::encode(e, v.tab);
        avro::encode(e, v.opt);
        avro::encode(e, v.ts);
        avro::encode(e, v.ddl);
        avro::encode(e, v.err);
        avro::encode(e, v.src);
        avro::encode(e, v.cur);
        avro::encode(e, v.cus);
    }
    static void decode(Decoder& d, doris::JdwData& v) {
        if (avro::ResolvingDecoder *rd =
            dynamic_cast<avro::ResolvingDecoder *>(&d)) {
            const std::vector<size_t> fo = rd->fieldOrder();
            for (std::vector<size_t>::const_iterator it = fo.begin();
                it != fo.end(); ++it) {
                switch (*it) {
                case 0:
                    avro::decode(d, v.mid);
                    break;
                case 1:
                    avro::decode(d, v.db);
                    break;
                case 2:
                    avro::decode(d, v.sch);
                    break;
                case 3:
                    avro::decode(d, v.tab);
                    break;
                case 4:
                    avro::decode(d, v.opt);
                    break;
                case 5:
                    avro::decode(d, v.ts);
                    break;
                case 6:
                    avro::decode(d, v.ddl);
                    break;
                case 7:
                    avro::decode(d, v.err);
                    break;
                case 8:
                    avro::decode(d, v.src);
                    break;
                case 9:
                    avro::decode(d, v.cur);
                    break;
                case 10:
                    avro::decode(d, v.cus);
                    break;
                default:
                    break;
                }
            }
        } else {
            avro::decode(d, v.mid);
            avro::decode(d, v.db);
            avro::decode(d, v.sch);
            avro::decode(d, v.tab);
            avro::decode(d, v.opt);
            avro::decode(d, v.ts);
            avro::decode(d, v.ddl);
            avro::decode(d, v.err);
            avro::decode(d, v.src);
            avro::decode(d, v.cur);
            avro::decode(d, v.cus);
        }
    }
};

}
#endif
