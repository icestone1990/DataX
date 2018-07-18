package com.alibaba.datax.plugin.reader.elasticsearchreader.util;

import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.StringUtils;

import java.text.DateFormat;
import java.text.SimpleDateFormat;

/**
 *
 * @author Stone
 * @date 2018/4/12 15:35
 * @param
 * @return
 * @desc 配置的column的映射类
 *
 */
public class ColumnEntry {

    private String name;
    private String type;
    private String format;
    private DateFormat dateParse;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
        if (StringUtils.isNotBlank(this.format)) {
            this.dateParse = new SimpleDateFormat(this.format);
        }
    }

    public DateFormat getDateFormat() {
        return this.dateParse;
    }

    public String toJSONString() {
        return ColumnEntry.toJSONString(this);
    }

    public static String toJSONString(ColumnEntry columnEntry) {
        return JSON.toJSONString(columnEntry);
    }
}
