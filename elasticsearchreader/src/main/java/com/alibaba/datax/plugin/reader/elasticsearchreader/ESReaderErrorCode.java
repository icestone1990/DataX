package com.alibaba.datax.plugin.reader.elasticsearchreader;

import com.alibaba.datax.common.spi.ErrorCode;

/**
 * @author Stone
 */
public enum ESReaderErrorCode implements ErrorCode {

    BAD_CONFIG_VALUE("ESReader-00", "您配置的值不合法."),
    REQUIRED_VALUE("ESReader-01", "您缺失了必须填写的参数值."),
    NOT_SUPPORT_TYPE("ESReader-02", "类型配置值不合法");

    private final String code;
    private final String description;

    ESReaderErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return this.code;
    }

    @Override
    public String getDescription() {
        return this.description;
    }

    @Override
    public String toString() {
        return String.format("Code:[%s], Description:[%s]. ", this.code,
                this.description);
    }
}