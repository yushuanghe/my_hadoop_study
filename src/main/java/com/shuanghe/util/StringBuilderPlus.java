package com.shuanghe.util;

import org.apache.commons.lang.StringUtils;

/**
 * Description:
 * <p>
 * Date: 2018/06/29
 * Time: 17:24
 *
 * @author yushuanghe
 */
public class StringBuilderPlus {

    private StringBuilder builder;

    public StringBuilderPlus() {
        builder = new StringBuilder();
    }

    public StringBuilder getStringBuilder() {
        return builder;
    }

    public StringBuilderPlus append(String add) {
        if (StringUtils.isNotBlank(add)) {
            builder.append(add);
        }
        return this;
    }

    @Override
    public String toString() {
        return builder.toString();
    }

}