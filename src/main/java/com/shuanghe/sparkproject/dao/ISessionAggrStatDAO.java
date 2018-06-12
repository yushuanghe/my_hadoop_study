package com.shuanghe.sparkproject.dao;

import com.shuanghe.sparkproject.domain.SessionAggrStat;

/**
 * session聚合统计模块DAO接口
 *
 * @author yushuanghe
 */
public interface ISessionAggrStatDAO {
    /**
     * 插入session聚合统计结果
     *
     * @param sessionAggrStat
     */
    void insert(SessionAggrStat sessionAggrStat);
}
