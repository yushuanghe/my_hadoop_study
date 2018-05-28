package com.shuanghe.sparkproject.dao;

import com.shuanghe.sparkproject.domain.SessionAggrStat;

public interface ISessionAggrStatDAO {
    /**
     * 插入session聚合统计结果
     *
     * @param sessionAggrStat
     */
    void insert(SessionAggrStat sessionAggrStat);
}
