package com.shuanghe.sparkproject.dao;

import com.shuanghe.sparkproject.domain.SessionRandomExtract;

/**
 * Description:session随机抽取模块DAO接口
 * Date: 2018/05/29
 * Time: 19:46
 *
 * @author yushuanghe
 */
public interface ISessionRandomExtractDAO {
    /**
     * 插入session随机抽取
     *
     * @param sessionRandomExtract
     */
    void insert(SessionRandomExtract sessionRandomExtract);
}