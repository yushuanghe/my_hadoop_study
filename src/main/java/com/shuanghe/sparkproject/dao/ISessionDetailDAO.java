package com.shuanghe.sparkproject.dao;

import com.shuanghe.sparkproject.domain.SessionDetail;

import java.util.List;

/**
 * Description:Session明细DAO接口
 * <p>
 * Date: 2018/05/29
 * Time: 20:08
 *
 * @author yushuanghe
 */
public interface ISessionDetailDAO {

    /**
     * 插入一条session明细数据
     *
     * @param sessionDetail
     */
    void insert(SessionDetail sessionDetail);

    /**
     * 批量插入session明细数据
     *
     * @param sessionDetails
     */
    void insertBatch(List<SessionDetail> sessionDetails);
}
