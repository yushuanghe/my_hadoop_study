package com.shuanghe.sparkproject.dao.impl;

import com.shuanghe.sparkproject.dao.ISessionDetailDAO;
import com.shuanghe.sparkproject.domain.SessionDetail;
import com.shuanghe.sparkproject.jdbc.JDBCHelper;

import java.util.ArrayList;
import java.util.List;

/**
 * Description:session明细DAO实现类
 * <p>
 * Date: 2018/05/29
 * Time: 20:09
 *
 * @author yushuanghe
 */
public class SessionDetailDAOImpl implements ISessionDetailDAO {
    /**
     * 插入一条session明细数据
     *
     * @param sessionDetail
     */
    @Override
    public void insert(SessionDetail sessionDetail) {
        String sql = "insert into session_detail values(?,?,?,?,?,?,?,?,?,?,?,?)";

        Long clickCategoryId = sessionDetail.getClickCategoryId();
        if (clickCategoryId == -1) {
            clickCategoryId = null;
        }

        Long clickProductId = sessionDetail.getClickProductId();
        if (clickProductId == -1) {
            clickProductId = null;
        }

        Object[] params = new Object[]{sessionDetail.getTaskid(),
                sessionDetail.getUserid(),
                sessionDetail.getSessionid(),
                sessionDetail.getPageid(),
                sessionDetail.getActionTime(),
                sessionDetail.getSearchKeyword(),
                clickCategoryId,
                clickProductId,
                sessionDetail.getOrderCategoryIds(),
                sessionDetail.getOrderProductIds(),
                sessionDetail.getPayCategoryIds(),
                sessionDetail.getPayProductIds()};

        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeUpdate(sql, params);
    }

    @Override
    public void insertBatch(List<SessionDetail> sessionDetails) {
        String sql = "insert into session_detail values(?,?,?,?,?,?,?,?,?,?,?,?)";

        List<Object[]> paramsList = new ArrayList<>();
        for (SessionDetail sessionDetail : sessionDetails) {
            Long clickCategoryId = sessionDetail.getClickCategoryId();
            if (clickCategoryId == -1) {
                clickCategoryId = null;
            }

            Long clickProductId = sessionDetail.getClickProductId();
            if (clickProductId == -1) {
                clickProductId = null;
            }

            Object[] params = new Object[]{sessionDetail.getTaskid(),
                    sessionDetail.getUserid(),
                    sessionDetail.getSessionid(),
                    sessionDetail.getPageid(),
                    sessionDetail.getActionTime(),
                    sessionDetail.getSearchKeyword(),
                    clickCategoryId,
                    clickProductId,
                    sessionDetail.getOrderCategoryIds(),
                    sessionDetail.getOrderProductIds(),
                    sessionDetail.getPayCategoryIds(),
                    sessionDetail.getPayProductIds()};
            paramsList.add(params);
        }

        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeBatch(sql, paramsList);
    }
}