package com.shuanghe.sparkproject.dao.impl;

import com.shuanghe.sparkproject.dao.ITop10SessionDAO;
import com.shuanghe.sparkproject.domain.Top10Session;
import com.shuanghe.sparkproject.jdbc.JDBCHelper;

/**
 * Description:top10活跃session的DAO实现
 * <p>
 * Date: 2018/05/30
 * Time: 16:09
 *
 * @author yushuanghe
 */
public class Top10SessionDAOImpl implements ITop10SessionDAO {
    @Override
    public void insert(Top10Session top10Session) {
        String sql = "insert into top10_session values(?,?,?,?)";

        Object[] params = new Object[]{top10Session.getTaskid(),
                top10Session.getCategoryid(),
                top10Session.getSessionid(),
                top10Session.getClickCount()};

        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeUpdate(sql, params);
    }
}