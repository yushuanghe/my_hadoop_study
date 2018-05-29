package com.shuanghe.sparkproject.dao.impl;

import com.shuanghe.sparkproject.dao.ISessionRandomExtractDAO;
import com.shuanghe.sparkproject.domain.SessionRandomExtract;
import com.shuanghe.sparkproject.jdbc.JDBCHelper;

/**
 * Description:随机抽取session的DAO实现
 * <p>
 * Date: 2018/05/29
 * Time: 19:51
 *
 * @author yushuanghe
 */
public class SessionRandomExtractDAOImpl implements ISessionRandomExtractDAO {
    @Override
    public void insert(SessionRandomExtract sessionRandomExtract) {
        String sql = "insert into session_random_extract values(?,?,?,?,?)";

        Object[] params = new Object[]{sessionRandomExtract.getTaskid(),
                sessionRandomExtract.getSessionid(),
                sessionRandomExtract.getStartTime(),
                sessionRandomExtract.getSearchKeywords(),
                sessionRandomExtract.getClickCategoryIds()};

        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeUpdate(sql, params);
    }
}