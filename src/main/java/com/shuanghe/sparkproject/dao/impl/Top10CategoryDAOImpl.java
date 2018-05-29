package com.shuanghe.sparkproject.dao.impl;

import com.shuanghe.sparkproject.dao.ITop10CategoryDAO;
import com.shuanghe.sparkproject.domain.Top10Category;
import com.shuanghe.sparkproject.jdbc.JDBCHelper;

/**
 * Description:top10品类DAO实现
 * <p>
 * Date: 2018/05/30
 * Time: 00:15
 *
 * @author yushuanghe
 */
public class Top10CategoryDAOImpl implements ITop10CategoryDAO {
    @Override
    public void insert(Top10Category category) {
        String sql = "insert into top10_category values(?,?,?,?,?)";

        Object[] params = new Object[]{category.getTaskid(),
                category.getCategoryid(),
                category.getClickCount(),
                category.getOrderCount(),
                category.getPayCount()};

        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeUpdate(sql, params);
    }
}