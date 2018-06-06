package com.shuanghe.sparkproject.dao.impl;

import com.shuanghe.sparkproject.dao.IPageSplitConvertRateDAO;
import com.shuanghe.sparkproject.domain.PageSplitConvertRate;
import com.shuanghe.sparkproject.jdbc.JDBCHelper;

/**
 * Description:页面切片转化率DAO实现类
 * <p>
 * Date: 2018/06/06
 * Time: 18:23
 *
 * @author yushuanghe
 */
public class PageSplitConvertRateDAOImpl implements IPageSplitConvertRateDAO {
    @Override
    public void insert(PageSplitConvertRate pageSplitConvertRate) {
        String sql = "insert into page_split_convert_rate values(?,?)";
        Object[] params = new Object[]{pageSplitConvertRate.getTaskid(),
                pageSplitConvertRate.getConvertRate()};

        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeUpdate(sql, params);
    }
}