package com.shuanghe.sparkproject.dao.impl;

import com.shuanghe.sparkproject.dao.IAdUserClickCountDAO;
import com.shuanghe.sparkproject.domain.AdUserClickCount;
import com.shuanghe.sparkproject.jdbc.JdbcManager;
import com.shuanghe.sparkproject.model.AdUserClickCountQueryResult;

import java.util.ArrayList;
import java.util.List;

/**
 * Description:用户广告点击量DAO实现类
 * <p>
 * Date: 2018/06/08
 * Time: 16:25
 *
 * @author yushuanghe
 */
public class AdUserClickCountDAOImpl implements IAdUserClickCountDAO {
    @Override
    public void updateBatch(List<AdUserClickCount> adUserClickCounts) {
        //JDBCHelper jdbcHelper = JDBCHelper.getInstance();

        // 首先对用户广告点击量进行分类，分成待插入的和待更新的
        List<AdUserClickCount> insertAdUserClickCounts = new ArrayList<>();
        List<AdUserClickCount> updateAdUserClickCounts = new ArrayList<>();

        String selectSQL = "SELECT count(1) FROM ad_user_click_count "
                + "WHERE date=? AND user_id=? AND ad_id=? ";
        Object[] selectParams = null;

        for (AdUserClickCount adUserClickCount : adUserClickCounts) {
            final AdUserClickCountQueryResult queryResult = new AdUserClickCountQueryResult();

            selectParams = new Object[]{adUserClickCount.getDate(),
                    adUserClickCount.getUserid(), adUserClickCount.getAdid()};

            //jdbcHelper.executeQuery(selectSQL, selectParams, rs -> {
            //    if (rs.next()) {
            //        int count = rs.getInt(1);
            //        queryResult.setCount(count);
            //    }
            //});
            JdbcManager.executeQuery(selectSQL, selectParams, rs -> {
                if (rs.next()) {
                    int count = rs.getInt(1);
                    queryResult.setCount(count);
                }
            });

            int count = queryResult.getCount();

            if (count > 0) {
                updateAdUserClickCounts.add(adUserClickCount);
            } else {
                insertAdUserClickCounts.add(adUserClickCount);
            }
        }

        // 执行批量插入
        String insertSQL = "INSERT INTO ad_user_click_count VALUES(?,?,?,?)";
        List<Object[]> insertParamsList = new ArrayList<>();

        for (AdUserClickCount adUserClickCount : insertAdUserClickCounts) {
            Object[] insertParams = new Object[]{adUserClickCount.getDate(),
                    adUserClickCount.getUserid(),
                    adUserClickCount.getAdid(),
                    adUserClickCount.getClickCount()};
            insertParamsList.add(insertParams);
        }

        //jdbcHelper.executeBatch(insertSQL, insertParamsList);
        JdbcManager.executeBatch(insertSQL, insertParamsList);

        // 执行批量更新
        String updateSQL = "UPDATE ad_user_click_count SET click_count=? "
                + "WHERE date=? AND user_id=? AND ad_id=? ";
        List<Object[]> updateParamsList = new ArrayList<>();

        for (AdUserClickCount adUserClickCount : updateAdUserClickCounts) {
            Object[] updateParams = new Object[]{adUserClickCount.getClickCount(),
                    adUserClickCount.getDate(),
                    adUserClickCount.getUserid(),
                    adUserClickCount.getAdid()};
            updateParamsList.add(updateParams);
        }

        //jdbcHelper.executeBatch(updateSQL, updateParamsList);
        JdbcManager.executeBatch(updateSQL, updateParamsList);
    }

    @Override
    public int findClickCountByMultiKey(String date, long userid, long adid) {
        String sql =
                "SELECT click_count "
                        + "FROM ad_user_click_count "
                        + "WHERE date=? "
                        + "AND user_id=? "
                        + "AND ad_id=?";

        Object[] params = new Object[]{date, (int) userid, (int) adid};

        final AdUserClickCountQueryResult queryResult = new AdUserClickCountQueryResult();

        //JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        //jdbcHelper.executeQuery(sql, params, rs -> {
        //    if (rs.next()) {
        //        int clickCount = rs.getInt(1);
        //        queryResult.setClickCount(clickCount);
        //    }
        //});
        JdbcManager.executeQuery(sql, params, rs -> {
            if (rs.next()) {
                int clickCount = rs.getInt(1);
                queryResult.setClickCount(clickCount);
            }
        });

        int clickCount = queryResult.getClickCount();

        return clickCount;
    }
}