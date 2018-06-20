package com.shuanghe.sparkproject.dao.impl;

import com.shuanghe.sparkproject.dao.IAdStatDAO;
import com.shuanghe.sparkproject.domain.AdStat;
import com.shuanghe.sparkproject.jdbc.JdbcManager;
import com.shuanghe.sparkproject.model.AdStatQueryResult;

import java.util.ArrayList;
import java.util.List;

/**
 * Description:广告实时统计DAO实现类
 * <p>
 * Date: 2018/06/08
 * Time: 17:50
 *
 * @author yushuanghe
 */
public class AdStatDAOImpl implements IAdStatDAO {
    @Override
    public void updateBatch(List<AdStat> adStats) {
        //JDBCHelper jdbcHelper = JDBCHelper.getInstance();

        // 区分开来哪些是要插入的，哪些是要更新的
        List<AdStat> insertAdStats = new ArrayList<>();
        List<AdStat> updateAdStats = new ArrayList<>();

        String selectSQL =
                "SELECT count(1) "
                        + "FROM ad_stat "
                        + "WHERE date=? "
                        + "AND province=? "
                        + "AND city=? "
                        + "AND ad_id=?";

        for (AdStat adStat : adStats) {
            final AdStatQueryResult queryResult = new AdStatQueryResult();

            Object[] params = new Object[]{adStat.getDate(),
                    adStat.getProvince(),
                    adStat.getCity(),
                    adStat.getAdid()};

            //jdbcHelper.executeQuery(selectSQL, params, rs -> {
            //    if (rs.next()) {
            //        int count = rs.getInt(1);
            //        queryResult.setCount(count);
            //    }
            //});
            JdbcManager.executeQuery(selectSQL, params, rs -> {
                if (rs.next()) {
                    int count = rs.getInt(1);
                    queryResult.setCount(count);
                }
            });

            int count = queryResult.getCount();

            if (count > 0) {
                updateAdStats.add(adStat);
            } else {
                insertAdStats.add(adStat);
            }
        }

        // 对于需要插入的数据，执行批量插入操作
        String insertSQL = "INSERT INTO ad_stat VALUES(?,?,?,?,?)";

        List<Object[]> insertParamsList = new ArrayList<>();

        for (AdStat adStat : insertAdStats) {
            Object[] params = new Object[]{adStat.getDate(),
                    adStat.getProvince(),
                    adStat.getCity(),
                    adStat.getAdid(),
                    adStat.getClickCount()};
            insertParamsList.add(params);
        }

        //jdbcHelper.executeBatch(insertSQL, insertParamsList);
        JdbcManager.executeBatch(insertSQL, insertParamsList);

        // 对于需要更新的数据，执行批量更新操作
        String updateSQL = "UPDATE ad_stat SET click_count=? "
                + "WHERE date=? "
                + "AND province=? "
                + "AND city=? "
                + "AND ad_id=?";

        List<Object[]> updateParamsList = new ArrayList<>();

        for (AdStat adStat : updateAdStats) {
            Object[] params = new Object[]{adStat.getClickCount(),
                    adStat.getDate(),
                    adStat.getProvince(),
                    adStat.getCity(),
                    adStat.getAdid()};
            updateParamsList.add(params);
        }

        //jdbcHelper.executeBatch(updateSQL, updateParamsList);
        JdbcManager.executeBatch(updateSQL, updateParamsList);
    }
}