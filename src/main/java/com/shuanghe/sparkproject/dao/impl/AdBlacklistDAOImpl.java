package com.shuanghe.sparkproject.dao.impl;

import com.shuanghe.sparkproject.dao.IAdBlacklistDAO;
import com.shuanghe.sparkproject.domain.AdBlacklist;
import com.shuanghe.sparkproject.jdbc.JdbcManager;

import java.util.ArrayList;
import java.util.List;

/**
 * Description:广告黑名单DAO实现类
 * <p>
 * Date: 2018/06/08
 * Time: 17:20
 *
 * @author yushuanghe
 */
public class AdBlacklistDAOImpl implements IAdBlacklistDAO {
    @Override
    public void insertBatch(List<AdBlacklist> adBlacklists) {
        String sql = "INSERT INTO ad_blacklist VALUES(?)";

        List<Object[]> paramsList = new ArrayList<>();

        for (AdBlacklist adBlacklist : adBlacklists) {
            Object[] params = new Object[]{adBlacklist.getUserid()};
            paramsList.add(params);
        }

        //JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        //jdbcHelper.executeBatch(sql, paramsList);
        JdbcManager.executeBatch(sql, paramsList);
    }

    @Override
    public List<AdBlacklist> findAll() {
        String sql = "SELECT user_id FROM ad_blacklist";

        final List<AdBlacklist> adBlacklists = new ArrayList<>();

        //JDBCHelper jdbcHelper = JDBCHelper.getInstance();

        //jdbcHelper.executeQuery(sql, null, rs -> {
        //    while (rs.next()) {
        //        long userid = rs.getLong(1);
        //
        //        AdBlacklist adBlacklist = new AdBlacklist();
        //        adBlacklist.setUserid(userid);
        //
        //        adBlacklists.add(adBlacklist);
        //    }
        //});
        JdbcManager.executeQuery(sql, null, rs -> {
            while (rs.next()) {
                long userid = rs.getLong(1);

                AdBlacklist adBlacklist = new AdBlacklist();
                adBlacklist.setUserid(userid);

                adBlacklists.add(adBlacklist);
            }
        });

        return adBlacklists;
    }
}