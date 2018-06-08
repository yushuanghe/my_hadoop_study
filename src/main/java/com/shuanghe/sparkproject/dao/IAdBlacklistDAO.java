package com.shuanghe.sparkproject.dao;

import com.shuanghe.sparkproject.domain.AdBlacklist;

import java.util.List;

/**
 * Description:广告黑名单DAO接口
 * <p>
 * Date: 2018/06/08
 * Time: 17:19
 *
 * @author yushuanghe
 */
public interface IAdBlacklistDAO {
    /**
     * 批量插入广告黑名单用户
     *
     * @param adBlacklists
     */
    void insertBatch(List<AdBlacklist> adBlacklists);

    /**
     * 查询所有广告黑名单用户
     *
     * @return
     */
    List<AdBlacklist> findAll();
}
