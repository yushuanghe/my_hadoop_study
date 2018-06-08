package com.shuanghe.sparkproject.dao;

import com.shuanghe.sparkproject.domain.AdStat;

import java.util.List;

/**
 * Description:广告实时统计DAO接口
 * <p>
 * Date: 2018/06/08
 * Time: 17:49
 *
 * @author yushuanghe
 */
public interface IAdStatDAO {
    void updateBatch(List<AdStat> adStats);
}
