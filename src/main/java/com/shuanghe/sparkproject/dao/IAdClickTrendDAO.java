package com.shuanghe.sparkproject.dao;

import com.shuanghe.sparkproject.domain.AdClickTrend;

import java.util.List;

/**
 * Description:广告点击趋势DAO接口
 * <p>
 * Date: 2018/06/08
 * Time: 19:50
 *
 * @author yushuanghe
 */
public interface IAdClickTrendDAO {
    void updateBatch(List<AdClickTrend> adClickTrends);
}
