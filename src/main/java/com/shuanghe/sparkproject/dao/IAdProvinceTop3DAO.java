package com.shuanghe.sparkproject.dao;

import com.shuanghe.sparkproject.domain.AdProvinceTop3;

import java.util.List;

/**
 * Description:各省份top3热门广告DAO接口
 * <p>
 * Date: 2018/06/08
 * Time: 18:21
 *
 * @author yushuanghe
 */
public interface IAdProvinceTop3DAO {
    void updateBatch(List<AdProvinceTop3> adProvinceTop3s);
}