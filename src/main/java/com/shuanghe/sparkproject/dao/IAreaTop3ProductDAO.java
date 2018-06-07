package com.shuanghe.sparkproject.dao;

import com.shuanghe.sparkproject.domain.AreaTop3Product;

import java.util.List;

/**
 * Description:各区域top3热门商品DAO接口
 * <p>
 * Date: 2018/06/07
 * Time: 17:56
 *
 * @author yushuanghe
 */
public interface IAreaTop3ProductDAO {
    void insertBatch(List<AreaTop3Product> areaTopsProducts);
}
