package com.shuanghe.sparkproject.domain;

import org.apache.spark.sql.Row;

/**
 * Description:Session明细
 * <p>
 * Date: 2018/05/29
 * Time: 20:08
 *
 * @author yushuanghe
 */
public class SessionDetail {
    private long taskid;
    private long userid;
    private String sessionid;
    private long pageid;
    private String actionTime;
    private String searchKeyword;
    private long clickCategoryId;
    private long clickProductId;
    private String orderCategoryIds;
    private String orderProductIds;
    private String payCategoryIds;
    private String payProductIds;

    public SessionDetail() {
    }

    public SessionDetail(long taskid, Row row) {
        super();
        this.setTaskid(taskid);
        this.setUserid(row.getAs("user_id"));
        this.setSessionid(row.getAs("session_id"));
        this.setPageid(row.getAs("page_id"));
        this.setActionTime(row.getAs("action_time"));
        this.setSearchKeyword(row.getAs("search_keyword"));

        Long clickCategoryId = row.getAs("click_category_id");
        if (clickCategoryId == null) {
            clickCategoryId = -1L;
        }
        this.setClickCategoryId(clickCategoryId);

        Long clickProductId = row.getAs("click_product_id");
        if (clickProductId == null) {
            clickProductId = -1L;
        }
        this.setClickProductId(clickProductId);

        this.setOrderCategoryIds(row.getAs("order_category_ids"));
        this.setOrderProductIds(row.getAs("order_product_ids"));
        this.setPayCategoryIds(row.getAs("pay_category_ids"));
        this.setPayProductIds(row.getAs("pay_product_ids"));
    }

    public long getTaskid() {
        return taskid;
    }

    public void setTaskid(long taskid) {
        this.taskid = taskid;
    }

    public long getUserid() {
        return userid;
    }

    public void setUserid(long userid) {
        this.userid = userid;
    }

    public String getSessionid() {
        return sessionid;
    }

    public void setSessionid(String sessionid) {
        this.sessionid = sessionid;
    }

    public long getPageid() {
        return pageid;
    }

    public void setPageid(long pageid) {
        this.pageid = pageid;
    }

    public String getActionTime() {
        return actionTime;
    }

    public void setActionTime(String actionTime) {
        this.actionTime = actionTime;
    }

    public String getSearchKeyword() {
        return searchKeyword;
    }

    public void setSearchKeyword(String searchKeyword) {
        this.searchKeyword = searchKeyword;
    }

    public long getClickCategoryId() {
        return clickCategoryId;
    }

    public void setClickCategoryId(long clickCategoryId) {
        this.clickCategoryId = clickCategoryId;
    }

    public long getClickProductId() {
        return clickProductId;
    }

    public void setClickProductId(long clickProductId) {
        this.clickProductId = clickProductId;
    }

    public String getOrderCategoryIds() {
        return orderCategoryIds;
    }

    public void setOrderCategoryIds(String orderCategoryIds) {
        this.orderCategoryIds = orderCategoryIds;
    }

    public String getOrderProductIds() {
        return orderProductIds;
    }

    public void setOrderProductIds(String orderProductIds) {
        this.orderProductIds = orderProductIds;
    }

    public String getPayCategoryIds() {
        return payCategoryIds;
    }

    public void setPayCategoryIds(String payCategoryIds) {
        this.payCategoryIds = payCategoryIds;
    }

    public String getPayProductIds() {
        return payProductIds;
    }

    public void setPayProductIds(String payProductIds) {
        this.payProductIds = payProductIds;
    }
}