package com.shuanghe.spark.sql.model;

import java.io.Serializable;

/**
 * Created with IntelliJ IDEA.
 * User: yushuanghe
 * Date: 18-4-17
 * Time: 下午11:06
 * To change this template use File | Settings | File Templates.
 * Description:
 */
public class Score implements Serializable {
    private int id;
    private int score;

    public Score(int id, int score) {
        this.id = id;
        this.score = score;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getScore() {
        return score;
    }

    public void setScore(int score) {
        this.score = score;
    }
}