package com.shuanghe.sparkproject.dao.factory;

import com.shuanghe.sparkproject.dao.ISessionAggrStatDAO;
import com.shuanghe.sparkproject.dao.ITaskDAO;
import com.shuanghe.sparkproject.dao.impl.SessionAggrStatDAOImpl;
import com.shuanghe.sparkproject.dao.impl.TaskDAOImpl;

/**
 * DAO工厂类
 *
 * @author yushuanghe
 */
public class DAOFactory {

    /**
     * 获取任务管理DAO
     *
     * @return DAO
     */
    public static ITaskDAO getTaskDAO() {
        return new TaskDAOImpl();
    }

    public static ISessionAggrStatDAO getSessionAggrStatDAO() {
        return new SessionAggrStatDAOImpl();
    }
}
