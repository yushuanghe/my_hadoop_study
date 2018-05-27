package com.shuanghe.sparkproject.test;

import com.shuanghe.sparkproject.dao.ITaskDAO;
import com.shuanghe.sparkproject.dao.factory.DAOFactory;
import com.shuanghe.sparkproject.domain.Task;

/**
 * 任务管理DAO测试类
 *
 * @author yushuanghe
 */
public class TaskDAOTest {

    public static void main(String[] args) {
        ITaskDAO taskDAO = DAOFactory.getTaskDAO();
        Task task = taskDAO.findById(2);
        System.out.println(task.getTaskName());
    }

}
