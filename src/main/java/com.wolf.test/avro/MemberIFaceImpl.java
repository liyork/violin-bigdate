package com.wolf.test.avro;

import com.wolf.test.avro.generateclass.MemberIFace;
import com.wolf.test.avro.generateclass.Members;
import com.wolf.test.avro.generateclass.Retmsg;
import org.apache.avro.AvroRemoteException;

/**
 * Description:
 * <br/> Created on 2018/1/19 10:32
 *
 * @author 李超
 * @since 1.0.0
 */
public class MemberIFaceImpl implements MemberIFace {


    final String userName = "rita";

    final String userPwd = "888888";

    /**
     * 登录业务处理
     */
    @Override
    public Retmsg login(Members m) throws AvroRemoteException {
        //验证登录权限
        if (m.getUserName().equals(userName) && m.getUserPwd().equals(userPwd)) {

            return new Retmsg("恭喜你，登录成功，欢迎进入AVRO测试环境。");
        }
        return new Retmsg("对不起，权限不足，不能登录。");

    }


}