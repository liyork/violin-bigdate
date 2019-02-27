package com.wolf.test.avro;

import com.wolf.test.avro.generateclass.MemberIFace;
import com.wolf.test.avro.generateclass.Members;
import com.wolf.test.avro.generateclass.Retmsg;
import org.apache.avro.Protocol;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.ipc.HttpTransceiver;
import org.apache.avro.ipc.NettyTransceiver;
import org.apache.avro.ipc.Transceiver;
import org.apache.avro.ipc.generic.GenericRequestor;
import org.apache.avro.ipc.specific.SpecificRequestor;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URL;

/**
 * Description:
 * <br/> Created on 2018/1/19 10:31
 *
 * @author 李超
 * @since 1.0.0
 */
public class MemberClient {

    /**
     * 采用HTTP方式建立和服务端的连接
     *
     * @throws IOException
     */

    @Test
    public void MemberHttpRPCDynBuilderClient() throws IOException {

        // 1.建立和服务端的http通讯
        Transceiver transceiver = new HttpTransceiver(new URL("http://127.0.0.1:60090"));
        bussinessDeal(transceiver);

    }


    /**
     * 采用Netty方式建立和服务端的连接
     *
     * @throws IOException
     */

    @Test
    public void MemberNettyRPCDynBuilderClient() throws IOException {

        // 1.建立和服务端的Netty通讯
        Transceiver transceiver = new NettyTransceiver(new InetSocketAddress("127.0.0.1", 60090));
        // 2.进行必要的业务处理
        bussinessDeal(transceiver);

    }


    /**
     * 进行必要的业务处理
     *
     * @param transceiver
     * @throws IOException
     */

    private void bussinessDeal(Transceiver transceiver) throws IOException {

        // 2.获取协议
        Protocol protocol = Protocol.parse(this.getClass().getResourceAsStream("/avro/members.avpr"));
        // 3.根据协议和通讯构造请求对象
        GenericRequestor requestor = new GenericRequestor(protocol, transceiver);
        // 4.根据schema获取messages主节点内容
        GenericRecord loginGr = new GenericData.Record(protocol.getMessages().get("login").getRequest());

        // 5.在根据协议里面获取request中的schema
        GenericRecord mGr = new GenericData.Record(protocol.getType("Members"));

        // 6.设置request中的请求数据
        mGr.put("userName", "rita");
        mGr.put("userPwd", "123456");

        // 7、把二级内容加入到一级message的主节点中
        loginGr.put("m", mGr);

        // 8.设置完毕后，请求方法，正式发送访问请求信息，并得到响应内容
        Object retObj = requestor.request("login", loginGr);
        // 9.进行解析操作
        GenericRecord upGr = (GenericRecord) retObj;

        System.out.println(upGr.get("msg"));

    }


    /**
     * Java工具生成协议代码方式：java -jar E:\avro\avro-tools-1.7.7.jar compile protocol
     * <p>
     * E:\avro\Members.avpr E:\avro 功能和动态调用方式一致
     *
     * @throws InterruptedException
     * @throws IOException
     */
    @Test
    public void MemberNettyRPCToolsBuilderClient() throws InterruptedException, IOException {
        // 1.和服务端建立通讯
        Transceiver transceiver = new NettyTransceiver(new InetSocketAddress("127.0.0.1", 60090));
        // 2.获取客户端对象
        MemberIFace memberIFace = SpecificRequestor.getClient(MemberIFace.class, transceiver);
        // 3.进行数据设置
        Members members = new Members();
        members.setUserName("rita");
        members.setUserPwd("123456");
        // 开始调用登录方法
        Retmsg retmsg = memberIFace.login(members);

        System.out.println("Recive Msg:" + retmsg.getMsg());

    }
}
