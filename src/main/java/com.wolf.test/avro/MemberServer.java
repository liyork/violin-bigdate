package com.wolf.test.avro;

import com.wolf.test.avro.generateclass.MemberIFace;
import org.apache.avro.Protocol;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.ipc.HttpServer;
import org.apache.avro.ipc.NettyServer;
import org.apache.avro.ipc.Server;
import org.apache.avro.ipc.generic.GenericResponder;
import org.apache.avro.ipc.specific.SpecificResponder;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Description:
 * <br/> Created on 2018/1/19 10:31
 *
 * @author 李超
 * @since 1.0.0
 */
public class MemberServer {

    private static final Logger LOGGER = LoggerFactory.getLogger(MemberServer.class);

    // ******************************************************ser

    // end*********************************************************

    /**
     * 服务端支持的网络通讯协议有：NettyServer、SocketServer、HttpServer
     * <p>
     * 采用HTTPSERVER方式调用
     *
     * @throws IOException
     * @throws InterruptedException
     */

    @Test
    public void MemberHttpRPCDynBuilderServer() throws IOException, InterruptedException {

        // 1.进行业务处理
        GenericResponder gr = bussinessDeal();
        // 2.开启一个HTTP服务端，进行等待客户端的连接
        Server server = new HttpServer(gr, 60090);
        server.start();
        System.out.println("Dyn Builder PRC Start Complete.");
        server.join();

    }


    /**
     * 服务端支持的网络通讯协议有：NettyServer、SocketServer、HttpServer
     * <p>
     * 采用Netty方式调用
     *
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void MemberNettyRPCDynBuilderServer() throws IOException, InterruptedException {

        // 1.进行业务处理

        GenericResponder gr = bussinessDeal();

        // 2.开启一个Netty服务端，进行等待客户端的连接

        Server server = new NettyServer(gr, new InetSocketAddress(60090));

        server.start();

        System.out.println("Dyn Builder PRC Start Complete.");

        server.join();

    }


    /**
     * 主要进行业务处理 服务端逻辑处理 采用动态生成代码处理方式，客户端和服务端只需要有protocol文件即可，不需要手工生成代码
     *
     * @return
     * @throws IOException
     */

    private GenericResponder bussinessDeal() throws IOException {

        // 1.构建协议
        final Protocol protocol = Protocol.parse(this.getClass().getResourceAsStream("/avro/members.avpr"));

        // 2.构建业务逻辑及响应客户端
        GenericResponder gr = new GenericResponder(protocol) {
            @Override
            public Object respond(Protocol.Message message, Object request) throws Exception {

                System.err.println("request:" + request);
                // 3.获取请求信息
                GenericRecord record = (GenericRecord) request;

                GenericRecord retGr = null;

                // 4.判断请求的方法
                if (message.getName().equals("login")) {
                    // 5.获取到传输的参数
                    Object obj = record.get("m");

                    GenericRecord mGr = (GenericRecord) obj;

                    String userName = mGr.get("userName").toString();
                    String userPwd = mGr.get("userPwd").toString();

                    // 6.进行相应的业务逻辑处理
                    System.out.println("Members:" + ",userName:" + userName + mGr + ",userPwd:" + userPwd);

                    String retMsg;
                    if (userName.equalsIgnoreCase("rita") && userPwd.equals("123456")) {
                        retMsg = "哈哈，恭喜你,成功登录。";
                        System.out.println(retMsg);

                    } else {
                        retMsg = "登录失败。";
                        System.out.println(retMsg);
                    }

                    // 7.获取返回值类型
                    retGr = new GenericData.Record(protocol.getMessages().get("login").getResponse());
                    // 8.构造回复消息
                    retGr.put("msg", retMsg);
                }
                System.err.println("DEAL SUCCESS!");
                return retGr;
            }
        };
        return gr;

    }


    /**
     * Java工具生成协议代码方式：java -jar  E:\avro\avro-tools-1.7.7.jar compile protocol E:\avro\Members.avpr E:\avro
     * <p>
     * 功能和动态调用方式一致
     *
     * @throws InterruptedException
     */

    @Test
    public void MemberNettyRPCToolsBuilderServer() throws InterruptedException {

        //1.构造接口和实现类的映射相应对象，MemberIFaceImpl该类为具体的业务实现类
        SpecificResponder responder = new SpecificResponder(MemberIFace.class, new MemberIFaceImpl());
        //2.Netty启动RPC服务
        Server server = new NettyServer(responder, new InetSocketAddress(60090));
        server.start();
        System.out.println("Tools Builder PRC Start Complete.");
        server.join();

    }

}
