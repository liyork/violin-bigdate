package com.wolf.test.avro;

import com.wolf.test.avro.generateclass.Members;
import com.wolf.test.avro.generateclass.User;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Random;

/**
 * Description:
 * cd D:\tmp\avro
 * put avro-1.7.6.jar avro-tools-1.7.6.jar user.avsc
 * java -jar avro-tools-1.7.6.jar compile schema user.avsc java.
 * copy User.java in project
 *
 * maven compile 能生成protocol(.avpr)中的类
 * <br/> Created on 2018/1/19 9:42
 *
 * @author 李超
 * @since 1.0.0
 */
public class AvroTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(MemberServer.class);


    @Test
    public void writeUser() throws IOException {
        // 声明并初始化User对象
        // 方式一
        User user1 = new User();
        user1.setName("zhangsan");
        user1.setFavoriteNumber(21);
        user1.setFavoriteColor(null);

        // 方式二 使用构造函数
        // Alternate constructor
        User user2 = new User("Ben", 7, "red");

        // 方式三，使用Build方式
        // Construct via builder
        User user3 = User.newBuilder()
                .setName("Charlie")
                .setFavoriteColor("blue")
                .setFavoriteNumber(null)
                .build();
        String path = "D:\\tmp\\user.avro"; // avro文件存放目录
        DatumWriter<User> userDatumWriter = new SpecificDatumWriter<User>(User.class);
        DataFileWriter<User> dataFileWriter = new DataFileWriter<User>(userDatumWriter);
        dataFileWriter.create(user1.getSchema(), new File(path));
        // 把生成的user对象写入到avro文件
        dataFileWriter.append(user1);
        dataFileWriter.append(user2);
        dataFileWriter.append(user3);
        dataFileWriter.close();
    }

    @Test
    public void readUser() throws IOException {
        DatumReader<User> reader = new SpecificDatumReader<User>(User.class);
        DataFileReader<User> dataFileReader = new DataFileReader<User>(new File("D:\\tmp\\user.avro"), reader);
        User user = null;
        while (dataFileReader.hasNext()) {
            user = dataFileReader.next();
            System.out.println(user);
        }
    }


    /**
     * 动态反序列：通过Schema文件进行动态反序列化操作
     *
     * @throws IOException
     */

    @Test
    public void MemberInfoDynDeser() throws IOException {
        // 1.schema文件解析
        Schema.Parser parser = new Schema.Parser();
        Schema mSchema = parser.parse(this.getClass().getResourceAsStream("/avro/members.avsc"));

        // 2.构建数据读对象
        DatumReader<GenericRecord> mGr = new SpecificDatumReader<GenericRecord>(mSchema);
        DataFileReader<GenericRecord> mDfr = new DataFileReader<GenericRecord>(new File("D:/tmp/avro/members.avro"), mGr);

        // 3.从序列化文件中进行数据反序列化取出数据
        while (mDfr.hasNext()) {
            GenericRecord gr = mDfr.next();
            System.err.println("deser data:" + gr.toString());
        }

        mDfr.close();
        System.out.println("Dyn Builder Ser Start Complete.");

    }


    /**
     * 通过Java工具来生成必要的类，进行反序列化操作
     *
     * @throws IOException
     */

    @Test
    public void MemberInfoToolsDeser() throws IOException {

        // 1.构建反序列化读取对象
        DatumReader<Members> mDr = new SpecificDatumReader<Members>(Members.class);
        DataFileReader<Members> mDfr = new DataFileReader<Members>(new File("D:/tmp/avro/members.avro"), mDr);

        // 2.循环读取文件数据
        while (mDfr.hasNext()) {
            Members m = mDfr.next();
            System.err.println("tools deser data :" + m);
        }

        // 3.关闭读取对象
        mDfr.close();
        System.out.println("Tools Builder Ser Start Complete.");

    }


    /**
     * 动态序列化：通过动态解析Schema文件进行内容设置，并序列化内容
     *
     * @throws IOException
     */
    @Test
    public void MemberInfoDynSer() throws IOException {

        // 1.解析schema文件内容
        Schema.Parser parser = new Schema.Parser();

        InputStream resourceAsStream = this.getClass().getResourceAsStream("/avro/Members.avsc");
        LOGGER.debug("resourceAsStream:{}", resourceAsStream);
        Schema mSchema = parser.parse(resourceAsStream);

        // 2.构建数据写对象
        DatumWriter<GenericRecord> mGr = new SpecificDatumWriter<GenericRecord>(mSchema);
        DataFileWriter<GenericRecord> mDfw = new DataFileWriter<GenericRecord>(mGr);

        // 3.创建序列化文件
        mDfw.create(mSchema, new File("D:/tmp/avro/members.avro"));

        // 4.添加序列化数据
        for (int i = 0; i < 20; i++) {

            GenericRecord gr = new GenericData.Record(mSchema);

            int r = i * new Random().nextInt(50);
            gr.put("userName", "xiaoming-" + r);
            gr.put("userPwd", "9999" + r);
            gr.put("realName", "小明" + r + "号");
            mDfw.append(gr);
        }
        // 5.关闭数据文件写对象
        mDfw.close();
        System.out.println("Dyn Builder Ser Start Complete.");

    }


    /**
     * 通过Java工具生成文件方式进行序列化操作 命令：C:\Users\Administrator>java -jar
     * <p>
     * java -jar avro-tools-1.7.6.jar compile schema members.avsc java.
     *
     * @throws IOException
     */

    @Test
    public void MemberInfoToolsSer() throws IOException {

        // 1.为Member生成对象进行设置必要的内容，这里实现三种设置方式的演示
        // 1.1、构造方式
        Members m1 = new Members("xiaoming", "123456", "校名");
        // 1.2、属性设置
        Members m2 = new Members();
        m2.setUserName("xiaoyi");
        m2.setUserPwd("888888");
        m2.setRealName("小艺");
        // 1.3、Builder方式设置
        Members m3 = Members.newBuilder().setUserName("xiaohong").setUserPwd("999999").setRealName("小红").build();

        // 2.构建反序列化写对象
        DatumWriter<Members> mDw = new SpecificDatumWriter<Members>(Members.class);
        DataFileWriter<Members> mDfw = new DataFileWriter<Members>(mDw);
        // 2.1.通过对Members.avsc的解析创建Schema
        Schema schema = new Schema.Parser().parse(MemberServer.class.getClass().getResourceAsStream("/avro/Members.avsc"));
        // 2.2.打开一个通道，把schema和输出的序列化文件关联起来
        mDfw.create(schema, new File("D:/tmp/avro/members.avro"));
        // 4.把刚刚创建的Users类数据追加到数据文件写入对象中

        mDfw.append(m1);

        mDfw.append(m2);

        mDfw.append(m3);

        // 5.关闭数据文件写入对象

        mDfw.close();

        System.out.println("Tools Builder Ser Start Complete.");

    }
}
