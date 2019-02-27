package com.wolf.test.kafka.producer;


import org.apache.kafka.common.serialization.Serializer;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Description:自定义序列化器
 * 很脆弱，若修改customerId为long则要修改序列化器使用8字节，
 * 若添加startDate字段，需要修改序列代码，可能出现新旧消息兼容性问题。
 * 把生产者和消费者耦合在一起。
 * avro则与语言无关，而升级前后也可以兼容，消费时schema是老的，那么新数据部分没用，若schema是新的，老数据部分没用
 * <br/> Created on 03/04/2018 8:46 PM
 *
 * @author 李超
 * @since 1.0.0
 */
public class CustomerSerializer implements Serializer<CustomerObject> {


    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    /**
     * idLength(4)+nameLength(4)+name
     * @param topic
     * @param data
     * @return
     */
    @Override
    public byte[] serialize(String topic, CustomerObject data) {

        if (null == data) {
            return null;
        }

        byte[] serializedName = new byte[0];
        int stringSize = 0;

        String customerName = data.getCustomerName();
        if (null != customerName) {
            try {
                serializedName = customerName.getBytes("utf-8");
                stringSize = serializedName.length;
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
        } else {
            serializedName = new byte[0];
            stringSize = 0;
        }

        ByteBuffer buffer = ByteBuffer.allocate(4 + 4 + stringSize);
        buffer.putInt(data.getCustomerId());
        buffer.putInt(stringSize);
        buffer.put(serializedName);

        return buffer.array();
    }

    @Override
    public void close() {

    }
}
