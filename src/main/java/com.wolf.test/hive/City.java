package com.wolf.test.hive;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * Description:
 * <br/> Created on 1/10/18 10:26 AM
 *
 * @author 李超
 * @since 1.0.0
 */
public class City extends UDF {

    //hive通过反射获取此方法。
    public Text evaluate(final Text text) {
        String substring = text.toString().substring(0, 3);
        return new Text(substring);
    }
}
