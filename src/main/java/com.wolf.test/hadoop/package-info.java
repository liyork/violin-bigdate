package com.wolf.test.hadoop;

//自定义job然后交给hadoop执行，job只包含了从哪里读取和写入哪里，以及读取后怎么操作数据进行map，以及如何合并数据reduce
//jobtracker根据namenode分配数据所在datanode的tasktrakcer执行后汇总，
