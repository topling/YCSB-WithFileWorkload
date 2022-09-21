### YCSB FileWorkload

为了更真实的模拟实际的应用场景对 MyTopling 进行测试，我们对 YCSB 做了一些修改，添加了一个 FileWorkload，这个 Workload 从提供的数据集作为数据源，而不是使用随机生成的数据。

#### 数据集
数据集必须是文本文件，每行一条记录，每条记录有多个字段，字段之间用分隔符分隔，分隔符默认是 '\t' ，分隔符可以在配置文件中指定。我们通常使用 [Wikipedia](https://dumps.wikimedia.org/backup-index.html) 数据进行测试，我们将数据处理成了行记录文本：[顺序](https://topling-tools.oss-cn-qingdao.aliyuncs.com/wikipedia-flat-seq.zst) 和 [乱序](https://topling-tools.oss-cn-qingdao.aliyuncs.com/wikipedia-flat-rand.zst)。

#### 相关选项参数

FileWorkload 选项

| 选项 | 说明 |
| ----- | ----- |
| table | 测试中使用的数据库表（ collection ）名（默认：usertable） |
| datafile | **数据源**的文件名，可以是 shell 替换，列如 `<(zcat data.gz)` |
| delimiter | 数据源的字段分隔符（默认：`'\t'`） |
| queuesize | 读取数据源数据时共享队列的容量，一般可以不用设置（默认：20000） |
| batchread | 批量读时一次的读取条数，仅在进行 mongodb 测试的时候生效（默认：1） |
| fieldnum　| 数据源的每条记录有多少个字段　|
| writetable | 混合测试中插入（写）操作的表名（ collection ）名（默认：usertable_for_write） |
| writeinread | 是否在读操作中进行写操作，为 true　时在读操作时按一定比例（由　writerate 指定）将读出的数据写入到另一张表（由　writetable 指定）中，为 false 时将会从　datafile 中读取数据插入到表（由　writetable 指定）中（默认：true） |
| writerate | 写操作比例，当 writeinread 为 true 时生效，为将读操作中读出的数据写入另一张表中的比例（默认：0） |
| readproportion | 当 writeinread 为 false 时生效，为读在所有操作中的比例，与 writeproportion 相加为 1 （默认：0.95）|
| writeproportion | 当 writeinread 为 false 时生效，为写在所有操作中的比例，与 readproportion 相加为 1 （默认：0.05）|
| usecustomkey | 是否使用自定义的字段作为　key，若为　true 则使用　keyfield 中定义的字段作为　key，flase 则为使用随机生成的字段作为　key（默认：flase） |
| keyfile | key **数据源**的文件名，仅在　usecustomkey　为 true 时有效，在读操作中作为 key |
| keyfield | 数据源中抽取出来作为　key 的字段，为使用｀,｀分割的数字列表，表示组成 key 的字段序号（从 0　开始），仅在　usecustomkey 为　true　时有效（默认："0"） |
| fieldnames | 每一行中各字段的名，当使用 usecustomkey 时 keyfield 对应的字段名可以使用任意字符（仅有占位作用） |
| mysql.upsert | 是否使用 replase into 代替 insert into 来插入数据（默认：flase），仅在进行 jdbc 测试的时候生效 |


YCSB 中与 FileWorkload 相关的选项

| 选项 | 说明 |
| ----- | ----- |
| recordcount | load 阶段需要加载的数据条数，当 usecustomkey 为 flase 时，会影响随机 key 的生成 |
| operationcount | run 阶段需要进行的测试次数 |
| workload | 指定 workload ，这里为 com.yahoo.ycsb.workloads.FileWorkload |
| requestdistribution | 当 usecustomkey 为 flase　时，在 run 阶段随机生成的 key 的分布（uniform | zipfian）（默认：uniform ） |
| mongodb.url | mongodb 连接字符串 |
| mongodb.upsert | 是否使用 upsert （默认　flase）|
| jdbc.driver | 进行 MySQL 测试的时候指定连接驱动，一般将 jdbc-binding-x.xx.x-SNAPSHOT.jar 下载到 lib 下指定为 com.mysql.jdbc.Driver |
| db.url | MySQL 连接字符串，列如 jdbc:mysql://127.0.0.1/ycsb |
| db.user | 测试使用的　MySQL 用户名 |
| db.passwd | MySQL 用户名对应的密码 |


#### 使用示例

编译
```
git clone https://github.com/topling/YCSB-WithFileWorkload
mvn package
```

编译后直接在此目录使用即可，也会在相应的目录下的 target 目录生成打包二进制文件

加载数据：
```
./bin/ycsb load jdbc -p workload=com.yahoo.ycsb.workloads.FileWorkload -P test.conf -cp /path/to/mysql-connector-java.jar -threads 16 -s
```

进行测试：
```
./bin/ycsb run jdbc -p workload=com.yahoo.ycsb.workloads.FileWorkload -P test.conf -cp /path/to/mysql-connector-java.jar -threads 16 -s
```

若使用 Wikipedia 数据可使用如下配置

```
# your_fileworkload

recordcount=38508221
operationcount=38508221

db.driver=com.mysql.jdbc.Driver
db.url=jdbc:mysql://database_host:3306/your_database_name
db.user=your_user
db.passwd=your_passwd

datafile=/path/to/data_file
keyfile=/path/to/key_file
 
fieldnames=cur_id,cur_namespace,cur_title,cur_text,cur_comment,cur_user,cur_user_text,cur_timestamp,cur_restrictions,cur_counter,cur_is_redirect,cur_minor_edit,cur_random,cur_touched,inverse_timestamp
delimiter=\t
usecustomkey=true
keyfield=0
fieldnum=15
batchread=1
readproportion=0.5
writeproportion=0.5
 
writeinread=flase
mysql.upsert=true
```