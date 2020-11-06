#!/bin/bash


database=$1

cur_time="`date "+%Y%m%d%H%M%S"`"
hdfs_log_dir=/test/hdfs_shell_log/auto_add_hive_partition/
log_file="shell_auto_add_partition_${cur_time}.log"

echo `$pwd`   1>> $log_file 2>> $log_file

parent_dir=$(hdfs dfs -ls /apps/data/warehouse/$database |grep  dm_ |awk '/^d/ {print $NF}')
for pd in $parent_dir
 do
   echo "### `date "+%Y-%m-%d %H:%M:%S"` 文件路径： $pd    自动添加分区开始。。。" 1>> $log_file 2>> $log_file
   echo $pd
   db_tb=(${pd//// })
   partition_dir=$(hdfs dfs -ls $pd  |awk '/^d/ {print $NF}')
   for cd in $partition_dir
     do
       #echo $cd
       part=`echo "$cd" | awk -F "/" '{print $NF}'`
       if [ ${part}=~'=' ];then
         part=`echo "$cd" | awk -F "/" '{print $NF}'`
         dt=(${part//=/ })
         add_sql="alter table ${db_tb[3]}.${db_tb[4]} add partition (${dt[0]}='${dt[1]}')   location  '$cd'; "
         echo $add_sql
         hive  -e   $add_sql                 1>> $log_file 2>> $log_file
         #echo $add_sql                                                       1>> $log_file 2>> $log_file
         echo "!!! `date "+%Y-%m-%d %H:%M:%S"` 分区：dt='${dt[1]}'   添加成功。。。" 1>> $log_file 2>> $log_file
       else
         echo  "&&& `date "+%Y-%m-%d %H:%M:%S"`  该表无分区。。。" 1>> $log_file 2>> $log_file
       fi

     done


hdfs dfs -put  $log_file  $hdfs_log_dir   1>> $log_file 2>> $log_file
#rm  -rf $log_file
