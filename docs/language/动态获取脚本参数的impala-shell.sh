#!/bin/bash

export PYTHON_EGG_CACHE=./myeggs

#遍历参数，拼接命令
commod="impala-shell.sh -i localhost:27001 "
a=0
#遍历每个参数，拼接命令
   for i in $@
    do
      a=$[$a+1]

      if [ $a -eq 1 ];then
        #拼接文件名
        commod="$commod  -f $i"
      else
        #参数切分为kv
        arr=(${i//=/ })
        commod="$commod  --var=${arr[0]}=${arr[1]}"
      fi
      echo  $commod
     done
# 执行命令 
eval $commod
