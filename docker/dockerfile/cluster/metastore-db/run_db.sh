#!/bin/bash

mysqld --user=root

mysqladmin -uroot password kylin
mysql -uroot -pkylin -e "grant all privileges on root.* to root@'%' identified by 'kylin' WITH GRANT OPTION; FLUSH PRIVILEGES;"

for db in $CREATE_DBS; do
  mysql -uroot -pkylin -e "create database $db;"
  done

while :
do
    sleep 10
done
