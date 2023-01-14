Запуск докера с MySQL 

sudo docker run --detach --name=mysql --env "MYSQL_ROOT_PASSWORD=root" --publish 6603:3306 --volume /root/docker/[container_name]/conf.d:/etc/mysql/conf.d mysql/mysql-server:latest
