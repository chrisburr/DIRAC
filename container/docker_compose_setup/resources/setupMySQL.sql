CREATE USER 'root'@'%' IDENTIFIED BY 'demopass';
GRANT ALL ON *.* TO 'root'@'%' WITH GRANT OPTION;
FLUSH PRIVILEGES;
