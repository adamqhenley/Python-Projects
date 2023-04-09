login to mysql shell

getting mysql shell:
    - https://dev.mysql.com/doc/mysql-shell/8.0/en/mysql-shell-install-macos-quick.html
    - https://dev.mysql.com/downloads/shell/
    - login to mysql shell: https://dev.mysql.com/doc/refman/8.0/en/connecting.html
    - mysql shell commands: https://dev.mysql.com/doc/mysql-shell/8.0/en/mysql-shell-commands.html
    - help to login: https://stackoverflow.com/questions/14235362/mac-install-and-open-mysql-using-terminal

    
in macos terminal:
    mysqlsh
    \sql
    \connect root@localhost:{port number}
    show databases;
    use {database name};
    select * from {table name} limit 10
    \quit   // quits mysqlsh and goes back to terminal