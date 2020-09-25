ARG MYSQL_VERSION=5.6.49
FROM mysql:${MYSQL_VERSION}

ARG CREATE_DBS="kylin hive"
ENV CREATE_DBS=$CREATE_DBS

COPY run_db.sh /run_db.sh
RUN chmod +x /run_db.sh

ENTRYPOINT ["docker-entrypoint.sh"]

CMD ["/run_db.sh"]
