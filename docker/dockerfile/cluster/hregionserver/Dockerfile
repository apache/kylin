ARG HBASE_VERSION=1.1.2

FROM apachekylin/kylin-hbase-base:hbase_${HBASE_VERSION}

ENV HBASE_VERSION ${HBASE_VERSION}

COPY run_hr.sh /run_hr.sh
RUN chmod +x /run_hr.sh

EXPOSE 16020 16030

CMD ["/run_hr.sh"]
