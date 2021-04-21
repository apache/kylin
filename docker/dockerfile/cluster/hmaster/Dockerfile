

ARG HBASE_VERSION=1.1.2

FROM apachekylin/kylin-ci-hbase-base:hbase_${HBASE_VERSION}

ENV HBASE_VERSION ${HBASE_VERSION}
COPY run_hm.sh /run_hm.sh
RUN chmod +x /run_hm.sh

EXPOSE 16000 16010

CMD ["/run_hm.sh"]
