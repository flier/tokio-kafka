FROM wurstmeister/kafka:0.10.2.1

COPY setup-tests.sh setup-topics.sh /tmp/

RUN chmod a+x /tmp/*.sh \
    && /tmp/setup-tests.sh
