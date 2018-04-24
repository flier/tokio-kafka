FROM wurstmeister/kafka:1.1.0

COPY setup-tests.sh setup-topics.sh /tmp/

RUN chmod a+x /tmp/*.sh \
    && /tmp/setup-tests.sh
