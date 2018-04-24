FROM wurstmeister/kafka:1.0.1

COPY setup-tests.sh setup-topics.sh /tmp/

RUN chmod a+x /tmp/*.sh \
    && /tmp/setup-tests.sh
