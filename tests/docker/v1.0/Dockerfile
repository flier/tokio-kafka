FROM wurstmeister/kafka:1.0.1

COPY setup-tests.sh /tmp

RUN chmod a+x /tmp/*.sh \
    && /tmp/setup-tests.sh
