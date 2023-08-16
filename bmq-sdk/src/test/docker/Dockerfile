ARG image
FROM ${image}

ENV BMQ_DOCKER_TMPDIR=/tmp/bmq-broker

COPY config/ /etc/local/bmq/
RUN ls -la /etc/local/bmq/*

CMD [ "/usr/local/bin/bmqbrkr"  \
    , "/etc/local/bmq"          \
    ]