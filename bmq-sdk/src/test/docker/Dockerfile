ARG image
FROM ${image}

ENV BMQ_DOCKER_TMPDIR=/tmp/bmq-broker

COPY config/ /etc/local/bmq/

EXPOSE 30114

CMD [ "/usr/local/bin/bmqbrkr"  \
    , "/etc/local/bmq"          \
    ]
