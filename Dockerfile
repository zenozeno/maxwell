FROM maven:3.5-jdk-8
ENV MAXWELL_VERSION=${project.version}

COPY . /workspace

RUN cd /workspace \
    && mkdir /app \
    && mv /workspace/target/maxwell-$MAXWELL_VERSION/maxwell-$MAXWELL_VERSION/* /app/ \
    && echo "$MAXWELL_VERSION" > /REVISION

WORKDIR /app

CMD [ "/bin/bash", "-c", "bin/maxwell $MAXWELL_OPTIONS" ]
