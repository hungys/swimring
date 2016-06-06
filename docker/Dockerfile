FROM golang

ADD swimring-cluster.sh /usr/local/bin/swimring-cluster

RUN go get github.com/hungys/swimring
RUN go install github.com/hungys/swimring
RUN cp /go/src/github.com/hungys/swimring/config.yml /go/bin/config.yml

WORKDIR /go/bin

ENTRYPOINT swimring-cluster

EXPOSE 7000 7001
