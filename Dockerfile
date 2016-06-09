FROM golang

COPY docker/swimring-cluster.sh /usr/local/bin/swimring-cluster
COPY . /go/src/app
WORKDIR /go/src/app

RUN go-wrapper download
RUN go-wrapper install

RUN cp config.yml /go/bin
WORKDIR /go/bin

ENTRYPOINT swimring-cluster

EXPOSE 7000 7001
