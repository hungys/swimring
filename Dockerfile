FROM golang

COPY docker/swimring-cluster.sh /usr/local/bin/swimring-cluster
COPY . /go/src/github.com/hungys/swimring
COPY config.yml /go/bin

RUN go get github.com/dgryski/go-farm github.com/op/go-logging github.com/olekukonko/tablewriter gopkg.in/yaml.v2
RUN go install github.com/hungys/swimring

WORKDIR /go/bin

ENTRYPOINT swimring-cluster

EXPOSE 7000 7001
