FROM golang

COPY docker/swimring-cluster.sh /usr/local/bin/swimring-cluster
COPY . /go/src/github.com/hungys/swimring
COPY config.yml /go/bin

WORKDIR /go/src/github.com/hungys/swimring
RUN curl https://glide.sh/get | sh
RUN glide install
RUN go install github.com/hungys/swimring

WORKDIR /go/bin

ENTRYPOINT swimring-cluster

EXPOSE 7000 7001
