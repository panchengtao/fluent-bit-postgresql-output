FROM golang:1.11 AS build
ENV GO111MODULE=on
WORKDIR /go/src/output
COPY . .
RUN go get ./... && go build -buildmode=c-shared -o ./publish/out_postgresql.so .

FROM fluent/fluent-bit:0.14 as runtime
WORKDIR /plugins
COPY --from=build /go/src/output/publish ./
CMD ["/fluent-bit/bin/fluent-bit","-e", "/plugins/out_postgresql.so", "-c", "/fluent-bit/etc/fluent-bit.conf"]
