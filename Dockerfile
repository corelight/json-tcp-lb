FROM golang:latest as builder

WORKDIR /src/json-tcp-lb
COPY go.mod ./
#noexternal deps yet
#RUN go mod download

COPY *.go ./

RUN go test ./...
RUN CGO_ENABLED=0 go build -ldflags="-w -s" -o  /usr/bin/json-tcp-lb

####

FROM alpine
COPY --from=builder /usr/bin/json-tcp-lb /usr/bin/json-tcp-lb
ENV USER=jsonlb
ENV UID=10001
RUN adduser \
    --disabled-password \
    --gecos "" \
    --home "/nonexistent" \
    --shell "/sbin/nologin" \
    --no-create-home \
    --uid "${UID}" \
    "${USER}"

USER $USER:$USER

ENTRYPOINT ["/usr/bin/json-tcp-lb"]
