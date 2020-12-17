# builder image
FROM golang:1.15 as builder
RUN mkdir /build
ADD *.go /build/
WORKDIR /build
RUN go get -d -v ./...
RUN CGO_ENABLED=0 GOOS=linux go build -a -o openweather2nats .


# generate clean, final image for end users
FROM golang:1.15
COPY --from=builder /build/openweather2nats .

# executable
ENTRYPOINT [ "./openweather2nats" ]

# Build
# $ docker build . -t openweather2nats:latest

# Run
# $ docker run --restart=always -d -v $(pwd)/config.yml:/go/config.yml --name weather2nats openweather2nats:latest
