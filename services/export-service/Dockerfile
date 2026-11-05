FROM golang:1.26
WORKDIR /src
COPY . .
RUN go build -o /export-service ./services/export-service/cmd/export-service
EXPOSE 9095
ENTRYPOINT ["/export-service"]
