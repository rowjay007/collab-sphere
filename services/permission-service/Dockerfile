FROM golang:1.26
WORKDIR /src
COPY . .
RUN go build -o /permission-service ./services/permission-service/cmd/permission-service
EXPOSE 9094
ENTRYPOINT ["/permission-service"]
