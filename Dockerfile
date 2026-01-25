FROM golang:1.23-alpine AS build
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o order-services .

FROM alpine:3.19
WORKDIR /app
COPY --from=build /app/order-services /app/order-services
EXPOSE 8086
ENV HTTP_ADDR=:8086
CMD ["/app/order-services"]
