FROM golang:1.23-alpine AS build
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o order-services .

FROM alpine:3.19
WORKDIR /app
RUN apk --no-cache add ca-certificates wget
COPY --from=build /app/order-services /app/order-services
EXPOSE 8086
ENV HTTP_ADDR=:8086
CMD ["/app/order-services"]
