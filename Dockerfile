FROM golang:1.22-alpine AS builder

WORKDIR /app

# Copy go.mod and go.sum files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the application
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o autotask-data-sink ./cmd/autotask-data-sink

# Use a minimal alpine image for the final container
FROM alpine:3.19

WORKDIR /app

# Install CA certificates for HTTPS
RUN apk --no-cache add ca-certificates

# Copy the binary from the builder stage
COPY --from=builder /app/autotask-data-sink .

# Create a non-root user to run the application
RUN adduser -D -g '' appuser
USER appuser

# Set the entrypoint
ENTRYPOINT ["/app/autotask-data-sink"] 