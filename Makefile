# Makefile for easy development
.PHONY: build run test clean docker-build docker-run docker-stop

# Variables
APP_NAME := payment-gateway
DOCKER_IMAGE := $(APP_NAME):latest
GO_VERSION := 1.21

# Build the application
build:
	CGO_ENABLED=1 go build -o $(APP_NAME) ./main.go

# Run the application locally
run: build
	./$(APP_NAME)

# Run tests
test:
	go test -v -race ./...

# Clean build artifacts
clean:
	rm -f $(APP_NAME)
	go clean

# Build Docker image
docker-build:
	docker build -t $(DOCKER_IMAGE) .

# Run with Docker Compose
docker-run:
	docker-compose up -d

# Stop Docker services
docker-stop:
	docker-compose down

# View logs
logs:
	docker-compose logs -f

# Run locally with payment processors
dev-run:
	cd payment-processor && docker-compose up -d
	sleep 5
	make run

# Full development setup
dev-setup: docker-build
	cd payment-processor && docker-compose up -d
	sleep 10
	docker-compose up -d
	echo "Services started. Access the API at http://localhost:9999"

# Run k6 tests
test-load:
	cd rinha-test && k6 run test.js

# Check resource usage
stats:
	docker stats --no-stream

# Format code
fmt:
	go fmt ./...
	go mod tidy

# Lint code
lint:
	golangci-lint run

# Build for production (optimized)
build-prod:
	CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build \
		-ldflags="-w -s -extldflags '-static'" \
		-tags sqlite_omit_load_extension \
		-o $(APP_NAME) ./main.go

# Push to registry (update with your registry)
push: docker-build
	docker tag $(DOCKER_IMAGE) your-registry/$(DOCKER_IMAGE)
	docker push your-registry/$(DOCKER_IMAGE)