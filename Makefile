.PHONY: build run stop clean help dev logs

# Помощь
help:
	@echo "Available commands:"
	@echo "  make build       - Compile the application"
	@echo "  make run         - Build and start Docker containers"
	@echo "  make stop        - Stop Docker containers"
	@echo "  make clean       - Clean build artifacts and stop containers"
	@echo "  make dev         - Watch for changes and auto-rebuild"
	@echo "  make logs        - View container logs"

# Компиляция приложения
build:
	@echo "Building..."
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o volatility ./cmd/volatility
	@echo "Build complete!"

# Запуск контейнеров
run:
	@echo "Starting Docker containers..."
	docker compose up -d --build
	@echo "Volatility service is running!"

# Остановка контейнеров
stop:
	@echo "Stopping Docker containers..."
	docker compose down

# Очистка проекта
clean: stop
	@echo "Cleaning up..."
	rm -f volatility
	@echo "Clean complete!"

# Logs
logs:
	docker compose logs -f
