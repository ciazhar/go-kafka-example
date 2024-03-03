deploy:
	cd deployments && docker-compose up

run-consumer:
	cd examples/basic && go run cmd/consumer.go

run-producer:
	cd examples/basic && go run cmd/producer.go

run-http-to-kafka:
	cd examples/http-to-kafka && go run cmd/main.go

run-trx-producer:
	cd examples/trx-producer && go run cmd/main.go