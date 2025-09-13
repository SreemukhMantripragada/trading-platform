.PHONY: up down ps logs topics doctor

up: 
	cd infra && docker compose up -d && docker compose ps
down: 
	cd infra && docker compose down -v
ps: 
	cd infra && docker compose ps
logs: 
	cd infra && docker compose logs -f
topics:
	docker exec -i kafka bash -lc 'BROKER=kafka:29092; for t in ticks "bars.1s" "bars.1m" "bars.tf.3m" "bars.tf.5m" "bars.tf.15m" orders fills dlq; do /opt/bitnami/kafka/bin/kafka-topics.sh --bootstrap-server $$BROKER --create --if-not-exists --topic $$t --partitions 3 --replication-factor 1 || true; done; /opt/bitnami/kafka/bin/kafka-topics.sh --bootstrap-server $$BROKER --list'
doctor: 
	python3 monitoring/doctor.py
