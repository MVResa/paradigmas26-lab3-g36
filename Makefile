# Variables de entorno requeridas para Spark y Java 17 en FAMAF
JAVA_17_HOME := /usr/lib/jvm/java-17-openjdk-amd64
SBT_FLAGS := SBT_OPTS="--add-exports=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED"
MOCK_DIR := ../reddit-mock

.PHONY: all compile run clean

all: compile

compile:
	JAVA_HOME=$(JAVA_17_HOME) $(SBT_FLAGS) sbt compile

run:
	@echo "🛑 Asegurando que no existan mocks colgados en el puerto 8123..."
	@-kill -9 $$(lsof -t -i:8123) 2>/dev/null || true
	@echo "🚀 Iniciando servidor simulador (reddit-mock) en segundo plano..."
	@cd $(MOCK_DIR) && (tail -f /dev/null | JAVA_HOME=$(JAVA_17_HOME) $(SBT_FLAGS) sbt run > ../mock.log 2>&1) &
	@echo "⏳ Esperando a que el puerto 8123 del servidor mock se active..."
	@for i in {1..30}; do \
		if nc -z localhost 8123 >/dev/null 2>&1 || curl -s http://localhost:8123 >/dev/null 2>&1 || [ $$? -ne 7 ]; then \
			echo "✅ Puerto detectado. Esperando 2 segundos para estabilizar el servidor..."; \
			sleep 2; \
			break; \
		fi; \
		sleep 1; \
	done
	@echo "📊 Ejecutando pipeline distribuido de Spark (sbt run)..."
	@trap 'echo "🛑 Apagando servidor mock..."; kill -9 $$(lsof -t -i:8123) 2>/dev/null || true' EXIT INT TERM; \
	JAVA_HOME=$(JAVA_17_HOME) $(SBT_FLAGS) sbt "run --subscription-file data/local_subscriptions.json --entities-dir data/valid_entities --top-k 10"

clean:
	sbt clean
	-cd $(MOCK_DIR) && sbt clean
	-rm -f mock.log