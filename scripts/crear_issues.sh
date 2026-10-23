#!/usr/bin/env bash
# Crea labels, milestones (una por semana) y los 17 Issues del backlog en GitHub.
# Requiere GitHub CLI autenticado (gh auth login) y ejecutarse dentro del repo clonado.
#
#   1. Reemplaza los tres usuarios de GitHub de abajo.
#   2. bash scripts/crear_issues.sh
#
# Ejecutar UNA sola vez (si se repite, duplica los Issues).
set -euo pipefail

KEVIN="tarantis-udec"
CARLOS="anotheruserbtww"
LIZETH="lizeth93993"

REPO=$(gh repo view --json nameWithOwner -q .nameWithOwner)
echo "Repositorio: $REPO"

# --- Labels ---------------------------------------------------------------
label() { gh label create "$1" --color "$2" --description "$3" --force >/dev/null; }
label "semana-1"   "0E8A16" "Diseño y organización"
label "semana-2"   "1D76DB" "Pipeline funcional y CI"
label "semana-3"   "0F9D8C" "Airflow, calidad y resiliencia"
label "semana-4"   "D93F0B" "Integración y release final"
label "kafka"      "F9A825" "Producer / Kafka"
label "spark"      "1F4E79" "Spark Structured Streaming"
label "datalake"   "13998A" "Data Lake"
label "airflow"    "2E7D32" "Airflow"
label "ci"         "6F42C1" "Tests / GitHub Actions"
label "docs"       "BFD4F2" "README, ADR, checkpoints"
label "prioridad-alta"  "B60205" "Bloquea el release de la semana"
label "prioridad-media" "FBCA04" "Necesario para el release"

# --- Milestones -----------------------------------------------------------
milestone() {
  gh api "repos/$REPO/milestones" -f title="$1" -f due_on="$2" -f description="$3" >/dev/null 2>&1 \
    || echo "  (milestone '$1' ya existe)"
}
milestone "v0.1.0-foundation"    "2026-10-23T05:00:00Z" "Semana 1 - Diseño"
milestone "v0.2.0-streaming"     "2026-10-30T05:00:00Z" "Semana 2 - Construir + CI"
milestone "v0.3.0-orchestration" "2026-11-06T05:00:00Z" "Semana 3 - Orquestar"
milestone "v1.0.0"               "2026-11-13T05:00:00Z" "Semana 4 - Liberar"

# --- Issues ---------------------------------------------------------------
issue() {  # titulo  asignado  milestone  labels  cuerpo
  gh issue create --title "$1" --assignee "$2" --milestone "$3" --label "$4" --body "$5" >/dev/null
  echo "  + $1"
}

M1="v0.1.0-foundation"; M2="v0.2.0-streaming"; M3="v0.3.0-orchestration"; M4="v1.0.0"

# Semana 1
issue "Crear repositorio, estructura base y colaboradores" "$KEVIN" "$M1" "semana-1,docs,prioridad-alta" \
"Crear la estructura recomendada del repositorio, .gitignore, requirements.txt y agregar a los 3 integrantes.

**Criterios de aceptación**
- [ ] Estructura de carpetas según la guía
- [ ] 3 colaboradores con acceso
- [ ] Rama main protegida (PR + 1 review)"

issue "Definir contrato JSON del evento, schema y samples" "$LIZETH" "$M1" "semana-1,kafka,prioridad-alta" \
"Definir el evento de pedido (campos mínimos: pedido, zona, timestamp, categoría, estado) con JSON Schema y ejemplos.

**Criterios de aceptación**
- [ ] samples/order_event.schema.json
- [ ] samples/order_event.sample.json y order_events.sample.jsonl
- [ ] tests/test_contrato.py pasa en local"

issue "Definir KPIs y reglas de alerta con umbrales iniciales" "$CARLOS" "$M1" "semana-1,spark,prioridad-alta" \
"Definir mínimo 3 KPIs y 2 alertas (demanda alta, capacidad insuficiente) con umbrales justificados.

**Criterios de aceptación**
- [ ] config/umbrales.yaml y config/zonas.yaml
- [ ] Justificación de cada umbral en el README"

issue "Definir topic, key y particiones de Kafka (ADR-001)" "$KEVIN" "$M1" "semana-1,kafka,docs,prioridad-alta" \
"Decidir topic, key y número de particiones con evidencia (simulación del particionador).

**Criterios de aceptación**
- [ ] docs/decisions/ADR-001-topic-key-particiones.md
- [ ] scripts/simular_particiones.py + tests/test_particiones.py"

issue "docker-compose preliminar con Kafka KRaft y creación del topic" "$CARLOS" "$M1" "semana-1,kafka,prioridad-media" \
"Compose con Kafka, servicio kafka-init que crea los topics y Kafka UI.

**Criterios de aceptación**
- [ ] docker compose config --quiet sin errores
- [ ] kafka-init muestra el topic con 3 particiones"

issue "README inicial, diagrama de arquitectura y checkpoint Semana 1" "$LIZETH" "$M1" "semana-1,docs,prioridad-media" \
"README con problema, equipo, variante, arquitectura, contrato, KPIs y alertas. Diagrama y checkpoint.

**Criterios de aceptación**
- [ ] README.md
- [ ] docs/arquitectura.png
- [ ] docs/checkpoints/semana1.md (máx. 1 página)"

# Semana 2
issue "Producer: simulador de pedidos por zona con picos" "$KEVIN" "$M2" "semana-2,kafka,prioridad-alta" \
"Producer en Python que publica el ciclo de vida de los pedidos con key zone_id, partitioner murmur2_random y modos de pico y baja de repartidores.

**Criterios de aceptación**
- [ ] Eventos válidos contra el schema
- [ ] Mensajes repartidos en las 3 particiones (evidencia con kafka-get-offsets)
- [ ] Servicio producer en docker-compose"

issue "Spark: lectura de Kafka, parseo y transformaciones" "$CARLOS" "$M2" "semana-2,spark,datalake,prioridad-alta" \
"readStream desde Kafka, parseo con el schema, validación, deduplicación por event_id, franja horaria. Escritura a raw y processed en Parquet con checkpoint.

**Criterios de aceptación**
- [ ] Mínimo 3 transformaciones
- [ ] Parquet en datalake/raw y datalake/processed
- [ ] Checkpoint por query"

issue "Spark: ventanas, KPIs y reglas de alerta" "$CARLOS" "$M2" "semana-2,spark,prioridad-alta" \
"Pedidos por minuto (tumbling 1 min), índice de calor (5 min / 1 min), alertas DEMANDA_ALTA y CAPACIDAD_INSUFICIENTE con config/umbrales.yaml.

**Criterios de aceptación**
- [ ] Agregación por ventana con watermark
- [ ] 2 reglas de alerta escritas en curated/alertas
- [ ] Al menos una alerta real observada con el modo pico del producer"

issue "Pruebas de reglas y workflow ci.yml" "$LIZETH" "$M2" "semana-2,ci,prioridad-alta" \
"Pruebas unitarias de las reglas de alerta (funciones puras) y ci.yml con push, pull_request y workflow_dispatch.

**Criterios de aceptación**
- [ ] compileall + pytest + docker compose config en CI
- [ ] Action visible en cada PR
- [ ] Evidencia de un CI que falla y se recupera"

issue "ADR-002 y checkpoint Semana 2" "$LIZETH" "$M2" "semana-2,docs,prioridad-media" \
"ADR-002 basado en una decisión tomada tras observar la ejecución (p. ej. balanceo de particiones o recalibración de umbrales) y checkpoint con valor real observado.

**Criterios de aceptación**
- [ ] docs/decisions/ADR-002-*.md con evidencia propia
- [ ] docs/checkpoints/semana2.md con enlace al PR que resolvió un problema real"

# Semana 3
issue "DAG de Airflow: calidad, agregación por franja y reporte" "$KEVIN" "$M3" "semana-3,airflow,prioridad-alta" \
"DAG con mínimo 5 tasks: check fuentes -> validar raw -> agregar por franja -> validar curated -> reporte. Con retries y XCom.

**Criterios de aceptación**
- [ ] ≥5 tasks con dependencias
- [ ] retries configurados y al menos un XCom
- [ ] Airflow en docker-compose y DAG ejecutado"

issue "Ampliar CI: validar/importar DAG y nuevas pruebas" "$CARLOS" "$M3" "semana-3,ci,prioridad-alta" \
"Agregar al CI la importación del DAG (sin errores de carga) y pruebas de las nuevas reglas de calidad.

**Criterios de aceptación**
- [ ] Paso de CI que importa el DAG
- [ ] CI verde"

issue "Fallo controlado, recuperación, ADR-003 y checkpoint Semana 3" "$LIZETH" "$M3" "semana-3,airflow,docs,prioridad-alta" \
"Provocar un fallo (Kafka detenido, raw vacío o esquema inválido), documentar el log real, las tareas afectadas y la recuperación.

**Criterios de aceptación**
- [ ] Evidencia de ejecución fallida y recuperada en Airflow
- [ ] docs/decisions/ADR-003-*.md
- [ ] docs/checkpoints/semana3.md"

# Semana 4
issue "Integración end-to-end y estabilización del pipeline" "$KEVIN" "$M4" "semana-4,kafka,spark,airflow,prioridad-alta" \
"Levantar todo con docker compose siguiendo el README y corregir los problemas de integración.

**Criterios de aceptación**
- [ ] Pipeline completo ejecutable desde cero siguiendo el README"

issue "Quality gate y GitHub Release v1.0.0" "$CARLOS" "$M4" "semana-4,ci,prioridad-alta" \
"CI final verde antes del tag, ejecución manual (workflow_dispatch) por cada integrante, tag y release notes.

**Criterios de aceptación**
- [ ] 3 ejecuciones workflow_dispatch (una por integrante)
- [ ] Tag y release v1.0.0 con notas"

issue "README final, tabla de contribuciones y video" "$LIZETH" "$M4" "semana-4,docs,prioridad-alta" \
"README final según la sección 10 de la guía y video de máximo 10 minutos con los tres integrantes.

**Criterios de aceptación**
- [ ] README completo con resultados y limitaciones
- [ ] Tabla de contribuciones con Issues y PR
- [ ] Video enlazado desde README o release"

echo "Listo. Ahora agrega los Issues al Project (Kanban)."
