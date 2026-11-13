# Guion del video final (máximo 10 minutos)

Reparto según la sección 11 de la guía. Los tres hablan y cada uno explica algo técnico que implementó o revisó. Los textos son una base: díganlos con sus palabras.

**Antes de grabar** (unos 15 minutos antes):

1. Empezar de cero (README, "Para empezar de cero") y `docker compose up -d --build`.
2. Esperar 3 minutos y activar el pico: `docker compose stop producer` y `PICO_ZONA=BOG-CHA REDUCIR_ZONA=BOG-SUB docker compose up -d producer`.
3. Esperar a que aparezcan líneas `[ALERTA]` en `docker compose logs spark` y correr el DAG una vez en Airflow para que quede en verde.
4. Dejar abiertas estas pestañas: el repo en GitHub, Kafka UI (http://localhost:8080), Airflow (http://localhost:8081) y una terminal de Git Bash en la carpeta del repo.

| Tiempo | Quién | Qué se muestra | Qué se dice (idea) |
|---|---|---|---|
| 0:00-1:00 | Santiago | README: problema de negocio | Somos el Equipo 2, variante 5, Demanda por zonas. En almuerzo y cena algunas zonas reciben más pedidos de los que sus repartidores pueden atender. El objetivo: detectarlo minuto a minuto y alertar a tiempo. |
| 1:00-2:30 | Carlos | Diagrama de arquitectura y tabla de Tecnologías | Recorrido del flujo producer → Kafka → Spark → Data Lake → Airflow. Decisiones: key `zone_id` con 3 particiones (ADR-001 y su verificación en el ADR-002: p1 con el 50 %), ventanas de 5 y 15 minutos con watermark de 2. |
| 2:30-3:30 | Santiago | Terminal: `docker compose ps`; Kafka UI con el topic y sus 3 particiones; `docker compose logs producer` | El producer simula 12 zonas con reloj x10. El log muestra los mensajes confirmados por partición. Activamos un pico x2.5 en Chapinero y la mitad de repartidores en Suba. |
| 3:30-4:45 | Carlos | `docker compose logs spark` (`[progreso]` y `[ALERTA]`); carpetas de `datalake/` | Spark valida, deduplica y calcula la franja; las 5 queries escriben Parquet con checkpoint. Aquí salta DEMANDA_ALTA CRITICAL en BOG-CHA y CAPACIDAD_INSUFICIENTE en BOG-SUB. |
| 4:45-5:45 | Lizeth | Airflow: Graph del DAG, un run en verde, log de `validar_raw`, XCom y `ultimo_reporte.md` | El DAG revisa cada hora que los datos lleguen y sean confiables: 6 tareas, 2 reintentos y métricas por XCom. Contar en 20 segundos el fallo controlado (fuente detenida, 3 intentos, recuperación con Clear). |
| 5:45-6:30 | Santiago | `python scripts/verificar_pipeline.py` | El verificador de la Semana 4 revisa Kafka, Data Lake y Airflow de una vez. Contar el arreglo de estabilización: Spark se caía porque dos queries escribían las alertas en la misma carpeta; ahora cada tipo tiene la suya y Spark se reinicia solo si falla. |
| 6:30-8:00 | Lizeth | GitHub: Projects (Kanban), un Issue cerrado por su PR, un PR con review y checks, Actions (CI y Release), Releases | Trabajamos con Issue → rama → PR → review → merge. Cada PR corre el CI; el release final solo se publica si pasa el quality gate (CI completo + documentación). |
| 8:00-9:00 | Carlos | README, sección Resultados | Valores reales: reparto 19.6 / 50 / 30.4 %, 31 a 113 filas/s en Spark, ~1 % de inválidos, 503 alertas, BOG-CHA como zona con más pedidos. |
| 9:00-10:00 | Los tres (unos 20 s cada uno) | README, sección Limitaciones y trabajo futuro | Un problema encontrado y lo aprendido (por ejemplo: el lag que no se veía en Kafka UI, Spark cayéndose en el fallo controlado, la cuenta de GitHub perdida en la Semana 1) y una mejora futura. |

**Después de grabar:** subir el video (YouTube como "No listado", o Google Drive con acceso para cualquiera con el enlace), reemplazar en el README el marcador del enlace del video (aparece dos veces) por el enlace real y probar que abra en una ventana de incógnito.
