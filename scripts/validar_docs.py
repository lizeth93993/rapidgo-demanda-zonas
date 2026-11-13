"""Quality gate de documentacion (Issue #16).

Revisa que la documentacion este lista para el release final:
  - existen los README, ADR y checkpoints de las 4 semanas;
  - el README tiene las secciones obligatorias de la guia (seccion 10);
  - cada checkpoint tiene sus 5 secciones (guia, 9.1) y ninguna esta vacia;
  - no quedan textos de plantilla sin llenar ([COMPLETAR], [NOMBRE], #__, ...);
  - los enlaces relativos del README apuntan a archivos que existen.

Uso:
  python scripts/validar_docs.py              # informa problemas y termina con 0 (aviso, en cada PR)
  python scripts/validar_docs.py --estricto   # termina con 1 si hay problemas (lo usa release.yml)
"""
from __future__ import annotations

import argparse
import re
import sys
import unicodedata
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

ARCHIVOS_OBLIGATORIOS = [
    "README.md",
    "datalake/README.md",
    "airflow/README.md",
    "docs/arquitectura.png",
    "docs/decisions/ADR-001-topic-key-particiones.md",
    "docs/decisions/ADR-002-ajuste-tras-ejecucion.md",
    "docs/decisions/ADR-003-orquestacion-y-recuperacion.md",
    "docs/checkpoints/semana1.md",
    "docs/checkpoints/semana2.md",
    "docs/checkpoints/semana3.md",
]

# Seccion 10 de la guia -> palabra que debe aparecer en algun titulo (##) del README
SECCIONES_README = {
    "Problema de negocio": "problema",
    "Arquitectura con diagrama": "arquitectura",
    "Tecnologias y rol de cada una": "tecnologia",
    "Pasos para ejecutar localmente": "ejecutar",
    "Topics y eventos": "kafka",
    "KPIs y reglas": "kpis",
    "Pruebas y GitHub Actions": "pruebas",
    "Airflow y el DAG": "airflow",
    "Resultados o evidencias": "resultados",
    "Limitaciones y trabajo futuro": "limitaciones",
    "Tabla de contribuciones": "contribuciones",
    "Enlace al video final": "video",
}

PLANTILLA = [
    (re.compile(r"\[COMPLETAR[^\]]*\]", re.IGNORECASE), "texto de plantilla [COMPLETAR]"),
    (re.compile(r"\[NOMBRE\]"), "nombre sin llenar [NOMBRE]"),
    (re.compile(r"\[ENLACE[_ ]VIDEO\]", re.IGNORECASE), "falta el enlace del video"),
    (re.compile(r"#__\b|#_\(completar\)_", re.IGNORECASE), "numero de PR sin llenar"),
    (re.compile(r"_\(completar[^)]*\)_", re.IGNORECASE), "texto de plantilla _(completar)_"),
]

ENLACE = re.compile(r"\]\((?!https?://|#|mailto:)([^)\s]+)\)")


def _normalizar(texto: str) -> str:
    sin_tildes = unicodedata.normalize("NFKD", texto).encode("ascii", "ignore").decode()
    return sin_tildes.lower()


def faltantes(raiz: Path) -> list[str]:
    return [f"falta el archivo {a}" for a in ARCHIVOS_OBLIGATORIOS if not (raiz / a).exists()]


def secciones_faltantes(readme: str) -> list[str]:
    titulos = [_normalizar(l) for l in readme.splitlines() if l.lstrip().startswith("#")]
    return [f"README sin seccion: {nombre} (un titulo que contenga '{clave}')"
            for nombre, clave in SECCIONES_README.items() if not any(clave in t for t in titulos)]


def checkpoints_incompletos(raiz: Path) -> list[str]:
    """Cada checkpoint debe tener las secciones 1 a 5 y texto debajo de cada una."""
    problemas = []
    for archivo in sorted((raiz / "docs" / "checkpoints").glob("semana*.md")):
        nombre = archivo.relative_to(raiz).as_posix()
        secciones: dict[str, list[str]] = {}
        actual = None
        for linea in archivo.read_text(encoding="utf-8").splitlines():
            m = re.match(r"##\s*(\d)\.", linea)
            if m:
                actual = m.group(1)
                secciones[actual] = []
            elif actual and linea.strip() not in ("", "-"):
                secciones[actual].append(linea)
        for n in "12345":
            if n not in secciones:
                problemas.append(f"{nombre}: falta la seccion {n}")
            elif not secciones[n]:
                problemas.append(f"{nombre}: la seccion {n} esta vacia")
    return problemas


def textos_de_plantilla(raiz: Path) -> list[str]:
    problemas = []
    archivos = [raiz / "README.md", raiz / "datalake" / "README.md", raiz / "airflow" / "README.md"]
    archivos += sorted((raiz / "docs").rglob("*.md")) if (raiz / "docs").exists() else []
    for archivo in archivos:
        if not archivo.exists():
            continue
        for n, linea in enumerate(archivo.read_text(encoding="utf-8").splitlines(), start=1):
            for patron, motivo in PLANTILLA:
                if patron.search(linea):
                    problemas.append(f"{archivo.relative_to(raiz).as_posix()}:{n}: {motivo}")
                    break
    return problemas


def enlaces_rotos(raiz: Path, readme: str) -> list[str]:
    rotos = []
    for destino in ENLACE.findall(readme):
        ruta = destino.split("#")[0]
        if ruta and not (raiz / ruta).exists():
            rotos.append(f"README enlaza a {ruta}, que no existe")
    return rotos


def revisar(raiz: Path = ROOT) -> list[str]:
    raiz = Path(raiz)
    problemas = faltantes(raiz)
    readme_path = raiz / "README.md"
    if readme_path.exists():
        readme = readme_path.read_text(encoding="utf-8")
        problemas += secciones_faltantes(readme)
        problemas += enlaces_rotos(raiz, readme)
    problemas += checkpoints_incompletos(raiz)
    problemas += textos_de_plantilla(raiz)
    return problemas


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="Quality gate de documentacion")
    p.add_argument("--estricto", action="store_true", help="terminar con codigo 1 si hay problemas")
    p.add_argument("--raiz", default=str(ROOT))
    args = p.parse_args(argv)
    problemas = revisar(Path(args.raiz))
    if not problemas:
        print("Documentacion lista para el release: sin problemas.")
        return 0
    etiqueta = "ERROR" if args.estricto else "AVISO"
    for problema in problemas:
        print(f"[{etiqueta}] {problema}")
    print(f"\n{len(problemas)} problema(s) de documentacion.")
    if args.estricto:
        print("Quality gate: la documentacion no esta lista, NO se publica el release.")
        return 1
    print("(Solo aviso: en el release, con --estricto, esto detiene la publicacion.)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
