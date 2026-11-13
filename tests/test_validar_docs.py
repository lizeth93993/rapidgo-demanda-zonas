"""Pruebas del quality gate de documentacion (scripts/validar_docs.py)."""
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
import validar_docs as vd  # noqa: E402

README_COMPLETO = """# RapidGo
## Problema de negocio
## Arquitectura
![Arquitectura](docs/arquitectura.png)
## Tecnologías
## Ejecutar el pipeline
## Kafka
## KPIs y reglas de alerta
## Pruebas y CI
## Airflow: control de calidad
## Resultados
## Limitaciones y trabajo futuro
## Contribuciones
## Video final
Ver [ADR-001](docs/decisions/ADR-001-topic-key-particiones.md) y [la web](https://example.com).
"""


CHECKPOINT = "# Checkpoint\n" + "".join(f"## {n}. Seccion\nContenido real.\n" for n in range(1, 6))


@pytest.fixture
def repo(tmp_path):
    for archivo in vd.ARCHIVOS_OBLIGATORIOS:
        destino = tmp_path / archivo
        destino.parent.mkdir(parents=True, exist_ok=True)
        destino.write_text(CHECKPOINT if "checkpoints" in archivo else "# Documento\nTodo lleno.\n", encoding="utf-8")
    (tmp_path / "README.md").write_text(README_COMPLETO, encoding="utf-8")
    return tmp_path


def test_repo_completo_pasa(repo):
    assert vd.revisar(repo) == []
    assert vd.main(["--raiz", str(repo), "--estricto"]) == 0


def test_archivo_faltante(repo):
    (repo / "docs" / "checkpoints" / "semana3.md").unlink()
    assert vd.revisar(repo) == ["falta el archivo docs/checkpoints/semana3.md"]


def test_seccion_faltante(repo):
    (repo / "README.md").write_text(README_COMPLETO.replace("## Contribuciones\n", ""), encoding="utf-8")
    assert any("Tabla de contribuciones" in p for p in vd.revisar(repo))


def test_tildes_no_importan(repo):
    texto = README_COMPLETO.replace("## Tecnologías", "## TECNOLOGIAS USADAS")
    assert vd.secciones_faltantes(texto) == []


@pytest.mark.parametrize("texto,motivo", [
    ("PRs: #__ Estructura base", "numero de PR"),
    ("- **PR:** #_(completar)_", "numero de PR"),
    ("Valor: [COMPLETAR con su valor]", "[COMPLETAR]"),
    ("| [NOMBRE] | @x |", "[NOMBRE]"),
    ("Video: [ENLACE_VIDEO]", "video"),
])
def test_textos_de_plantilla(repo, texto, motivo):
    (repo / "docs" / "checkpoints" / "semana1.md").write_text(CHECKPOINT + f"{texto}\n", encoding="utf-8")
    problemas = vd.revisar(repo)
    assert len(problemas) == 1 and problemas[0].startswith("docs/checkpoints/semana1.md:12") and motivo in problemas[0]


def test_checkpoint_cortado(repo):
    """Caso real del repo: semana2.md quedo con '## 2. Valor real observado' y un guion."""
    (repo / "docs" / "checkpoints" / "semana2.md").write_text(
        "# Checkpoint\n## 1. Que quedo\nAlgo.\n## 2. Valor real observado\n-\n", encoding="utf-8")
    assert vd.revisar(repo) == [
        "docs/checkpoints/semana2.md: la seccion 2 esta vacia",
        "docs/checkpoints/semana2.md: falta la seccion 3",
        "docs/checkpoints/semana2.md: falta la seccion 4",
        "docs/checkpoints/semana2.md: falta la seccion 5",
    ]


def test_enlace_roto(repo):
    (repo / "README.md").write_text(README_COMPLETO + "[guion](docs/video/guion.md)\n", encoding="utf-8")
    assert vd.revisar(repo) == ["README enlaza a docs/video/guion.md, que no existe"]


def test_modo_aviso_no_falla(repo, capsys):
    (repo / "README.md").write_text("# Solo titulo\n", encoding="utf-8")
    assert vd.main(["--raiz", str(repo)]) == 0
    assert "AVISO" in capsys.readouterr().out
    assert vd.main(["--raiz", str(repo), "--estricto"]) == 1
