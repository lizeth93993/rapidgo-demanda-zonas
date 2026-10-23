"""Verifica que la simulacion de particiones usa el mismo hash que Kafka (evidencia del ADR-001)."""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from simular_particiones import murmur2, particion  # noqa: E402

# Vectores de prueba de org.apache.kafka.common.utils.UtilsTest
VECTORES = {
    b"21": -973932308,
    b"foobar": -790332482,
    b"a-little-bit-long-string": -985981536,
    b"a-little-bit-longer-string": -1486304829,
    b"lkjh234lh9fiuh90y23oiuhsafujhadof229phr9h19h89h8": -58897971,
    b"abc": 479470107,
}


@pytest.mark.parametrize("data,esperado", VECTORES.items())
def test_murmur2_igual_a_kafka(data, esperado):
    assert murmur2(data) == esperado


def test_misma_zona_siempre_misma_particion():
    assert {particion("BOG-CHA", 3) for _ in range(100)} == {particion("BOG-CHA", 3)}
