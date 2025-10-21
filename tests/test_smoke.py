import importlib
import pathlib

def test_imports():
    # PWT module must import
    pwt = importlib.import_module("src.pwt.pwt_loader_clean")
    assert hasattr(pwt, "load_pwt"), "load_pwt() debe existir en el loader de PWT"

    # BanRep module (si existe alguno conocido) debe importar
    banrep_candidates = ["banrep_consolidate_v3", "banrep_loader_clean_v3"]
    ok = False
    for name in banrep_candidates:
        try:
            importlib.import_module(f"src.banrep.{name}".format(name=name))
            ok = True
            break
        except ModuleNotFoundError:
            pass
    assert ok, "No se encontró un módulo BanRep conocido"

def test_repo_layout():
    root = pathlib.Path(__file__).resolve().parents[1]
    assert (root / "README.md").exists()
    assert (root / "CONTRIBUTING.md").exists()
