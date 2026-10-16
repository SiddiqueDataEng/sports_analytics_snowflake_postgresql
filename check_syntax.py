import ast, pathlib, sys
files = [
    "dashboard/conn.py",
    "dashboard/app.py",
    "dashboard/pages/management.py",
    "dashboard/pages/analytics.py",
]
root = pathlib.Path(__file__).parent
ok = True
for f in files:
    try:
        src = (root / f).read_text(encoding="utf-8")
        ast.parse(src)
        print(f"OK   {f}")
    except SyntaxError as e:
        print(f"FAIL {f}: {e}")
        ok = False
    except Exception as e:
        print(f"ERR  {f}: {e}")
        ok = False
sys.exit(0 if ok else 1)
