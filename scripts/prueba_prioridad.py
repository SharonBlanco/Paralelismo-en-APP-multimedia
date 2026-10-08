"""
=== PRUEBA DE PRIORIDADES ===
Demuestra que un caso urgente se adelanta a los que ya estaban esperando.

1. Envía casos de video con prioridad BAJA (1), para que se forme fila en
   la cola de video (todas sus sub-tareas van a la misma cola).
2. Espera a que haya sub-tareas esperando en esa cola.
3. Envía un caso de video con prioridad ALTA (10).
4. Espera a que terminen todos y muestra en qué orden los workers TOMARON
   las sub-tareas (la prioridad decide el orden de inicio, no el de fin).

Uso (con el coordinador y los workers encendidos):
  python scripts/prueba_prioridad.py
  python scripts/prueba_prioridad.py --bajos caso_012_video caso_013_video --urgente caso_014_video

Si el coordinador está en otra computadora: export COORDINATOR_URL=http://<IP>:8000
"""

import argparse
import sys
import time
from datetime import datetime
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "cliente"))
import cliente  # reutiliza el envío por partes y la espera del cliente

URL = cliente.COORDINATOR_URL


def esperar_fila(minimo, timeout=300):
    """Espera hasta que la cola de video tenga al menos `minimo` sub-tareas esperando"""
    t0 = time.time()
    while time.time() - t0 < timeout:
        q = requests.get(f"{URL}/api/stats", timeout=30).json().get("queues", {}).get("video", {})
        esperando = q.get("waiting", 0)
        print(f"\r[*] Sub-tareas esperando en la cola de video: {esperando}   ", end="", flush=True)
        if esperando >= minimo:
            print()
            return esperando
        time.sleep(1)
    print("\n[!] No se formó fila a tiempo; se envía el urgente igual")
    return 0


def hora(iso):
    return datetime.fromisoformat(iso) if iso else None


def main():
    ap = argparse.ArgumentParser(description="Prueba de prioridades")
    ap.add_argument("--dataset", default="./dataset_real")
    ap.add_argument("--bajos", nargs="+", default=["caso_012_video", "caso_013_video"],
                    help="casos que se envían con prioridad 1")
    ap.add_argument("--urgente", default="caso_014_video", help="caso que se envía con prioridad 10")
    ap.add_argument("--fila-minima", type=int, default=6,
                    help="cuántas sub-tareas tienen que estar esperando antes de enviar el urgente")
    args = ap.parse_args()

    ds = Path(args.dataset)
    print("=" * 60)
    print("PRUEBA DE PRIORIDADES")
    print("=" * 60)

    # 1. casos de prioridad baja
    bajos = []
    for nombre in args.bajos:
        r = cliente.enviar_caso(ds / nombre, f"PRIO-1 {nombre}", prioridad=1)
        if not r:
            sys.exit("[!] No se pudo enviar un caso de prioridad baja")
        bajos.append(r["case_id"])

    # 2. esperar a que se forme fila
    esperar_fila(args.fila_minima)

    # 3. caso urgente
    r = cliente.enviar_caso(ds / args.urgente, f"PRIO-10 {args.urgente}", prioridad=10)
    if not r:
        sys.exit("[!] No se pudo enviar el caso urgente")
    urgente = r["case_id"]
    t_urgente = datetime.now().astimezone()
    print(f"[*] Caso urgente enviado a las {t_urgente:%H:%M:%S}")

    # 4. esperar a que terminen
    cliente.esperar(bajos + [urgente])

    # 5. analizar en qué orden se tomaron las sub-tareas
    filas = []
    for cid in bajos + [urgente]:
        d = requests.get(f"{URL}/api/cases/{cid}", timeout=30).json()
        for s in d["subtasks"]:
            if s.get("assigned_at"):
                filas.append({"prio": d["case"]["priority"], "caso": d["case"]["case_name"],
                              "archivo": s["file_name"], "op": s["operation"],
                              "worker": s["assigned_worker"], "tomada": hora(s["assigned_at"]),
                              "fin": hora(s["finished_at"])})
    filas.sort(key=lambda f: f["tomada"])

    urg = [f for f in filas if f["prio"] == 10]
    if not urg:
        sys.exit("[!] No hay datos de las sub-tareas del caso urgente")
    ultima_urgente = max(f["tomada"] for f in urg)
    # Sub-tareas de prioridad baja tomadas DESPUÉS de que llegó el urgente pero
    # ANTES de que se tomara la última del urgente: no deberían existir.
    adelantadas = [f for f in filas if f["prio"] == 1 and t_urgente < f["tomada"] < ultima_urgente]
    bajas_esperando = [f for f in filas if f["prio"] == 1 and f["tomada"] > t_urgente]

    print("\n" + "=" * 60)
    print("ORDEN EN QUE LOS WORKERS TOMARON LAS SUB-TAREAS")
    print("=" * 60)
    marcado = False
    for f in filas:
        if not marcado and f["tomada"] > t_urgente:
            print(f"---------------- {t_urgente:%H:%M:%S}  llega el caso urgente (prioridad 10) ----------------")
            marcado = True
        print(f"{f['tomada']:%H:%M:%S}  prio {f['prio']:>2}  {f['worker'] or '-':9s}  "
              f"{f['op']:18s}  {f['archivo'][:34]}")

    def fin_caso(prio):
        return max(f["fin"] for f in filas if f["prio"] == prio and f["fin"])

    print("\n" + "=" * 60)
    print("RESULTADO")
    print("=" * 60)
    print(f"Sub-tareas de prioridad 1 que todavía esperaban cuando llegó el urgente: {len(bajas_esperando)}")
    print(f"Sub-tareas del urgente (prioridad 10): {len(urg)}")
    print(f"Sub-tareas de prioridad 1 que se tomaron antes que el urgente terminara de repartirse: {len(adelantadas)}")
    print(f"Terminó el urgente: {fin_caso(10):%H:%M:%S}  ·  terminaron los de prioridad 1: {fin_caso(1):%H:%M:%S}")
    if not bajas_esperando:
        print("\n[!] No quedaban tareas de prioridad 1 esperando: no hubo fila y la prueba no demuestra nada.")
        print("    Repetila con más casos bajos (--bajos ...) o con menos workers encendidos.")
    elif not adelantadas:
        print("\n[✓] PRIORIDAD RESPETADA: desde que llegó el caso urgente, los workers tomaron primero")
        print("    todas sus sub-tareas, aunque las de prioridad 1 llevaban más tiempo esperando.")
    else:
        print("\n[!] Algunas sub-tareas de prioridad 1 se tomaron antes que las del urgente:")
        for f in adelantadas:
            print(f"    {f['tomada']:%H:%M:%S}  {f['worker']}  {f['archivo']}")


if __name__ == "__main__":
    main()
