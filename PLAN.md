# Prefect Demo: 3 Dummy-Jobs mit Scheduling, Trigger & Logging

## Context

Der Nutzer möchte den Job-Orchestrator **Prefect** (prefect.io) ausprobieren (Klarstellung: "perfect" war eine Verwechslung mit "Prefect" — bestätigt durch den Nutzer, da `/home/puck/projects/perfect` komplett leer ist und es keinen Orchestrator namens "perfect" gibt). Ziel ist ein minimalistisches, komplett dockerisiertes Beispiel mit:
- zwei Dummy-Jobs, die **regelmäßig** (Cron-Schedule) laufen
- einem dritten Dummy-Job, der **von einem anderen Job getriggert** wird (kein eigener Schedule) — job-a startet job-c direkt per API-Call, ähnlich einem Webhook, inkl. Parameterübergabe
- explizitem Einsatz der **Logging-Features** von Prefect (verschiedene Log-Level, sichtbar in der Web-UI)
- explizitem Einsatz der **Lineage-Features** von Prefect (Assets / `@materialize`), um Datenabhängigkeiten zwischen den Jobs sichtbar zu machen
- Ausführung via **Docker Compose**
- Jobs in **Python**

Alles wird von Grund auf neu angelegt, da das Verzeichnis leer ist.

**Wichtige Einschränkung zu Lineage:** Prefect hat ein Lineage-Feature ("Assets", `@materialize`-Decorator mit `asset_deps` für Upstream/Downstream-Abhängigkeiten). Die dazugehörige grafische Lineage-Übersicht in der Web-UI ist aber aktuell **nur in Prefect Cloud verfügbar, nicht im selbst gehosteten Open-Source-Server**. Der Nutzer hat sich bewusst für **selbst gehostet** entschieden: Assets werden im Code voll genutzt (Materialisierungs-Events, `asset_deps`), sichtbar wird das im selbst gehosteten Server über den **Event-Feed** (jede Materialisierung erzeugt ein `prefect.asset.materialization.succeeded`-Event mit der Asset-URI als Resource) und über den **Task-Graphen jedes einzelnen Flow-Runs**, nicht über eine dedizierte Asset-Lineage-Grafik. Falls die volle grafische Lineage-Ansicht später gewünscht ist, reicht ein Wechsel von `PREFECT_API_URL` auf einen Prefect-Cloud-Workspace — der Asset-Code selbst ist unverändert kompatibel.

## Architektur

3 Docker-Compose-Services, kein separates Worker/Work-Pool-Setup nötig (Prefect's `serve()` reicht für ein minimalistisches Setup):

1. **postgres** — Backend-Datenbank für den Prefect Server (Standard-Postgres-Image)
2. **prefect-server** — Prefect API + Web-UI (offizielles Image `prefecthq/prefect`), Port 4200, mit Healthcheck
3. **runner** — eigenes, kleines Image (gebaut aus lokalem `Dockerfile`, basierend auf demselben Prefect-Image), das die drei Flows lädt und via `serve()` gleichzeitig hostet: zwei mit Cron-Schedule, eine ganz ohne Schedule (nur direkt aufrufbar)

**Update nach Test-Implementierung (wichtige Abweichung vom ursprünglichen Plan):**
- Der ursprüngliche Ansatz nutzte einen **Deployment-Trigger** (`DeploymentEventTrigger`, server-seitige Automation), der auf das `prefect.flow-run.Completed`-Event von job-a reagiert. Beim Testen zeigte sich das als **unzuverlässig**: automatisch per Cron gestartete Läufe von job-a lösten den job-c-Trigger reproduzierbar NICHT aus, während manuell gestartete Läufe zuverlässig funktionierten (auch nach diversen Fixversuchen: expliziter Wildcard-Match, korrekte String-Templates). Das liegt vermutlich an einem Prefect-eigenen Quirk mit dem zusätzlichen `auto-scheduled`-Tag, das Cron-Läufe als zusätzliche Related-Resource am Event mitführen.
- Redis (`redis:7` als `PREFECT_SERVER_EVENTS_MESSAGING_BROKER`/`_CACHE`, Paket `prefect_redis` ist im offiziellen Image bereits enthalten) wurde probeweise ergänzt, um die Event-Zustellung robuster zu machen — hat das Automation-Problem aber nicht behoben.
- **Finale Lösung:** job-a ruft am Ende seines Flows direkt `prefect.deployments.run_deployment("job-c/job-c", parameters={...}, timeout=0)` auf — ein expliziter, webhook-artiger Aufruf im Code statt einer serverseitigen Automation. Das ist zuverlässig (in Tests wiederholt erfolgreich, sowohl manuell als auch per Cron) und übergibt dabei Parameter. `run_deployment` verlinkt den job-c-Lauf standardmäßig als **Subflow** von job-a, wodurch der Zusammenhang auch in der UI sichtbar bleibt (Flow-Run-Graph zeigt die Parent-Child-Beziehung).
- job-c braucht dadurch **keinen Trigger und kein Automation-Setup mehr** — einfach `job_c.to_deployment(name="job-c")` ohne `schedule`/`triggers`.
- **Redis wurde wieder entfernt.** Der einzige verbleibende Grund war "robustere Zustellung der Asset-Materialisierungs-Events". Das war aber schon *vor* dem Redis-Einbau nachweislich zuverlässig — die Materialisierungs-Events von job-a/job-b waren über `/events/filter` problemlos abrufbar, mit dem Standard-"memory"-Broker. Nur die Trigger-Auswertung (mehrere unabhängige Consumer auf demselben In-Memory-Bus) war betroffen, und die gibt es jetzt nicht mehr. Zurück zu 3 statt 4 Containern, minimal wie ursprünglich gewünscht.

## Weitere Iterationen nach dem initialen Setup

- **job-a würfelt eine Zufallszahl** (1–100) in seiner `@materialize`-Task (`produce_job_a_output`) — simuliert, dass job-a "etwas tut" und ein Ergebnis produziert. Abhängig vom Ergebnis triggert job-a job-c **mehrfach** (`(number - 50) // 10 + 1`-mal ab einem Wert > 50), um zu zeigen, dass ein Job einen anderen auch mehrfach anstoßen kann.
- **Konkurrenz-Limit:** `serve()` überschreibt `PREFECT_RUNNER_PROCESS_LIMIT` immer mit seinem eigenen `limit`-Parameter (Default `None` = unbegrenzt) — die Env-Variable wird deshalb explizit in `runner.py` ausgelesen und an `serve(limit=...)` durchgereicht, statt sich auf das (in Kombination mit `serve()` wirkungslose) Prefect-Setting zu verlassen.
- **Exakt gepinnte Version:** `prefecthq/prefect:3.8.3-python3.14` statt eines "floating" Tags wie `3-python3.12`, für reproduzierbare Builds. Conda-Image-Varianten wurden geprüft und bewusst nicht verwendet — sie bieten nur einen alternativen Paketmanager für eine einzelne Umgebung, keine Multi-Version-Isolation.
- **Externes Triggern per HTTP:** Jedes Deployment (auch job-c) lässt sich zusätzlich zum internen Trigger ganz normal über die Prefect-REST-API von außen anstoßen (`POST /api/deployments/{id}/create_flow_run`) — z.B. aus einem Slurm-Batch-Skript oder anderen externen Prozessen. Dafür gibt es `trigger_job_c.sh` (reines Bash + curl, kein Python nötig).

## Dateien

```
/home/puck/projects/perfect/
├── docker-compose.yml
├── Dockerfile
├── trigger_job_c.sh      # job-c manuell/extern per HTTP triggern
├── PLAN.md
└── flows/
    ├── job_a.py     # regelmäßig, Cron alle 2 Minuten; würfelt Zufallszahl, triggert job-c ggf. mehrfach
    ├── job_b.py     # regelmäßig, Cron alle 3 Minuten
    ├── job_c.py     # kein Schedule, wird von job_a getriggert (und ist auch extern triggerbar)
    └── runner.py    # deployt & served alle drei Flows, liest PREFECT_RUNNER_PROCESS_LIMIT selbst aus
```

## Logging-Features, die sichtbar werden

- Log-Level-Trennung (DEBUG/INFO/WARNING) über `PREFECT_LOGGING_LEVEL` steuerbar
- Flow-Run-Logs und Task-Run-Logs getrennt im UI unter *Flow Runs → [Run] → Logs*
- `log_prints=True`, damit auch normale `print()`-Ausgaben im UI landen
- Strukturierte Log-Metadaten (Timestamp, Level, Flow/Task-Run-Name) automatisch durch `get_run_logger()`

## Lineage-Features, die sichtbar werden (selbst gehostet)

- `@materialize` + `asset_deps` im Code zeigen echte Asset-Lineage (job-c hängt explizit von job-a's Asset ab)
- Jede Materialisierung erzeugt ein `prefect.asset.materialization.succeeded`-Event, sichtbar im **Event-Feed** der UI, gefiltert/durchsuchbar nach der Asset-URI (`demo://job-a/output` etc.)
- Der **Task-Graph eines einzelnen Flow-Runs** (UI → Flow Run → Graph) zeigt die `@materialize`-Task als Knoten
- Keine dedizierte, cross-flow Asset-Lineage-Grafik (Cloud-only) — das ist die bewusst akzeptierte Einschränkung des selbst gehosteten Setups

## Verifikation

1. `docker compose up --build` im Projektverzeichnis
2. UI unter `http://localhost:4200` öffnen, warten bis `prefect-server` healthy und `runner` gestartet ist
3. Unter *Deployments* prüfen, dass `job-a`, `job-b`, `job-c` gelistet sind (job-c ohne Schedule)
4. Nach spätestens 2 Minuten sollte ein `job-a`-Run erscheinen, nach 3 Minuten ein `job-b`-Run, und **kurz nach jedem `job-a`-Completion** (wenige Sekunden) ein oder mehrere `job-c`-Runs als Subflow(s) von job-a
5. In den Flow-Run-Details von job-a/b/c die Logs öffnen und die verschiedenen Log-Level + Task-Logs + Print-Ausgaben begutachten
6. Im **Event-Feed** nach `prefect.asset.materialization` filtern und prüfen, dass für job-a, job-b, job-c jeweils Materialisierungs-Events mit den richtigen Asset-URIs und (bei job-c) der Abhängigkeit zu job-a's Asset auftauchen
7. `./trigger_job_c.sh` ausführen, um job-c zusätzlich von außen anzustoßen
8. `docker compose down` zum Aufräumen

## Status

Das Setup wurde vollständig gebaut und mehrfach live getestet (`docker compose up -d --build`): job-a und job-b laufen zuverlässig per Cron inkl. aller Log-Level und Asset-Materialisierung; job-a startet job-c zuverlässig sowohl bei manuellen als auch bei Cron-Läufen; externes Triggern per HTTP/Bash-Skript funktioniert; Concurrency-Limit greift nachweislich.
