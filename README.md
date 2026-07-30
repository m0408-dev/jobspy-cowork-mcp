# JobSpy Cowork MCP Server

Ein **MCP-Server**, der echte Stellenanzeigen über **11 freie Quellen** durchsucht — die offizielle deutsche Bundesagentur-für-Arbeit-API, acht Remote-/Job-APIs (Himalayas, Remotive, RemoteOK, Arbeitnow, Jobicy, WeWorkRemotely, The Muse, HackerNews „Who is hiring") sowie Indeed + LinkedIn via [JobSpy](https://github.com/speedyapply/JobSpy) — gebaut als **Custom Connector für Claude Cowork** (oder Claude.ai / Desktop / Code).

**5 Tools:** `search_all_jobs` (alle Quellen parallel, dedupliziert — bestes Standard-Tool), `search_german_jobs` (Arbeitsagentur), `search_remote_jobs` (nur Remote), `search_jobs` (JobSpy direkt), `list_job_sources` (Quellen-Katalog). Alles frei, kein Key, keine Proxys nötig.

Der Server ist für **öffentlichen Betrieb gehärtet**: Per-IP-Rate-Limit, Concurrency-Cap um die Scraper (schützt die geteilte Datacenter-IP vor Bans), Non-Root-Container, Healthcheck + Watchdog. **Für viele Nutzer gilt trotzdem: eine 1-GB-Gratis-VM mit einer IP skaliert nicht beliebig — der robuste Weg ist, dass jede/r die eigene Gratis-Instanz deployt** (Anleitung unten).

> **Wie es gebaut ist:** reines Python mit [FastMCP](https://gofastmcp.com). JobSpy wird direkt im Prozess aufgerufen — **keine Shell, kein Docker-Subprocess**. Dadurch existiert die Command-Injection-Lücke typischer Wrapper hier gar nicht. Ein Codebase, zwei Betriebsarten: `stdio` (lokal) und `http` (öffentlich, für Cowork).

---

## v2 — Raw Pull: der Server holt, die KI filtert

Der Server trifft **keine Relevanz-Entscheidungen** mehr. Er maximiert die Trefferzahl und liefert zu jedem Job die Signale mit, die zum Filtern nötig sind (`relevance`, `remote_confidence`, `remote_signals`, `date_posted`). Nichts wird zurückgehalten, weil es „unpassend aussieht" — das ist Aufgabe der aufrufenden KI. Jede Kappung wird im Payload ausgewiesen (`size_cap_dropped`, `capped`, `total_available`), damit ein Ausschnitt nie wie „der ganze Markt" aussieht.

Was sich gegenüber v1 konkret geändert hat — alle Zahlen am 30.07.2026 gegen die Live-API gemessen:

| Problem in v1 | Messung | Fix in v2 |
|---|---|---|
| **Nur eine Seite Arbeitsagentur.** `size≤100`, `page=1`, fertig. | „IT-Support" deutschlandweit hat **8043** Anzeigen. v1 sah davon **max. 100**. | Tiefe Paginierung bis `results_per_source` (bis 1000), plus `total_available` im Ergebnis. |
| **Deutsche Komposita unsichtbar.** Ein Query findet nur eine Schreibweise. | `Nachtschicht` 97 Treffer, `Nachtdienst` 77 — **Schnittmenge 3**. Die beiden Mengen sind praktisch disjunkt. | `expand_query` (Default an) zerlegt Komposita und hängt Geschwister-Endungen an: `Nachtschicht` → `Nacht`, `Nachtdienst`, `Nachtwache`, `Nachtarbeit`… Alle Varianten werden parallel gefeuert und unioniert. Zusätzlich `search_terms=[...]` für eigene Wortvarianten in **einem** Aufruf. |
| **`remote_only=true` lieferte fast nichts.** Es setzte die Arbeitsagentur-Checkbox `arbeitszeit=ho`, die Arbeitgeber selbst ankreuzen müssen. | „IT-Support" ohne Filter **8043**, mit `arbeitszeit=ho` **0**. Gleichzeitig erwähnen **17 von 30** Anzeigentexten Homeoffice/Remote. | `remote_only` **filtert nicht mehr** — es ergänzt die Homeoffice-Spalte und sortiert Remote nach vorn. Der Remote-Status kommt jetzt aus dem Anzeigentext. |
| **`days_old` warf lebende Anzeigen weg.** | Deutsche Anzeigen bleiben Wochen bis Monate offen. | Default **`days_old=0`** (kein Datumsfilter). Das Datum steht am Job, die KI entscheidet über Aktualität. |
| **`remote_confidence` war geraten.** Die Listen-API liefert `description: null`, also wurde aus dem Titel geraten — „Sandwichartist" landete auf `likely`. | Detail-Endpoint liefert den vollen Text, ~3200 Zeichen im Schnitt, **~107 Anfragen/s**. | Beschreibungen werden nachgeladen (`fetch_details`, Default an) und das Urteil aus dem echten Text gebildet. Ohne Text sagt das Feld jetzt **`unknown`** statt zu raten. `remote_signals` nennt die Belegstellen. |
| **Firmenname als Treffer.** „It-excelsus GmbH" matchte auf „IT-Support". | — | Feldgewichtung: Titel 1.0, Text 0.5, Firma/Ort 0.15. Ein reiner Firmennamen-Treffer landet auf `relevance` ≈ 0 — wird aber **nicht gelöscht**, nur nach hinten sortiert. |
| **Dedup verschluckte Städte.** Schlüssel war Titel+Firma. | Eine Kette schreibt dieselbe Stelle in 30 Orten aus — v1 behielt eine. | Stadt gehört jetzt zum Dedup-Schlüssel (~45 % mehr Treffer bei einem Arbeitsagentur-Pull). |

**Wichtig zum deutschen Markt:** von den neun freien APIs decken realistisch nur **Arbeitsagentur** und **Arbeitnow** Deutschland ab; die anderen sieben sind US-lastige Remote-Boards und die Quelle des internationalen Rauschens. Für einen sauberen DE-Lauf: `sources=["arbeitsagentur","arbeitnow"]`.

**Zwei-Schritt-Muster:** erst breit mit `response_format="concise"` (kein Anzeigentext → ~5× mehr Jobs pro Antwort; der Text wird trotzdem serverseitig gelesen, um `remote_confidence` zu bilden), dann die Shortlist mit `response_format="detailed"` nachladen.

---

## ⚠️ Das Wichtigste zuerst: Cloud-IPs werden geblockt

Cowork verbindet sich **aus Anthropics Cloud** zu deinem Server — lokale MCP-Server funktionieren in Cowork **nicht**. Dein Scraper muss also öffentlich gehostet sein und läuft damit auf einer **Datacenter-IP**.

**LinkedIn, Glassdoor und Indeed blocken Datacenter-IPs gezielt.** Konsequenz:

| Betrieb | Kosten | LinkedIn/Glassdoor | Indeed/Google/ZipRecruiter |
|---|---|---|---|
| **Lokal** (dein Rechner, Heim-IP) | kostenlos | ✅ funktioniert | ✅ funktioniert |
| **Cloud ohne Proxys** | ~$7/mo Hosting | ❌ meist geblockt | ⚠️ oft ok, mal geblockt |
| **Cloud mit Residential-Proxys** | Hosting + Proxys (~$/GB) | ✅ funktioniert | ✅ funktioniert |

**Für Cowork brauchst du also Residential-Proxys**, damit alle Börsen zuverlässig laufen. Der Server ist vollständig proxy-ready (siehe [Proxys](#proxys)). Ohne Proxys kannst du dich in der Cloud auf Indeed/Google beschränken — die gehen oft auch so.

---

## Schnellstart: lokal testen (2 Minuten, kostenlos)

Voraussetzung: [uv](https://docs.astral.sh/uv/) (hast du bereits) oder Python 3.10+.

```bash
cd jobspy-cowork-mcp
uv sync                 # bzw.  pip install -r requirements.txt

# Sofort-Test ohne Claude — scrapt echte Jobs und zeigt sie an:
uv run python example_search.py "python developer" "Berlin, Germany"
```

Bekommst du eine Liste echter Jobs? Dann funktioniert alles. (Von deiner Heim-IP klappt das ohne Proxys.)

---

## Für Cowork deployen

Cowork braucht eine **öffentliche HTTPS-URL**, die Streamable HTTP spricht. Zwei Wege liegen bei:

| | Render | Oracle Cloud Always Free |
|---|---|---|
| Kosten | ~7 $/mo (Starter-Plan) | dauerhaft kostenlos (2 OCPU/12 GB ARM) |
| Setup-Aufwand | 5 Minuten, HTTPS inklusive | 20–30 Minuten, du bist der Sysadmin |
| HTTPS | automatisch (onrender.com) | selbst einrichten (Caddy, liegt bei) |
| Proxy-Bedarf für LinkedIn/Glassdoor | ja | **ja, genauso** — Oracle-IPs werden von Cloudflare/LinkedIn eher noch aggressiver geblockt als der Durchschnitt |

Render ist der schnellere Weg, Oracle der günstigere. Beide Anleitungen unten — [Railway](https://railway.app)/[Fly.io](https://fly.io) gehen ebenfalls, analog zu Render.

### Schritt 1 — Code auf GitHub

```bash
cd jobspy-cowork-mcp
git init && git add -A && git commit -m "JobSpy Cowork MCP server"
# neues, PRIVATES GitHub-Repo anlegen und pushen:
git remote add origin https://github.com/<dein-user>/jobspy-cowork-mcp.git
git push -u origin main
```

### Schritt 2 — auf Render deployen

1. Render → **New +** → **Blueprint** → dein Repo wählen. Render liest `render.yaml`.
2. Vor dem Deploy unter **Environment** setzen:
   - **`MCP_HTTP_PATH`** → ändere den zufälligen Teil, z. B. `/mcp-a8f3c2e1b9/`. **Das ist dein Zugangsschutz** — nur wer die volle URL kennt, erreicht den Server. Merk dir den Wert.
   - **`JOBSPY_PROXIES`** → deine Residential-Proxys (siehe unten). Ohne die klappen LinkedIn/Glassdoor in der Cloud nicht zuverlässig.
3. **Create** → warten, bis „Live". Du bekommst eine URL wie `https://jobspy-cowork-mcp.onrender.com`.
4. Test: `https://<deine-url>/health` muss `ok` zeigen.

> Nimm den **Starter-Plan ($7/mo)**, nicht Free — Free-Instanzen schlafen nach ~15 Min ein, dann läuft die erste Cowork-Anfrage in einen Timeout, bis der Server wieder wach ist.

Deine **Connector-URL** ist dann: `https://<deine-url>/mcp-a8f3c2e1b9/` (Host + dein geheimer Pfad).

### Alternative zu Schritt 2 — auf Oracle Cloud Always Free deployen

Kein API-Key/Token nötig — alles läuft über die Console-UI und ein Cloud-Init-Skript, das beim ersten Boot automatisch läuft.

1. **Instanz erstellen**: Oracle Console → **Compute → Instances → Create Instance**.
   - Name: `jobspy-mcp`
   - Image: **Ubuntu** (aktuelle LTS)
   - Shape: **Ändern** → Ampere (ARM) → `VM.Standard.A1.Flex`, 2 OCPU / 12 GB (Always-Free-Kontingent)
   - SSH-Key: neues Schlüsselpaar erzeugen lassen und den **privaten Key selbst herunterladen** (brauchst du für SSH-Zugriff später — bleibt bei dir)
   - **Advanced options → Management → Initialization script** → Inhalt von [`deploy/oracle-cloud-init.yaml`](deploy/oracle-cloud-init.yaml) einfügen, vorher `GITHUB_USER` durch deinen GitHub-Namen ersetzen
   - **Create**
2. **Firewall auf VCN-Ebene öffnen** (zusätzlich zum OS-Firewall, den das Skript schon regelt): Instanz-Detailseite → Subnetz-Link → Security List → **Add Ingress Rules** → TCP 80 und TCP 443, Source `0.0.0.0/0`.
3. **Öffentliche IP notieren** (steht auf der Instanz-Detailseite), dann per SSH verbinden (`ssh ubuntu@<ip>` mit dem heruntergeladenen Key) und ausführen:
   ```bash
   curl -O https://raw.githubusercontent.com/<dein-user>/jobspy-cowork-mcp/main/deploy/caddy-setup.sh
   chmod +x caddy-setup.sh
   ./caddy-setup.sh <deine-öffentliche-ip>
   ```
   Das richtet automatisches HTTPS ein (über [sslip.io](https://sslip.io), kein eigener Domainname nötig) und zeigt dir am Ende deine fertige Connector-URL.
4. Test: `https://<ip-mit-bindestrichen>.sslip.io/health` muss `ok` zeigen.
5. **Proxys nachtragen** (empfohlen für LinkedIn/Glassdoor): auf der VM in `/opt/jobspy-cowork-mcp/.env` eintragen und den Container neu starten (`docker restart jobspy`).

### Schritt 3 — als Connector in Cowork/Claude hinzufügen

1. In Claude/Cowork: **Settings → Connectors → Add custom connector**
   *(Bei Team/Enterprise muss das ein Owner unter Organization settings → Connectors tun.)*
2. Transport: **Streamable HTTP**. URL: deine Connector-URL von oben.
3. Speichern, verbinden. Fertig — Cowork hat jetzt das Tool `search_jobs` und kann bei deinen Bewerbungen selbst Jobs suchen.

---

## Proxys

Nötig für zuverlässiges Cloud-Scraping (v. a. LinkedIn/Glassdoor). Du brauchst **Residential-** oder **Mobile-Proxys** (Datacenter-Proxys werden ebenfalls geblockt). Anbieterkategorie: „residential rotating proxies" — es gibt viele, pro GB abgerechnet.

Format (JobSpy), als Env-Var `JOBSPY_PROXIES`, komma-getrennt:

```
user:pass@host:port,user:pass@host:port,localhost
```

- Bei Render/Railway/Fly trägst du das als **Secret Env-Var** ein (nicht in den Code!).
- Lokal in `.env` (siehe `.env.example`) — lokal brauchst du meist gar keine.

---

## Die Tools: `search_all_jobs` / `search_german_jobs`

Das Standard-Tool ist `search_all_jobs`; `search_german_jobs` ist derselbe Pull, nur auf die Arbeitsagentur beschränkt (dafür der tiefste).

| Parameter | Typ | Default | Beschreibung |
|---|---|---|---|
| `search_term` | str | – | Suchbegriff, **beliebiger Beruf** — kein IT-Tool |
| `location` | str | `"Germany"` | Stadt/Region; `"Germany"` = bundesweit |
| `search_terms` | list? | – | weitere Wortvarianten, im **selben** Aufruf unioniert, z. B. `["Nachtdienst","Nachtwache"]` |
| `results_per_source` (bzw. `results_wanted`) | int | `100` | Treffer **pro Quelle** vor Dedup, bis 1000. Die Arbeitsagentur paginiert dafür durch |
| `days_old` | int | `0` | `0` = kein Datumsfilter (empfohlen) |
| `remote_only` | bool | `false` | **Boost, kein Filter**: ergänzt die Homeoffice-Spalte, sortiert Remote nach vorn, löscht nichts |
| `expand_query` | bool | `true` | Komposita zerlegen + Geschwister-Endungen + Synonyme |
| `fetch_details` | bool | `true` | Anzeigentexte der Arbeitsagentur nachladen (Basis für `remote_confidence`) |
| `sources` | list? | alle 9 | für DE sinnvoll: `["arbeitsagentur","arbeitnow"]` |
| `response_format` | str | `"concise"` | `concise` = ohne Anzeigentext (viel mehr Jobs passen rein); `detailed` = mit Volltext |
| `include_jobspy` | bool | `false` | zusätzlich Indeed + LinkedIn scrapen (langsam, IP-limitiert) |
| `dach_only` | bool | `false` | optionaler Vorfilter gegen klar nicht-europäische Stellen |

Rückgabe: `{ count, found_before_size_cap, total_available, queries_used, duplicates_removed, per_source, jobs: [...] }`.

Pro Job: `title, company, location, date_posted` (ISO-normalisiert), `job_url`, `source`, `is_remote`, **`relevance`** (0–1, reines Sortiersignal), **`remote_confidence`** (`strict` | `hybrid` | `mixed` | `likely` | `no_signal` | `unknown`) und **`remote_signals`** (die gefundenen Belegstellen).

> `total_available` ist der größte Einzel-Query-Pool der gefeuerten Suchen. Ist er viel größer als `count`, siehst du einen Ausschnitt — dann `results_per_source` erhöhen oder den Ort eingrenzen.

---

## Das Tool: `search_jobs`

| Parameter | Typ | Default | Beschreibung |
|---|---|---|---|
| `search_term` | str | – | Suchbegriffe, z. B. „python backend developer" |
| `location` | str | `"Germany"` | Stadt/Region, z. B. „Berlin, Germany". `"remote"` für remote-only |
| `site_name` | list | `["indeed","linkedin","google"]` | Börsen: indeed, linkedin, glassdoor, google, zip_recruiter, bayt, naukri, bdjobs |
| `results_wanted` | int | `20` | Ergebnisse **pro Börse** (1–100) |
| `hours_old` | int | `168` | nur Jobs der letzten N Stunden (0 = kein Filter) |
| `job_type` | str? | – | fulltime, parttime, internship, contract |
| `is_remote` | bool | `false` | nur Remote-Stellen |
| `distance` | int | `50` | Umkreis in Meilen |
| `country_indeed` | str | `"germany"` | Land für Indeed & Glassdoor |
| `google_search_term` | str? | – | eigener Google-Query (überschreibt den automatischen) |
| `linkedin_fetch_description` | bool | `false` | volle LinkedIn-Beschreibungen holen (deutlich langsamer) |
| `include_description` | bool | `true` | (gekürzte) Beschreibungen mitliefern |
| `offset` | int | `0` | erste N Treffer überspringen (Pagination) |
| `proxies` | list? | – | Proxys für diesen Aufruf (überschreibt `JOBSPY_PROXIES`) |

Rückgabe: JSON `{ count, sites_searched, jobs: [...] }`. Jeder Job hat Titel, Firma, Ort, Gehaltsspanne, Remote-Flag, Bewerbungs-URL (`job_url` / `job_url_direct`) und eine gekürzte Beschreibung.

**Tempo-Tipp:** LinkedIn rate-limitet ab ~10 Seiten pro IP. Für schnelle, zuverlässige Läufe: `results_wanted` moderat halten und `linkedin_fetch_description=false`. Cowork bricht Tool-Aufrufe nach 5 Min ab.

---

## Bonus: als lokaler Connector in Claude Desktop / Claude Code

Kostenlos, deine Heim-IP, beste Trefferquote (aber **nicht** in Cowork nutzbar):

**Claude Code:**
```bash
claude mcp add jobspy -- uv --directory C:\Users\Momo\jobspy-cowork-mcp run python server.py
```
(mit `MCP_TRANSPORT=stdio`)

**Claude Desktop** (`claude_desktop_config.json`):
```json
{
  "mcpServers": {
    "jobspy": {
      "command": "uv",
      "args": ["--directory", "C:\\Users\\Momo\\jobspy-cowork-mcp", "run", "python", "server.py"],
      "env": { "MCP_TRANSPORT": "stdio" }
    }
  }
}
```

---

## Troubleshooting

| Symptom | Ursache / Fix |
|---|---|
| `count: 0` in der Cloud | IP geblockt → `JOBSPY_PROXIES` setzen, oder nur Indeed/Google nutzen |
| Erste Cowork-Anfrage timeouten | Render Free-Instanz schlief → Starter-Plan |
| Tool-Aufruf bricht ab (>5 Min) | `results_wanted` senken, `linkedin_fetch_description=false` |
| LinkedIn liefert wenig | rate-limitet — Proxys nutzen, kleiner batchen |
| Connector verbindet nicht | URL inkl. Geheim-Pfad korrekt? `/health` erreichbar? HTTPS? |

---

## Konfiguration (Env-Vars)

Alle in `.env.example` dokumentiert. Wichtigste: `MCP_TRANSPORT` (`stdio`|`http`), `MCP_HTTP_PATH` (Geheim-Pfad), `JOBSPY_PROXIES`, optional `MCP_AUTH_TOKEN` (Bearer-Token, für Clients die Auth verlangen).

**Härtung / öffentlicher Betrieb:**
- `RATE_LIMIT_PER_MIN` (Default `40`) — Requests pro Minute und IP; darüber HTTP 429. `0` schaltet das Limit ab.
- `JOBSPY_CONCURRENCY` (Default `1`) — gleichzeitige Indeed/LinkedIn-Scrapes. Niedrig halten: unbegrenzte Parallel-Scrapes über *eine* Datacenter-IP führen zu IP-Bans.
- `search_all_jobs` nutzt Indeed/LinkedIn (JobSpy) standardmäßig **nicht** — nur wenn `include_jobspy=true`. Die freien APIs sind der sichere öffentliche Default; die Scraper teilen sich die eine IP.
- Container läuft als Non-Root (uid 10001) mit HEALTHCHECK; ein Host-Watchdog-Cron (im cloud-init) startet den Container neu, falls die App hängt.

## Hinweis

Scraping öffentlicher Jobbörsen kann gegen deren AGB verstoßen und zu IP-Sperren führen; nutze es für deine persönliche Jobsuche verantwortungsvoll und mit moderaten Raten. Aufbauend auf [JobSpy](https://github.com/speedyapply/JobSpy) (MIT).
