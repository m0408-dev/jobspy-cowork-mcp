# JobSpy Cowork MCP Server

Ein **MCP-Server**, der echte Stellenanzeigen über **Indeed, LinkedIn, Glassdoor, Google Jobs, ZipRecruiter, Bayt, Naukri und BDJobs** in einem einzigen Aufruf durchsucht — gebaut, um ihn als **Custom Connector in Claude Cowork** (oder Claude.ai / Desktop / Code) einzubinden.

Er stellt genau ein Tool bereit: **`search_jobs`**. Angetrieben von der [JobSpy](https://github.com/speedyapply/JobSpy)-Bibliothek.

> **Wie es gebaut ist:** reines Python mit [FastMCP](https://gofastmcp.com). JobSpy wird direkt im Prozess aufgerufen — **keine Shell, kein Docker-Subprocess**. Dadurch existiert die Command-Injection-Lücke typischer Wrapper hier gar nicht. Ein Codebase, zwei Betriebsarten: `stdio` (lokal) und `http` (öffentlich, für Cowork).

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

## Hinweis

Scraping öffentlicher Jobbörsen kann gegen deren AGB verstoßen und zu IP-Sperren führen; nutze es für deine persönliche Jobsuche verantwortungsvoll und mit moderaten Raten. Aufbauend auf [JobSpy](https://github.com/speedyapply/JobSpy) (MIT).
