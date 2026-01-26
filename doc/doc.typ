// ============================================================
// CITY MOOD MAP - Professionelle Projektdokumentation
// HAW Hamburg | Datenmanagement und Algorithmen für Big Data
// ============================================================

#set page(
  paper: "a4",
  margin: (top: 25mm, bottom: 25mm, left: 22mm, right: 22mm),
  header: context {
    if counter(page).get().first() > 1 [
      #set text(size: 9pt, fill: rgb("#4a5568"))
      City Mood Map – Projektdokumentation
      #h(1fr)
      HAW Hamburg | DAD
      #line(length: 100%, stroke: 0.5pt + rgb("#e2e8f0"))
    ]
  },
  footer: context {
    set text(size: 9pt, fill: rgb("#4a5568"))
    line(length: 100%, stroke: 0.5pt + rgb("#e2e8f0"))
    v(-8pt)
    h(1fr)
    [Seite #counter(page).display() von #counter(page).final().first()]
    h(1fr)
  }
)

// Typografie
#set text(font: "Georgia", size: 11pt, lang: "de", hyphenate: true)
#set par(justify: true, leading: 0.65em, first-line-indent: 0em)
#set heading(numbering: "1.1")

// Überschriften Styling
#show heading.where(level: 1): it => {
  pagebreak(weak: true)
  set text(size: 18pt, weight: "bold", fill: rgb("#1a365d"))
  v(1.5em)
  it
  v(1em)
  line(length: 100%, stroke: 2pt + rgb("#3182ce"))
  v(1em)
}

#show heading.where(level: 2): it => {
  set text(size: 14pt, weight: "bold", fill: rgb("#2c5282"))
  v(1em)
  it
  v(0.5em)
}

#show heading.where(level: 3): it => {
  set text(size: 12pt, weight: "bold", fill: rgb("#2d3748"))
  v(0.8em)
  it
  v(0.3em)
}

// Links
#show link: set text(fill: rgb("#2b6cb0"))
#show link: underline

// Code Blocks
#show raw.where(block: true): it => {
  set text(size: 9.5pt, font: "Consolas")
  block(
    fill: rgb("#f7fafc"),
    stroke: 1pt + rgb("#cbd5e0"),
    radius: 4pt,
    inset: 12pt,
    width: 100%,
    it
  )
}

// Inline Code
#show raw.where(block: false): box.with(
  fill: rgb("#edf2f7"),
  inset: (x: 4pt, y: 2pt),
  outset: (y: 2pt),
  radius: 2pt,
)

// Tabellen
#set table(
  stroke: (x, y) => if y == 0 {
    (bottom: 1pt + rgb("#cbd5e0"))
  } else {
    (bottom: 0.5pt + rgb("#e2e8f0"))
  },
  fill: (x, y) => if y == 0 {
    rgb("#edf2f7")
  } else if calc.rem(y, 2) == 0 {
    rgb("#f7fafc")
  }
)

// ============================================================
// TITELSEITE
// ============================================================

#align(center)[
  #v(3cm)
  
  #block(
    fill: gradient.linear(rgb("#1a365d"), rgb("#2c5282"), angle: 90deg),
    radius: 10pt,
    inset: 25pt,
    width: 100%,
  )[
    #set text(fill: white)
    #text(size: 32pt, weight: "bold")[City Mood Map]
    #v(0.5em)
    #text(size: 16pt)[Quantifizierung der Stadtstimmung durch]
    #v(0.2em)
    #text(size: 16pt)[Event-Driven Big-Data-Analysen]
  ]
  
  #v(2cm)
  
  #text(size: 13pt, weight: "semibold", fill: rgb("#2d3748"))[
    Projektdokumentation
  ]
  
  #v(0.5cm)
  
  #text(size: 11pt, fill: rgb("#4a5568"))[
    Datenmanagement und Algorithmen für Big Data (DAD)
  ]
  
  #v(1cm)
  
  #line(length: 50%, stroke: 1pt + rgb("#cbd5e0"))
  
  #v(1.5cm)
  
  #grid(
    columns: (1fr, 1fr),
    gutter: 30pt,
    [
      #set align(left)
      #text(size: 11pt)[
        *Autoren:* \
        Dustin Wickert \
        Arash Sedighi
        
        #v(0.5cm)
        
        *Institution:* \
        Hochschule für Angewandte \
        Wissenschaften Hamburg (HAW)
      ]
    ],
    [
      #set align(right)
      #text(size: 11pt)[
        *Datum:* \
        #datetime.today().display("[day].[month].[year]")
        
        #v(0.5cm)
        
        *Version:* \
        1.0
        
        #v(0.5cm)
        
        *Fakultät:* \
        Technik und Informatik
      ]
    ]
  )
  
  #v(2.5cm)
  
  #block(
    fill: rgb("#ebf8ff"),
    stroke: 1.5pt + rgb("#4299e1"),
    radius: 8pt,
    inset: 18pt,
    width: 95%,
  )[
    #set text(size: 10.5pt)
    #set align(left)
    #set par(justify: true, leading: 0.7em)
    *Abstract:* Dieses Projekt entwickelt ein analytisches Big-Data-System zur Berechnung eines täglichen „Mood Scores" für die Stadt Hamburg. Durch Integration heterogener Datenquellen – Wetterdaten, Luftqualität, Verkehrsaufkommen, öffentliche Warnmeldungen und städtische Ereignisse – entsteht ein quantifizierbarer Indikator für die Stadtstimmung. Die Implementierung erfolgt als Event-Driven Architecture mit Apache Kafka als zentralem Event-Bus, Apache Spark Structured Streaming für die Echtzeit-Verarbeitung und PostgreSQL als persistente Datenschicht. Die Visualisierung erfolgt über Grafana-Dashboards. Das Projekt demonstriert moderne Big-Data-Technologien und deren Anwendung auf Smart-City-Analytics.
  ]
]

#pagebreak()

// ============================================================
// INHALTSVERZEICHNIS
// ============================================================

#outline(
  title: [Inhaltsverzeichnis],
  indent: 2em,
  depth: 3
)

#pagebreak()

// ============================================================
// 1. EXECUTIVE SUMMARY
// ============================================================

= Executive Summary

== Projektvision

Das Projekt *City Mood Map* adressiert die Frage: *Wie fühlt sich eine Stadt heute an?* Durch die Aggregation und Analyse multipler Datenströme aus urbanen Sensoren, öffentlichen APIs und Open-Data-Portalen wird ein täglicher Stimmungsindikator (Mood Score) berechnet, der auf einer Skala von -1 (negativ) bis +1 (positiv) die aktuelle „Stimmung" der Stadt Hamburg quantifiziert.

== Kernergebnisse

#block(
  fill: rgb("#f0fff4"),
  stroke: 1pt + rgb("#9ae6b4"),
  radius: 6pt,
  inset: 15pt,
)[
  #set par(leading: 0.8em)
  *Implementierte Komponenten:*
  - 9 autonome Daten-Fetcher-Services (Weather, Air Pollution, Traffic, Transparenz, NINA Alerts, Water Level, BBC News, NYT News, Street Construction)
  - Event-Driven Scheduler zur Orchestrierung
  - Apache Kafka als zentraler Event-Bus (20 Topics)
  - Redis für Caching, Deduplizierung und Rate Limiting
  - Spark Structured Streaming Pipeline mit Great Expectations Datenqualitätsprüfung
  - PostgreSQL-Datenbankschicht mit UPSERT-Strategie und History-Tracking
  - Sentiment-Analyse für Nachrichten mittels Flair NLP
  - Grafana-basierte Visualisierungsschicht mit Echtzeit-Dashboards
]

#v(1em)

#table(
  columns: (auto, 1fr),
  align: (left, left),
  inset: 10pt,
  [*Metrik*], [*Wert*],
  [Verarbeitete Datenquellen], [9 heterogene APIs/Feeds],
  [Kafka Topics], [20 (9 Trigger + 11 Data)],
  [Streaming-Latenz], [< 20 Sekunden (Micro-Batch)],
  [Window-Duration], [60 Minuten],
  [Watermark-Delay], [5 Minuten],
  [Persistierung], [PostgreSQL mit Upsert + History-Tracking],
  [Caching/Deduplizierung], [Redis 8.4],
  [Datenqualität], [Great Expectations mit HTML-Reports],
  [Skalierung], [Horizontal (Spark Workers, 10G Memory)],
)

== Technologie-Stack

#grid(
  columns: (1fr, 1fr),
  gutter: 15pt,
  [
    *Backend & Processing:*
    - Apache Kafka 4.1.1 (KRaft)
    - Apache Spark 3.5.1 (Streaming)
    - PostgreSQL 16
    - Redis 8.4.0 (Caching)
    - Python 3.8+ (Fetcher Services)
    - Flair NLP (Sentiment-Analyse)
  ],
  [
    *Infrastructure:*
    - Docker Compose (Orchestrierung)
    - Grafana (Visualisierung)
    - Kafka UI (Monitoring)
    - PySpark (Stream Processing)
    - Great Expectations (Datenqualität)
  ]
)

== Anwendungsfälle

*Primäre Use Cases:*
1. *Stadtplanung:* Analyse zeitlicher Muster (Wochentage, Jahreszeiten, Ferienzeiten)
2. *Öffentliche Verwaltung:* Frühwarnung bei negativen Trends
3. *Forschung:* Korrelationsanalysen zwischen Umweltfaktoren und Stadtstimmung
4. *Bürgerinformation:* Transparentes Dashboard zur aktuellen Stadtlage

#pagebreak()

// ============================================================
// 2. EINLEITUNG
// ============================================================

= Einleitung

== Motivation und wissenschaftlicher Kontext

=== Smart Cities und Urban Analytics

Die zunehmende Urbanisierung – bis 2050 werden ca. 68% der Weltbevölkerung in Städten leben (UN World Urbanization Prospects, 2018) – stellt Städte vor neue Herausforderungen hinsichtlich Lebensqualität, Ressourcenmanagement und Bürgerzufriedenheit. Das Konzept der *Smart City* nutzt digitale Technologien und Datenanalysen, um städtische Systeme effizienter und lebenswerter zu gestalten.

Ein zentraler Aspekt ist die Messung und Quantifizierung von *Lebensqualität* und *Stadtbefinden*. Traditionelle Ansätze basieren auf Umfragen oder statischen Indikatoren. Diese sind jedoch zeitaufwändig, kostenintensiv und bieten keine Echtzeit-Perspektive.

=== Datengetriebene Stadtanalyse

Mit der Verfügbarkeit von Open Data, IoT-Sensoren und öffentlichen APIs eröffnen sich neue Möglichkeiten für kontinuierliche, objektive Messungen. Das vorliegende Projekt nutzt Big-Data-Technologien, um aus heterogenen Datenströmen einen aggregierten Indikator zu berechnen.

== Problemstellung

*Zentrale Forschungsfrage:*

Kann durch die Aggregation multipler urbaner Datenquellen (Wetter, Luftqualität, Verkehr, Ereignisse) ein aussagekräftiger, tagesaktueller Indikator für die „Stimmung" einer Stadt berechnet werden?

*Teilfragestellungen:*
1. Welche Datenquellen sind relevant und öffentlich verfügbar?
2. Wie können heterogene Datenströme in Echtzeit integriert werden?
3. Welche Big-Data-Architektur eignet sich für Event-Driven Analytics?
4. Wie wird Datenqualität in einer Streaming-Pipeline sichergestellt?
5. Wie kann ein Mood Score aus Rohdaten abgeleitet werden?

== Projektziele

#block(
  fill: rgb("#fffaf0"),
  stroke: 1pt + rgb("#fbd38d"),
  radius: 6pt,
  inset: 15pt,
)[
  *Primäre Ziele:*
  
  1. *Architektur:* Entwurf und Implementierung einer skalierbaren Event-Driven Big-Data-Pipeline
  
  2. *Integration:* Anbindung multipler heterogener Datenquellen über einheitliche Kafka-Topics
  
  3. *Processing:* Echtzeit-Aggregation mittels Spark Structured Streaming
  
  4. *Qualität:* Integration von Datenqualitätsprüfungen mit automatischer Reportgenerierung
  
  5. *Visualisierung:* Grafana-basierte Dashboards für zeitliche Analysen
  
  6. *Skalierbarkeit:* Horizontale Skalierung durch Microservices und Spark-Cluster
]

== Projektumfang und Abgrenzung

*Im Scope:*
- Stadt Hamburg als Beispiel-Use-Case
- 9 Datenquellen (Wetter, Luftqualität, Verkehr, Transparenz, NINA, Wasserpegel, BBC News, NYT News, Baustellen)
- Stundenaggregation mit 60-Minuten-Fenster
- Micro-Batch-Verarbeitung im 20-Sekunden-Intervall
- Historische Speicherung mit vollständigem Audit-Trail
- Sentiment-Analyse für Nachrichten-Feeds
- Datenqualitätsprüfung mit automatisierten Reports

*Nicht im Scope:*
- Predictive Analytics / Machine Learning (zukünftige Erweiterung)
- Mobile App oder Web-Frontend
- Weitere Städte (konzeptionell erweiterbar)
- Social-Media-Sentiment-Analyse (Twitter, Reddit)
- Real-Time Alerting

== Related Work

Ähnliche Ansätze zur Messung städtischer Stimmung existieren in verschiedenen Kontexten:

*Sentiment Analysis aus Social Media:*
- Twitter-basierte Mood-Indizes (Bollen et al., 2011)
- Limitierung: Bias durch Nutzerdemografie, fehlende Objektivität

*IoT-Sensor-Netzwerke:*
- Smart City Projekte (Barcelona, Singapur)
- Fokus auf einzelne Dimensionen (Verkehr, Umwelt)

*Citizen Science Plattformen:*
- Manuelle Datenerfassung durch Bürger
- Limitierung: Geringe Datendichte, Subjektivität

*Differenzierung dieses Projekts:*
- Kombination multipler objektiver Datenquellen
- Vollautomatisierte Event-Driven Pipeline
- Open-Source-Technologien
- Reproduzierbar und erweiterbar

== Dokumentationsstruktur

Diese Dokumentation ist wie folgt strukturiert:

- *Kapitel 3:* Systemarchitektur (Event-Driven Design, Komponenten)
- *Kapitel 4:* Datenquellen und Fetcher-Services (9 Fetcher + Scheduler)
- *Kapitel 5:* Stream Processing mit Apache Spark
- *Kapitel 6:* Datenmodell in PostgreSQL
- *Kapitel 7:* Datenqualität mit Great Expectations
- *Kapitel 8:* Mood Score Algorithmus (implementiert)
- *Kapitel 9:* Visualisierung mit Grafana
- *Kapitel 10:* Technologie-Stack (inkl. Redis, Flair)
- *Kapitel 11-15:* Performance, Sicherheit, Deployment, Monitoring, Troubleshooting
- *Kapitel 16:* Ergebnisse und Erkenntnisse
- *Anhänge:* Projektstruktur, Glossar, Referenzen

#pagebreak()

// ============================================================
// 3. SYSTEMARCHITEKTUR
// ============================================================

= Systemarchitektur

== Architektur-Überblick

Das City Mood Map System folgt einer *Event-Driven Architecture* (EDA) mit Elementen der *Lambda Architecture*. Die Kernidee: Alle Komponenten kommunizieren asynchron über Events, die in Apache Kafka als zentralem Event-Bus persistiert werden.

#figure(
  image("architecture.png", width: 100%),
  caption: [Systemarchitektur: Event-Driven Pipeline mit Kafka als zentralem Event-Bus]
)

=== Architektur-Muster

*Event-Driven Architecture (EDA):*
- Lose Kopplung zwischen Produzenten und Konsumenten
- Asynchrone Kommunikation über Events
- Skalierbarkeit durch unabhängige Services
- Fehlertoleranz durch Event-Replay

*Lambda Architecture Elemente:*
- *Speed Layer:* Spark Structured Streaming (Echtzeit-Aggregation)
- *Batch Layer:* Tägliche Aggregationen in PostgreSQL
- *Serving Layer:* Grafana Dashboards

== Komponenten-Übersicht

Das System besteht aus folgenden Hauptkomponenten:

#table(
  columns: (auto, 2fr, 1.5fr, auto),
  align: (left, left, left, center),
  inset: 10pt,

  [*Komponente*], [*Funktion*], [*Technologie*], [*Instanzen*],

  [Scheduler], [Sendet stündliche Trigger-Events], [Python + Kafka], [1],
  [Weather Fetcher], [Holt Wetterdaten], [Python + Kafka + Redis], [1],
  [Air Pollution], [Holt Luftqualitätsdaten], [Python + Kafka + Redis], [1],
  [Traffic Fetcher], [Verarbeitet GeoJSON-Daten], [Python + Kafka + Redis], [1],
  [Transparenz], [Durchsucht Open Data], [Python + Kafka + Redis], [1],
  [NINA Alerts], [Holt Warnmeldungen], [Python + Kafka + Redis], [1],
  [Water Level], [Holt Pegelstände], [Python + Kafka + Redis], [1],
  [BBC News], [Parst RSS-Feed + Sentiment], [Python + Kafka + Flair], [1],
  [NYT News], [Parst RSS-Feeds + Sentiment], [Python + Kafka + Flair], [1],
  [Street Construction], [Holt Baustellendaten], [Python + Kafka + Redis], [1],
  [Kafka Broker], [Zentraler Event-Bus], [Kafka 4.1.1 (KRaft)], [1],
  [Redis], [Caching + Deduplizierung], [Redis 8.4.0], [1],
  [Spark Master], [Cluster-Koordination], [Spark 3.5.1], [1],
  [Spark Worker], [Transformationen (10G, 6 Cores)], [Spark 3.5.1], [1-n],
  [PySpark Client], [Streaming-Job (6G Driver)], [PySpark], [1],
  [PostgreSQL], [Persistenz + History], [PostgreSQL 16], [1],
  [Grafana], [Visualisierung], [Grafana Latest], [1],
  [Kafka UI], [Monitoring], [provectuslabs/kafka-ui], [1],
)

== Datenfluss End-to-End

#block(
  fill: rgb("#f0fff4"),
  stroke: 1pt + rgb("#9ae6b4"),
  radius: 6pt,
  inset: 15pt,
)[
  *Schritt-für-Schritt Ablauf:*
  
  1. *Scheduler* sendet stündlich Trigger-Events
  2. *Fetcher* reagieren auf Trigger, rufen APIs auf
  3. *Kafka* empfängt und persistiert Events
  4. *Spark Streaming* liest, transformiert, aggregiert
  5. *PostgreSQL* speichert via UPSERT
  6. *Grafana* visualisiert Daten
]

== Event-Driven Design

Der Scheduler orchestriert alle Fetcher über Events:

```python
FETCH_TOPICS = [
    "fetch-weather",
    "fetch-air-pollution",
    "fetch-traffic",
    "fetch-news"
]

while True:
    for topic in FETCH_TOPICS:
        event = {"type": "FETCH_TRIGGER", "timestamp": datetime.now().isoformat()}
        producer.send(topic, event)
    producer.flush()
    time.sleep(3600)
```

*Vorteile:*
- Entkopplung zwischen Services
- Neue Datenquellen durch neues Topic
- Fehler in einem Service beeinträchtigen andere nicht

#pagebreak()

// ============================================================
// 4. DATENQUELLEN & FETCHER-SERVICES
// ============================================================

= Datenquellen und Fetcher-Services

Dieses Kapitel dokumentiert alle 9 Datenquellen und den Scheduler-Service. Alle Fetcher nutzen eine gemeinsame `BaseFetcher`-Klasse und Redis für Deduplizierung sowie Rate Limiting.

== Scheduler-Service

Der Scheduler ist die zentrale Orchestrierungskomponente.

*Funktion:* Sendet stündlich Trigger-Events an alle Fetcher-Topics (zur vollen Stunde)

*Kafka Topics (Producer):*
- `fetch-weather`
- `fetch-air-pollution`
- `fetch-traffic`
- `fetch-bbc-rss`
- `fetch-nyt-rss`
- `fetch-public-alerts`
- `fetch-street-construction`
- `fetch-transparenz`
- `fetch-water-levels`

*Konfiguration (Environment Variables):*
- `SCHEDULER_IMMEDIATE_TRIGGER`: Sofortiger Trigger beim Start (default: true)
- `SCHEDULER_TRIGGER_ON_START`: Trigger beim Container-Start (default: false)

*Retention Policy:* 24 Stunden

*Implementierung:*
```python
# Erstellt Topics mit 24h Retention
admin_client.create_topics([
    NewTopic(name=topic, num_partitions=1, replication_factor=1,
             topic_configs={"retention.ms": "86400000"})
])
```

== Weather Fetcher (Open-Meteo API)

*API:* `https://api.open-meteo.com/v1/forecast`

*Standort:* Hamburg (53.5507°N, 9.993°E)

*Datenfelder:*
- *Current:* temperature_2m, precipitation, wind_speed_10m, cloud_cover
- *Hourly:* temperature_2m, rain, snowfall, visibility
- *Daily:* sunrise, sunset, uv_index_max, temperature_2m_max/min

*Kafka Topics (Producer):*
- `hh-weather-current`
- `hh-weather-daily`

*Event-Struktur:*
```json
{
  "fetch_timestamp": "2026-01-04T12:00:00",
  "source": "open-meteo",
  "type": "current_weather",
  "current": {
    "temperature_2m": 8.5,
    "precipitation": 0.2,
    "wind_speed_10m": 15.3
  }
}
```

== Air Pollution Fetcher (Open-Meteo Air Quality)

*API:* `https://air-quality-api.open-meteo.com/v1/air-quality`

*Datenfelder:*
- *Feinstaub:* PM2.5, PM10
- *Gase:* CO, CO₂, SO₂, Ozon, Methan, Ammoniak
- *Pollen:* Birke, Gras, Beifuß, Ragweed
- *Index:* European AQI, Aerosol Optical Depth

*Kafka Topic:* `hh-air-pollution-current`

== Traffic Fetcher (Lokale GeoJSON-Daten)

*Datenquelle:* Lokale ZIP-Archive mit GeoJSON-Features

*Verarbeitung:*
1. ZIP-Dateien aus `./data/traffic_hh/` lesen
2. GeoJSON-Features extrahieren
3. Jedes Feature einzeln an Kafka senden

*Kafka Topic:* `hh-traffic-data`

*Event-Struktur:*
```json
{
  "fetch_timestamp": "2026-01-04T12:00:00",
  "source": "traffic_hh_zip",
  "feature_index": 42,
  "feature": { /* GeoJSON Feature */ }
}
```

== Transparenz Fetcher (Hamburg Open Data)

*API:* `http://suche.transparenz.hamburg.de/api/3/action/package_search`

*Keyword-Suche:*
```python
KEYWORDS = ["unfall", "stoerung", "sperrung", 
            "feuerwehr", "polizei", "baustelle"]
```

*Kafka Topic:* `hh-transparenz-events`

*Event-Struktur:*
```json
{
  "source": "transparenz_portal",
  "fetch_timestamp": "2026-01-04T12:00:00",
  "event_id": "abc-123",
  "title": "Verkehrsstörung",
  "category": ["sperrung"],
  "published_at": "2026-01-04T10:00:00"
}
```

== NINA Alert Fetcher (BBK Warnmeldungen)

*API:* `https://nina.api.proxy.bund.dev/api31/dashboard/020000000000.json`

*Datenfelder:*
- *Severity:* Minor, Moderate, Severe, Extreme
- *Urgency:* Immediate, Expected, Future
- *Provider:* BBK, DWD, Polizei
- *Headline, Description, Valid*

*Kafka Topic:* `hh-public-alerts-current`

*Event-Struktur:*
```json
{
  "fetch_timestamp": "2026-01-04T12:00:00",
  "source": "nina",
  "type": "public_alert",
  "alert": {
    "id": "alert-123",
    "severity": "Moderate",
    "urgency": "Immediate",
    "headline": "Unwetterwarnung"
  }
}
```

== Water Level Fetcher (PegelOnline API)

*API:* `https://www.pegelonline.wsv.de/webservices/rest-api/v2/stations.json`

*Parameter:*
- Gewässer: Elbe
- Radius: 10km um Hamburg
- Messreihe: Wasserstand (W)

*Kafka Topic:* `hh-water-level-current`

*Event-Struktur:*
```json
{
  "fetch_timestamp": "2026-01-04T12:00:00",
  "source": "pegelonline",
  "type": "current_water_level",
  "station": {
    "name": "Hamburg St. Pauli",
    "water": "ELBE",
    "value": 325.5,
    "unit": "cm",
    "timestamp": "2026-01-04T11:45:00"
  }
}
```

== BBC News Fetcher (RSS Feed mit Sentiment-Analyse)

*Datenquelle:* BBC Europe RSS Feed (`http://feeds.bbci.co.uk/news/world/europe/rss.xml`)

*Verarbeitung:*
1. RSS-Feed parsen
2. Sentiment-Analyse der Headlines mittels Flair NLP
3. Deduplizierung via Redis (Article-ID Hash)

*Kafka Topic:* `bbc-europe-news`

*Event-Struktur:*
```json
{
  "fetch_timestamp": "2026-01-04T12:00:00",
  "source": "bbc",
  "type": "news_article",
  "article": {
    "title": "EU announces new climate policy",
    "link": "https://bbc.com/...",
    "published": "2026-01-04T10:00:00",
    "sentiment": "POSITIVE",
    "sentiment_score": 0.85
  }
}
```

== NYT News Fetcher (RSS Feeds mit Sentiment-Analyse)

*Datenquellen:*
- NYT Europe: `https://rss.nytimes.com/services/xml/rss/nyt/Europe.xml`
- NYT World: `https://rss.nytimes.com/services/xml/rss/nyt/World.xml`

*Verarbeitung:*
1. Beide RSS-Feeds parallel parsen
2. Sentiment-Analyse der Headlines mittels Flair NLP
3. Deduplizierung via Redis (Article-ID Hash)

*Kafka Topics:*
- `nyt-europe-news`
- `nyt-world-news`

*Event-Struktur:*
```json
{
  "fetch_timestamp": "2026-01-04T12:00:00",
  "source": "nyt",
  "type": "news_article",
  "feed": "europe",
  "article": {
    "title": "Economic growth in Germany",
    "link": "https://nytimes.com/...",
    "published": "2026-01-04T09:30:00",
    "sentiment": "NEUTRAL",
    "sentiment_score": 0.52
  }
}
```

== Street Construction Fetcher (Hamburg Baustellen API)

*API:* Hamburg GeoServices Baustellen-API

*Datenfelder:*
- Standort (Koordinaten)
- Straßenname
- Störungsart (Vollsperrung, Teilsperrung, Einengung)
- Zeitraum (Start, Ende)
- Beschreibung

*Kafka Topic:* `hh-street-construction`

*Event-Struktur:*
```json
{
  "fetch_timestamp": "2026-01-04T12:00:00",
  "source": "hamburg_baustellen",
  "type": "construction_site",
  "construction": {
    "id": "const-456",
    "street": "Mönckebergstraße",
    "disruption_type": "Teilsperrung",
    "start_date": "2026-01-01",
    "end_date": "2026-02-15",
    "coordinates": [9.995, 53.551]
  }
}
```

== Kafka Topics Übersicht

=== Trigger Topics (9)

#table(
  columns: (auto, 1fr),
  align: (left, left),
  inset: 10pt,

  [*Topic-Name*], [*Beschreibung*],

  [fetch-weather], [Trigger für Weather Fetcher],
  [fetch-air-pollution], [Trigger für Air Pollution Fetcher],
  [fetch-traffic], [Trigger für Traffic Fetcher],
  [fetch-bbc-rss], [Trigger für BBC News Fetcher],
  [fetch-nyt-rss], [Trigger für NYT News Fetcher],
  [fetch-public-alerts], [Trigger für NINA Alert Fetcher],
  [fetch-street-construction], [Trigger für Street Construction Fetcher],
  [fetch-transparenz], [Trigger für Transparenz Fetcher],
  [fetch-water-levels], [Trigger für Water Level Fetcher],
)

=== Data Topics (11)

#table(
  columns: (auto, auto, 1fr),
  align: (left, left, left),
  inset: 10pt,

  [*Topic-Name*], [*Quelle*], [*Beschreibung*],

  [hh-weather-current], [Open-Meteo], [Aktuelle Wetterdaten],
  [hh-weather-daily], [Open-Meteo], [Tägliche Wetterprognose],
  [hh-air-pollution-current], [Open-Meteo AQI], [Aktuelle Luftqualitätsdaten],
  [hh-air-pollution-daily], [Open-Meteo AQI], [Tägliche Luftqualitätsprognose],
  [hh-traffic-data], [Hamburg GeoServices], [Verkehrsdaten (GeoJSON)],
  [hh-transparenz-events], [Transparenzportal], [Hamburg Open Data Events],
  [hh-public-alerts-current], [NINA/BBK], [Öffentliche Warnmeldungen],
  [hh-water-level-current], [PegelOnline], [Elbe Pegelstände],
  [hh-street-construction], [Hamburg API], [Baustellendaten],
  [bbc-europe-news], [BBC RSS], [BBC Europe News mit Sentiment],
  [nyt-europe-news], [NYT RSS], [NYT Europe News mit Sentiment],
  [nyt-world-news], [NYT RSS], [NYT World News mit Sentiment],
)

#pagebreak()

// ============================================================
// 5. STREAM PROCESSING (SPARK)
// ============================================================

= Stream Processing mit Apache Spark

== Spark Structured Streaming

*Konzept:* Micro-Batch Processing (alle 20 Sekunden)

*Konfiguration:*
```python
spark = SparkSession.builder \
    .appName("CityMoodPipeline") \
    .master("spark://spark-master:7077") \
    .config("spark.jars.packages",
            "org.postgresql:postgresql:42.7.3," +
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .config("spark.driver.memory", "6g") \
    .config("spark.executor.memory", "10g") \
    .config("spark.executor.cores", "6") \
    .getOrCreate()
```

*Streaming-Parameter:*
- Trigger-Intervall: 20 Sekunden
- Watermark-Delay: 5 Minuten
- Window-Duration: 60 Minuten

== Input Topics

Die Pipeline liest von 9 Data Topics:

```python
INPUT_TOPICS = [
    "bbc-europe-news",
    "nyt-europe-news",
    "nyt-world-news",
    "hh-air-pollution-current",
    "hh-weather-current",
    "hh-traffic-data",
    "hh-public-alerts-current",
    "hh-street-construction",
    "hh-water-level-current"
]
```

*Hinweis:* `hh-transparenz-events` wird produziert aber nicht in der Pipeline konsumiert.

== Schema-Normalisierung

Jede Datenquelle hat ein spezifisches Schema, das auf ein gemeinsames Format normalisiert wird:

*News Schema (BBC/NYT):*
```python
news_schema = StructType([
    StructField("fetch_timestamp", StringType(), True),
    StructField("source", StringType(), True),
    StructField("title", StringType(), True),
    StructField("sentiment", StringType(), True),
    StructField("sentiment_score", DoubleType(), True)
])
```

*Air Quality Schema:*
```python
air_schema = StructType([
    StructField("fetch_timestamp", StringType(), True),
    StructField("pm2_5", DoubleType(), True),
    StructField("pm10", DoubleType(), True),
    StructField("nitrogen_dioxide", DoubleType(), True),
    StructField("ozone", DoubleType(), True),
    StructField("european_aqi", IntegerType(), True)
])
```

*Weather Schema:*
```python
weather_schema = StructType([
    StructField("fetch_timestamp", StringType(), True),
    StructField("temperature_2m", DoubleType(), True),
    StructField("weather_code", IntegerType(), True),
    StructField("precipitation", DoubleType(), True),
    StructField("wind_speed_10m", DoubleType(), True)
])
```

== Windowed Aggregation

Die Pipeline verwendet zeitfensterbasierte Aggregation:

```python
windowed_df = parsed_df \
    .withWatermark("event_time", "5 minutes") \
    .groupBy(
        window("event_time", "60 minutes"),
        "source"
    ) \
    .agg(
        count("*").alias("event_count"),
        avg("score").alias("avg_score")
    )
```

*Window-Konfiguration:*
- Window-Größe: 60 Minuten
- Watermark: 5 Minuten (für verspätete Events)
- Slide-Intervall: Tumbling Window (kein Overlap)

== Score-Berechnung pro Quelle

Jede Datenquelle wird in einen Teilscore (0.0 - 1.0) transformiert:

#table(
  columns: (auto, auto, 1fr),
  align: (left, center, left),
  inset: 10pt,

  [*Quelle*], [*Gewicht*], [*Score-Logik*],

  [News (BBC/NYT)], [25%], [Sentiment-Durchschnitt (POSITIVE=1.0, NEUTRAL=0.5, NEGATIVE=0.0)],
  [Air Quality], [15%], [Gewichteter AQI-Index (PM2.5, PM10, NO₂, O₃, SO₂, CO)],
  [Weather], [20%], [Temperatur + Wetter-Code + Niederschlag + Wind (40/30/20/10)],
  [Traffic], [15%], [Verkehrsfluss (fließend=1.0, zähfließend=0.6, stockend=0.3)],
  [Alerts], [15%], [Severity-basiert (Minor=0.9, Moderate=0.7, Severe=0.4)],
  [Construction], [5%], [Anzahl aktiver Baustellen (invers)],
  [Water Level], [5%], [Elbe-Pegel (400-700cm optimal = 1.0)],
)

== UPSERT-Strategie mit History

Um Duplicate Key Errors bei Restarts zu vermeiden und einen Audit-Trail zu gewährleisten:

```python
# Haupt-Tabelle (aktueller Stand)
sql_main = """
INSERT INTO city_mood_scores
  (window_start, city_mood_score, news_score, air_score,
   weather_score, traffic_score, alert_score, construction_score,
   water_score, total_data_points, avg_aqi, avg_temp, avg_water_level)
VALUES %s
ON CONFLICT (window_start)
DO UPDATE SET
    city_mood_score = EXCLUDED.city_mood_score,
    updated_at = CURRENT_TIMESTAMP;
"""

# History-Tabelle (Audit-Trail)
sql_history = """
INSERT INTO city_mood_score_history
  (window_start, city_mood_score, ..., batch_id,
   validation_success, validation_success_percent)
VALUES %s;
"""
```

*Vorteile:*
- Idempotenz für Haupt-Tabelle gewährleistet
- Vollständiger Audit-Trail in History
- Batch-ID für Nachvollziehbarkeit
- Datenqualitäts-Metriken pro Batch

#pagebreak()

// ============================================================
// 6. DATENMODELL (PostgreSQL)
// ============================================================

= Datenmodell in PostgreSQL

== Entity-Relationship Übersicht

#table(
  columns: (auto, auto, 1fr),
  align: (left, left, left),
  inset: 10pt,

  [*Tabelle*], [*Primary Key*], [*Beschreibung*],

  [cities], [id], [Stammdaten der Stadt Hamburg],
  [city_mood_scores], [window_start], [Aktuelle Mood Scores pro Zeitfenster],
  [city_mood_score_history], [id (SERIAL)], [Audit-Trail aller berechneten Scores],
)

== Tabelle: cities

```sql
CREATE TABLE cities (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    country VARCHAR(100),
    longitude DOUBLE PRECISION NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO cities (name, country, longitude, latitude)
VALUES ('Hamburg', 'Germany', 9.993682, 53.551086);
```

== Tabelle: city_mood_scores

Haupttabelle für aktuelle Mood Scores (UPSERT-fähig):

```sql
CREATE TABLE city_mood_scores (
    window_start TIMESTAMP PRIMARY KEY,
    city_mood_score DOUBLE PRECISION,      -- Skala: 0.0 bis 1.0

    -- Komponenten-Scores
    news_score DOUBLE PRECISION,
    air_score DOUBLE PRECISION,
    weather_score DOUBLE PRECISION,
    traffic_score DOUBLE PRECISION,
    alert_score DOUBLE PRECISION,
    construction_score DOUBLE PRECISION,
    water_score DOUBLE PRECISION,

    -- Datenpunkt-Zählungen
    news_count INTEGER,
    air_count INTEGER,
    weather_count INTEGER,
    traffic_count INTEGER,
    alert_count INTEGER,
    construction_count INTEGER,
    water_count INTEGER,
    total_data_points INTEGER,

    -- Aggregierte Metriken
    avg_aqi DOUBLE PRECISION,
    avg_temp DOUBLE PRECISION,
    avg_water_level DOUBLE PRECISION,

    -- Timestamps
    computed_at TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

*Indizes:*
```sql
CREATE INDEX idx_window_start ON city_mood_scores(window_start DESC);
CREATE INDEX idx_city_mood_score ON city_mood_scores(city_mood_score);
CREATE INDEX idx_updated_at ON city_mood_scores(updated_at DESC);
```

*Beispieldaten:*

#table(
  columns: (auto, auto, auto, auto, auto),
  align: (left, center, center, center, center),
  inset: 8pt,

  [*window_start*], [*mood_score*], [*news*], [*weather*], [*traffic*],
  [2026-01-04 12:00], [0.72], [0.65], [0.85], [0.78],
  [2026-01-04 13:00], [0.68], [0.55], [0.80], [0.65],
  [2026-01-04 14:00], [0.75], [0.70], [0.82], [0.80],
)

== Tabelle: city_mood_score_history

Audit-Trail für alle berechneten Scores mit Batch-Tracking und Validierungs-Metriken:

```sql
CREATE TABLE city_mood_score_history (
    id SERIAL PRIMARY KEY,
    window_start TIMESTAMP,
    city_mood_score DOUBLE PRECISION,

    -- (Alle Felder wie in city_mood_scores)
    news_score DOUBLE PRECISION,
    air_score DOUBLE PRECISION,
    -- ...

    -- Batch-Tracking
    batch_id BIGINT,
    written_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Datenqualitäts-Metriken
    validation_success BOOLEAN,
    validation_success_percent DOUBLE PRECISION,
    validation_evaluated_expectations INTEGER,
    validation_successful_expectations INTEGER,
    validation_failed_expectations INTEGER,
    validation_failed_list TEXT,
    validation_warnings_list TEXT
);
```

*Indizes:*
```sql
CREATE INDEX idx_history_window ON city_mood_score_history(window_start DESC);
CREATE INDEX idx_history_written ON city_mood_score_history(written_at DESC);
CREATE INDEX idx_history_batch ON city_mood_score_history(batch_id);
CREATE INDEX idx_history_validation ON city_mood_score_history(validation_success);
```

== View: v_city_mood_latest

Convenience View für die letzten 100 Scores mit Trend-Berechnung:

```sql
CREATE VIEW v_city_mood_latest AS
SELECT
    window_start,
    city_mood_score,
    city_mood_score - LAG(city_mood_score)
        OVER (ORDER BY window_start) AS mood_change,
    news_score, air_score, weather_score,
    traffic_score, alert_score, construction_score, water_score,
    total_data_points,
    updated_at
FROM city_mood_scores
ORDER BY window_start DESC
LIMIT 100;
```

== Function: cleanup_old_history()

Automatische Bereinigung alter History-Einträge:

```sql
CREATE OR REPLACE FUNCTION cleanup_old_history()
RETURNS INTEGER AS $$
DECLARE
    deleted_count INTEGER;
BEGIN
    DELETE FROM city_mood_score_history
    WHERE written_at < NOW() - INTERVAL '90 days';
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;
```

#pagebreak()

// ============================================================
// 7. DATENQUALITÄT
// ============================================================

= Datenqualität

== Great Expectations Framework

Das Projekt verwendet *Great Expectations* als Datenqualitäts-Framework. Jeder Micro-Batch durchläuft umfassende Qualitätsprüfungen vor der Persistierung.

== Implementierte Expectations

#table(
  columns: (auto, 1fr, auto),
  align: (left, left, center),
  inset: 10pt,

  [*Expectation*], [*Regel*], [*Schwellwert*],
  [Score Range], [city_mood_score BETWEEN 0.0 AND 1.0], [Pflicht],
  [Component Scores], [Alle Teilscores BETWEEN 0.0 AND 1.0], [Pflicht],
  [Non-Null Scores], [city_mood_score IS NOT NULL], [Pflicht],
  [Temperature Range], [avg_temp BETWEEN -30 AND 50], [Warnung],
  [AQI Range], [avg_aqi BETWEEN 0 AND 500], [Warnung],
  [Water Level Range], [avg_water_level BETWEEN 0 AND 2000], [Warnung],
  [Data Availability], [total_data_points >= 1], [Pflicht],
  [Window Consistency], [window_start innerhalb erwarteter Range], [Warnung],
)

== Qualitätsprüfung im Code

```python
from great_expectations.core import ExpectationSuite
from great_expectations.dataset import PandasDataset

def validate_batch(batch_df, batch_id):
    pdf = batch_df.toPandas()
    ge_df = PandasDataset(pdf)

    # Score-Validierung
    ge_df.expect_column_values_to_be_between(
        "city_mood_score", min_value=0.0, max_value=1.0
    )

    # Komponenten-Score-Validierung
    for col in ["news_score", "air_score", "weather_score",
                "traffic_score", "alert_score", "construction_score",
                "water_score"]:
        ge_df.expect_column_values_to_be_between(
            col, min_value=0.0, max_value=1.0
        )

    # Plausibilitätschecks
    ge_df.expect_column_values_to_be_between(
        "avg_temp", min_value=-30, max_value=50
    )
    ge_df.expect_column_values_to_be_between(
        "avg_aqi", min_value=0, max_value=500
    )

    # Daten-Verfügbarkeit
    ge_df.expect_column_values_to_be_greater_than(
        "total_data_points", 0
    )

    results = ge_df.validate()
    return results
```

== Validierungs-Metriken

Jeder Batch speichert folgende Validierungs-Metriken in der History-Tabelle:

```python
validation_record = {
    "validation_success": results.success,
    "validation_success_percent": results.statistics["success_percent"],
    "validation_evaluated_expectations": results.statistics["evaluated_expectations"],
    "validation_successful_expectations": results.statistics["successful_expectations"],
    "validation_failed_expectations": results.statistics["unsuccessful_expectations"],
    "validation_failed_list": json.dumps([
        exp.expectation_config.expectation_type
        for exp in results.results if not exp.success
    ]),
    "validation_warnings_list": json.dumps(warnings)
}
```

== Report-Generierung

Für jeden Batch werden zwei Reports erzeugt:

*JSON Report (maschinenlesbar):*
```json
{
  "batch_id": 42,
  "generated_at_utc": "2026-01-04T12:00:00",
  "rows_in_batch": 24,
  "validation": {
    "success": true,
    "success_percent": 100.0,
    "evaluated_expectations": 12,
    "successful_expectations": 12,
    "failed_expectations": 0
  },
  "statistics": {
    "avg_mood_score": 0.72,
    "min_mood_score": 0.65,
    "max_mood_score": 0.85,
    "total_data_points": 1847
  }
}
```

*HTML Report (visuell):*
- Batch-ID und Timestamp
- Validation Status (PASSED/FAILED)
- Expectation Results (Tabelle)
- Score-Verteilung (Grafik)
- Komponenten-Breakdown
- Sample der Daten

*Speicherort:*
- `gx-reports/report_batch_<id>.json`
- `gx-reports/report_batch_<id>.html`

== Monitoring in Grafana

Die Validierungs-Metriken sind in Grafana visualisiert:
- Validation Success Rate über Zeit
- Failed Expectations pro Batch
- Datenqualitäts-Trend

#pagebreak()

// ============================================================
// 8. MOOD SCORE ALGORITHMUS (KONZEPT)
// ============================================================

= Mood Score Algorithmus

== Implementierter Ansatz

Der Mood Score ist eine gewichtete Kombination von 7 Teilscores:

$ "MoodScore" = sum_(i=1)^7 w_i dot "Score"_i $

Mit: $ sum_(i=1)^7 w_i = 1 $ und $ "Score"_i in [0, 1] $

*Wertebereich:* 0.0 (sehr negativ) bis 1.0 (sehr positiv)

== Gewichtung der Faktoren

#table(
  columns: (auto, auto, 1fr),
  align: (left, center, left),
  inset: 10pt,

  [*Faktor*], [*Gewicht*], [*Begründung*],
  [News], [0.25], [Aktuelle Nachrichtenlage beeinflusst Stimmung stark],
  [Weather], [0.20], [Größter Einfluss auf tägliches Wohlbefinden],
  [Air Quality], [0.15], [Gesundheitsrelevanz, besonders für sensible Gruppen],
  [Traffic], [0.15], [Mobilitäts-Stress, Pendler-Erfahrung],
  [Alerts], [0.15], [Öffentliche Sicherheit und Warnungen],
  [Construction], [0.05], [Langfristige Störungen, weniger volatil],
  [Water Level], [0.05], [Elbe-Pegel, relevant für Hafen und Hochwasser],
)

== Teilscores Berechnung

=== NewsScore (Sentiment-basiert)

Nutzt Flair NLP für Sentiment-Analyse der BBC und NYT Headlines:

```python
def calculate_news_score(articles):
    sentiment_map = {
        "POSITIVE": 1.0,
        "NEUTRAL": 0.5,
        "NEGATIVE": 0.0
    }
    scores = [sentiment_map.get(a["sentiment"], 0.5) for a in articles]
    return sum(scores) / len(scores) if scores else 0.5
```

=== AirQualityScore

Gewichteter Durchschnitt mehrerer Luftqualitätsparameter:

```python
def calculate_air_score(data):
    # Gewichtungen der Parameter
    weights = {
        "pm2_5": 0.25,      # Feinstaub PM2.5
        "pm10": 0.15,       # Feinstaub PM10
        "nitrogen_dioxide": 0.20,  # NO₂
        "ozone": 0.15,      # O₃
        "sulphur_dioxide": 0.10,   # SO₂
        "carbon_monoxide": 0.10,   # CO
        "dust": 0.025,
        "pollen": 0.025
    }

    # Normalisierung auf 0-1 (invers: niedriger AQI = besserer Score)
    normalized = {}
    normalized["pm2_5"] = max(0, 1 - data["pm2_5"] / 75)
    normalized["pm10"] = max(0, 1 - data["pm10"] / 150)
    # ...

    return sum(weights[k] * normalized[k] for k in weights)
```

=== WeatherScore

Kombiniert Temperatur, Wetter-Code, Niederschlag und Wind:

```python
def calculate_weather_score(data):
    # Temperatur-Score (Optimal: 15-22°C)
    temp = data["temperature_2m"]
    if 15 <= temp <= 22:
        temp_score = 1.0
    elif 10 <= temp < 15 or 22 < temp <= 28:
        temp_score = 0.8
    elif 5 <= temp < 10 or 28 < temp <= 32:
        temp_score = 0.6
    else:
        temp_score = 0.4

    # Wetter-Code Score (WMO Codes)
    code_score = WEATHER_CODE_SCORES.get(data["weather_code"], 0.5)

    # Niederschlag Score
    precip_score = max(0, 1 - data["precipitation"] / 10)

    # Wind Score (Optimal: 5-15 km/h)
    wind = data["wind_speed_10m"]
    wind_score = 1.0 if wind < 20 else max(0, 1 - (wind - 20) / 30)

    # Gewichtete Kombination
    return (temp_score * 0.4 + code_score * 0.3 +
            precip_score * 0.2 + wind_score * 0.1)
```

=== TrafficScore

Basiert auf Verkehrsflussstatus aus GeoJSON-Daten:

```python
def calculate_traffic_score(features):
    status_scores = {
        "fliessend": 1.0,      # Fließender Verkehr
        "zaehfliessend": 0.6,  # Zähfließend
        "stockend": 0.3,       # Stockend
        "stau": 0.1            # Stau
    }

    scores = [status_scores.get(f["status"], 0.5) for f in features]
    return sum(scores) / len(scores) if scores else 0.5
```

=== AlertScore

Severity-basierte Bewertung von NINA-Warnmeldungen:

```python
def calculate_alert_score(alerts):
    if not alerts:
        return 1.0  # Keine Warnungen = optimal

    severity_scores = {
        "Minor": 0.9,
        "Moderate": 0.7,
        "Severe": 0.4,
        "Extreme": 0.1
    }

    scores = [severity_scores.get(a["severity"], 0.5) for a in alerts]
    return min(scores)  # Schlimmste Warnung bestimmt Score
```

=== ConstructionScore

Anzahl-basierte Bewertung aktiver Baustellen:

```python
def calculate_construction_score(count):
    # Baseline: 50 Baustellen = neutral
    baseline = 50
    score = max(0, 1 - (count - baseline) / baseline)
    return min(1.0, score)
```

=== WaterScore

Elbe-Pegel-basierte Bewertung:

```python
def calculate_water_score(level_cm):
    # Optimal: 400-700 cm (Normalpegel)
    if 400 <= level_cm <= 700:
        return 1.0
    elif 300 <= level_cm < 400 or 700 < level_cm <= 800:
        return 0.8
    elif 200 <= level_cm < 300 or 800 < level_cm <= 900:
        return 0.6
    else:
        return 0.3  # Extrem niedrig oder Hochwassergefahr
```

== Finale Berechnung

```python
def calculate_city_mood_score(components):
    weights = {
        "news": 0.25,
        "air": 0.15,
        "weather": 0.20,
        "traffic": 0.15,
        "alerts": 0.15,
        "construction": 0.05,
        "water": 0.05
    }

    mood_score = sum(
        weights[k] * components.get(f"{k}_score", 0.5)
        for k in weights
    )

    return round(mood_score, 4)
```

#pagebreak()

// ============================================================
// 9. VISUALISIERUNG (GRAFANA)
// ============================================================

= Visualisierung mit Grafana

== Grafana Dashboards

Zwei vorkonfigurierte Dashboards:

=== Simple Timeline Dashboard

Zeigt zeitliche Entwicklung der Event-Counts:
- Line Charts pro Datenquelle
- Time Range Selector
- Aggregation nach Tag

=== Geo Dashboard (PlantUML)

Geografische Visualisierung (Konzept):
- Karte von Hamburg
- Marker für Events
- Heatmaps für Dichte

== Datenquelle Konfiguration

*PostgreSQL Datasource:*
```yaml
apiVersion: 1
datasources:
  - name: PostgreSQL
    type: postgres
    url: postgres:5432
    database: city_mood
    user: spark
    secureJsonData:
      password: spark
```

== Metriken und Panels

*Typische Abfragen:*
```sql
SELECT day_date, SUM(event_count) as total
FROM daily_source_counts
WHERE source = 'open-meteo'
GROUP BY day_date
ORDER BY day_date;
```

#pagebreak()

// ============================================================
// 10. TECHNOLOGIE-STACK
// ============================================================

= Technologie-Stack

== Komponenten-Übersicht

#table(
  columns: (auto, auto, auto, 1fr),
  align: (left, left, left, left),
  inset: 10pt,

  [*Kategorie*], [*Technologie*], [*Version*], [*Begründung*],

  [Container], [Docker Compose], [3.9], [Multi-Container-Orchestrierung],
  [Message Broker], [Apache Kafka], [4.1.1], [Event-Streaming, KRaft-Modus (kein Zookeeper)],
  [Stream Processing], [Apache Spark], [3.5.1], [Unified Batch+Streaming, Structured Streaming],
  [Datenbank], [PostgreSQL], [16], [ACID, UPSERT, Grafana-Support, Views],
  [Cache], [Redis], [8.4.0], [Deduplizierung, Rate Limiting, Fetch-Tracking],
  [Visualisierung], [Grafana], [Latest], [Dashboards, PostgreSQL-Integration],
  [NLP], [Flair], [0.13+], [Sentiment-Analyse für News],
  [Data Quality], [Great Expectations], [0.18+], [Validierung, Reporting],
  [Sprache], [Python], [3.8+], [Fetcher-Services, PySpark],
  [Monitoring], [Kafka UI], [Latest], [Topic-Management, Consumer Groups],
)

== Python-Dependencies

*Fetcher-Services (app/services/requirements.txt):*
```
kafka-python>=2.0.0
requests>=2.28.0
redis>=5.0.0
feedparser>=6.0.0     # RSS-Parsing
flair>=0.13.0         # Sentiment-Analyse
torch>=2.0.0          # Flair-Backend
schedule>=1.2.0       # Zeitplanung
```

*PySpark Client (app/stream/requirements.txt):*
```
pyspark==3.5.1
great_expectations>=0.18.0
pandas>=2.0.0
psycopg2-binary>=2.9.0
numpy>=1.24.0
```

*Gemeinsame Utilities (app/common/):*
- `base_fetcher.py`: Basisklasse für alle Fetcher
- `common_utils.py`: Shared Helper-Funktionen

== JVM-Dependencies

*Spark Packages:*
- `org.postgresql:postgresql:42.7.3`: JDBC Driver
- `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1`: Kafka Connector

== Redis-Integration

Redis wird für folgende Zwecke genutzt:

*Deduplizierung:*
```python
# Hash der Article-ID speichern
redis_client.setex(f"article:{article_hash}", 86400, "1")
```

*Fetch-Timestamp-Tracking:*
```python
# Verhindert doppelte API-Calls innerhalb einer Stunde
last_fetch = redis_client.get(f"fetch:{source}:timestamp")
if last_fetch and (now - last_fetch) < 3600:
    return  # Skip fetch
```

*Rate Limiting (Redis Lock):*
```python
with redis_client.lock(f"lock:{source}", timeout=300):
    # Nur ein Fetcher pro Quelle gleichzeitig
    fetch_data()
```

== Flair Sentiment-Analyse

```python
from flair.models import TextClassifier
from flair.data import Sentence

classifier = TextClassifier.load("en-sentiment")

def analyze_sentiment(text):
    sentence = Sentence(text)
    classifier.predict(sentence)
    label = sentence.labels[0]
    return {
        "sentiment": label.value,  # POSITIVE, NEGATIVE
        "score": label.score       # 0.0 - 1.0
    }
```

#pagebreak()

// ============================================================
// 11. PERFORMANCE & SKALIERUNG
// ============================================================

= Performance und Skalierung

== Performance-Metriken

*Gemessene Werte (Entwicklungsumgebung):*

#table(
  columns: (1fr, auto),
  align: (left, right),
  inset: 10pt,

  [*Metrik*], [*Wert*],
  [End-to-End Latenz], [< 20 Sekunden],
  [Kafka Durchsatz], [~2000 Events/Minute],
  [Spark Batch-Intervall], [20 Sekunden],
  [Window Duration], [60 Minuten],
  [Watermark Delay], [5 Minuten],
  [PostgreSQL Write-Latenz], [< 100ms],
  [Sentiment-Analyse Latenz], [~500ms pro Article],
  [RAM-Verbrauch (Gesamt)], [~12 GB],
  [CPU-Last (6 Cores)], [< 60%],
)

== Aktuelle Konfiguration

*Spark Worker (docker-compose.yml):*
```yaml
spark-worker:
  environment:
    - SPARK_WORKER_MEMORY=10G
    - SPARK_WORKER_CORES=6
```

*PySpark Driver:*
```yaml
pyspark:
  environment:
    - SPARK_DRIVER_MEMORY=6G
```

== Skalierungs-Strategie

=== Horizontale Skalierung

*Spark Workers:*
```yaml
spark-worker:
  deploy:
    replicas: 3  # Mehrere Worker
    resources:
      limits:
        memory: 10G
```

*Kafka Partitions:*
```sh
--partitions 3  # Pro Topic für parallele Consumption
```

=== Vertikale Skalierung

*Spark Memory Tuning:*
```yaml
SPARK_WORKER_MEMORY: 10G
SPARK_WORKER_CORES: 6
SPARK_DRIVER_MEMORY: 6G
```

== Bottleneck-Analyse

*Potenzielle Engpässe:*
1. Traffic Fetcher: Viele GeoJSON-Features → Kafka Partitionierung
2. Sentiment-Analyse: Flair Model Loading → Model Caching
3. PostgreSQL Write: UPSERT-Performance → Connection Pooling
4. Redis Locks: Bei vielen Fetchern → Lock Timeout tuning
5. Spark Batch-Delay: Zu viele Events → Batch-Intervall anpassen

#pagebreak()

// ============================================================
// 12. SICHERHEIT
// ============================================================

= Sicherheit

== Netzwerk-Isolation

Alle Services laufen im isolierten Docker-Netzwerk `spark-network`:

```yaml
networks:
  spark-network:
    driver: bridge
    name: spark-network
```

== Authentifizierung

*PostgreSQL:*
- User: `spark`
- Password: `spark` (Produktiv: Secrets Management)

*Grafana:*
- Admin: `admin` / `admin` (Produktiv: ändern!)

== Datenschutz (DSGVO)

*Betroffene Datentypen:*
- Keine personenbezogenen Daten
- Öffentliche APIs (Open Data)
- Aggregierte Metriken (keine Einzelpersonen)

*Maßnahmen:*
- Retention Policies in Kafka (24h)
- Aggregation statt Rohdaten
- Anonymisierung wo nötig

== Secrets Management (Produktiv)

*Empfehlungen:*
- Docker Secrets oder Vault
- Environment Variables aus `.env`-Datei
- Keine Hardcoded Credentials

#pagebreak()

// ============================================================
// 13. DEPLOYMENT & BETRIEB
// ============================================================

= Deployment und Betrieb

== Systemvoraussetzungen

*Hardware:*
- CPU: 4 Cores (min.), 8 Cores (empfohlen)
- RAM: 8 GB (min.), 16 GB (empfohlen)
- Disk: 20 GB freier Speicher

*Software:*
- Docker Desktop (Windows/Mac) oder Docker Engine (Linux)
- Docker Compose 2.0+
- Git (zum Klonen des Repos)

== Installation

```sh
# Repository klonen
git clone https://github.com/your-repo/city-mood.git
cd city-mood

# Docker Compose Services starten
docker compose up -d --build

# Logs verfolgen
docker logs -f pyspark-client
```

== Konfiguration

*Environment Variables (optional):*
```sh
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
POSTGRES_USER=spark
POSTGRES_PASSWORD=spark
POSTGRES_DB=city_mood
```

== Datenbank initialisieren

```sh
# PostgreSQL Schema laden
Get-Content sql/init.sql | docker exec -i postgres psql -U spark -d city_mood
```

== Kafka Topics erstellen

Die Trigger-Topics werden automatisch vom Scheduler erstellt. Data-Topics werden von den Fetchern erstellt. Bei Bedarf manuell:

```sh
# Data Topics
docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic hh-traffic-data --bootstrap-server localhost:9092
docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic hh-weather-current --bootstrap-server localhost:9092
docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic hh-air-pollution-current --bootstrap-server localhost:9092
docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic bbc-europe-news --bootstrap-server localhost:9092
docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic nyt-europe-news --bootstrap-server localhost:9092
docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic hh-street-construction --bootstrap-server localhost:9092
```

== Services starten

```sh
# Alle Services
docker compose up -d

# Einzelner Service
docker compose up -d pyspark

# Rebuild nach Code-Änderungen
docker compose up -d --build
```

== Services stoppen

```sh
# Stoppen ohne Daten zu löschen
docker compose down

# Mit Volume-Löschung (Daten zurücksetzen)
docker compose down -v
```

== Wichtige URLs

#table(
  columns: (1fr, 1fr),
  align: (left, left),
  inset: 10pt,

  [*Service*], [*URL*],
  [Grafana Dashboard], [http://localhost:3000],
  [Kafka UI], [http://localhost:8090],
  [Spark Master UI], [http://localhost:8080],
  [Spark Worker UI], [http://localhost:8081],
  [PostgreSQL], [localhost:5432],
  [Data Quality Reports], [`./gx-reports/report_batch_*.html`],
)

*Login-Credentials:*
- Grafana: admin / admin
- PostgreSQL: spark / spark

*Redis:* Internes Netzwerk (kein externer Port)

#pagebreak()

// ============================================================
// 14. MONITORING & DEBUGGING
// ============================================================

= Monitoring und Debugging

== Log-Management

*Docker Logs:*
```sh
# Alle Logs eines Service
docker logs -f pyspark-client

# Letzte 100 Zeilen
docker logs --tail 100 pyspark-client

# Seit Zeitpunkt
docker logs --since 10m pyspark-client
```

*Spark Event Logs:*
- Gespeichert in: `./spark-logs/`
- Aufruf via Spark History Server (optional)

== Health Checks

*Container Status:*
```sh
docker ps
docker compose ps
```

*Kafka Topics:*
```sh
docker exec kafka /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```

*PostgreSQL:*
```sh
docker exec -it postgres psql -U spark -d city_mood -c "SELECT COUNT(*) FROM daily_source_counts;"
```

== Debugging-Strategien

*Spark UI nutzen:*
- `http://localhost:8080`: Job-Übersicht
- `http://localhost:8081`: Worker-Details
- SQL-Tab: Query-Pläne analysieren

*Kafka Consumer Groups:*
```sh
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list
```

*Test-Event senden:*
```sh
echo '{"fetch_timestamp":"2026-01-04T12:00:00","source":"test","feature":"{}"}' | docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic hh-traffic-data
```

#pagebreak()

// ============================================================
// 15. TROUBLESHOOTING
// ============================================================

= Troubleshooting

== Häufige Probleme

#table(
  columns: (1fr, 1.5fr),
  align: (left, left),
  inset: 10pt,
  
  [*Problem*], [*Lösung*],
  
  [Docker cannot connect], [Docker Desktop starten],
  
  [Port bereits belegt], [Port in docker-compose.yml ändern oder Prozess beenden],
  
  [Spark: no resources accepted], [Worker Memory/CPU reduzieren in docker-compose.yml],
  
  [Duplicate Key Error], [Upsert (ON CONFLICT) bereits implementiert, alte Daten löschen],
  
  [Keine Batches/Daten], [Topics prüfen, Fetcher-Logs checken, Test-Event senden],
  
  [Kafka: Topic not found], [Topic manuell erstellen (siehe Kapitel 13)],
  
  [PySpark ModuleNotFoundError], [Dependencies installieren: docker exec -u root pyspark pip install ...],
  
  [PostgreSQL Connection refused], [Container-Status prüfen, Netzwerk prüfen],
)

== Diagnose-Befehle

```sh
# Container-Status
docker ps

# Netzwerk prüfen
docker network inspect spark-network

# PostgreSQL Verbindung testen
docker exec -it postgres psql -U spark -d city_mood

# Kafka Topics auflisten
docker exec kafka /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092

# Kafka Consumer testen
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic hh-traffic-data --from-beginning --max-messages 5
```

== Recovery-Strategien

*Bei Spark-Crash:*
1. Logs prüfen: `docker logs pyspark-client`
2. Container neu starten: `docker compose restart pyspark`
3. Falls persistent: Checkpoints löschen (temporäre Verzeichnisse)

*Bei Kafka-Datenverlust:*
- Retention Policy prüfen (24h für Trigger-Topics)
- Topics neu erstellen falls nötig
- Fetcher manuell triggern

#pagebreak()

// ============================================================
// 16. ERGEBNISSE & ERKENNTNISSE
// ============================================================

= Ergebnisse und Erkenntnisse

== Projektergebnisse

*Erfolgreich implementiert:*
- ✅ 9 Datenquellen integriert (Weather, Air Quality, Traffic, Transparenz, NINA, Water Level, BBC News, NYT News, Street Construction)
- ✅ Event-Driven Architecture mit Kafka (KRaft-Modus)
- ✅ Echtzeit-Streaming mit Spark (20s Micro-Batch)
- ✅ Sentiment-Analyse für Nachrichten (Flair NLP)
- ✅ Great Expectations Datenqualitätsprüfungen mit HTML-Reports
- ✅ Redis-basierte Deduplizierung und Rate Limiting
- ✅ PostgreSQL Persistierung mit UPSERT + History-Tracking
- ✅ City Mood Score Algorithmus (7 gewichtete Komponenten)
- ✅ Grafana Dashboards mit Echtzeit-Visualisierung

*Datenvolumen (Beispiel 24h):*
- Weather: ~24 Events
- Air Pollution: ~24 Events
- Traffic: ~1500 Events (GeoJSON Features)
- Transparenz: ~20-50 Events
- NINA Alerts: ~0-10 Events
- Water Level: ~24 Events
- BBC News: ~50-100 Events (mit Sentiment)
- NYT News: ~100-200 Events (Europe + World, mit Sentiment)
- Street Construction: ~50-200 Events

== Lessons Learned

*Technische Herausforderungen:*
1. *Spark Version Compatibility:* Downgrade von 4.0.1 auf 3.5.1 wegen Great Expectations
2. *Duplicate Key Errors:* UPSERT essentiell für Streaming-Idempotenz
3. *Docker Permissions:* Spark User benötigt Schreibrechte für Ivy Cache
4. *Kafka Topic Creation:* Auto-Create funktioniert nicht zuverlässig
5. *Redis Locks:* Notwendig um doppelte Fetches bei Container-Restarts zu verhindern
6. *Sentiment Model Loading:* Flair-Modelle benötigen Zeit beim ersten Load

*Best Practices:*
- Früh Event-Driven Pattern etablieren
- Idempotenz von Anfang an einplanen
- Datenqualität in Pipeline integrieren, nicht nachträglich
- Monitoring und Logging ab Tag 1
- Redis für State-Management außerhalb von Spark
- History-Tabelle für Audit-Trail

== Limitationen

*Technisch:*
- Micro-Batch (20s) statt echtes Real-Time
- Einzelne Spark Worker (nicht hochverfügbar)
- Keine automatische Skalierung
- Transparenz-Daten werden produziert aber nicht konsumiert

*Daten:*
- Nur Hamburg als Beispiel
- Abhängigkeit von API-Verfügbarkeit (Open-Meteo, NINA, PegelOnline)
- RSS-Feeds können verzögert sein
- Keine historischen Daten vor Projektstart

#pagebreak()

// ============================================================
// ANHANG
// ============================================================

= Anhang

== A. Projektstruktur

```
City-Mood/
├── app/
│   ├── common/                      # Gemeinsame Utilities
│   │   ├── base_fetcher.py          # Basisklasse für Fetcher
│   │   └── common_utils.py          # Helper-Funktionen
│   ├── services/                    # Fetcher-Microservices
│   │   ├── scheduler/
│   │   │   ├── Dockerfile
│   │   │   └── scheduler.py
│   │   ├── weather_fetcher/
│   │   ├── air_pollution_fetcher/
│   │   ├── traffic_fetcher/
│   │   ├── transparenz_fetcher/
│   │   ├── nina_alert_fetcher/
│   │   ├── water_level_fetcher/
│   │   ├── bbc_news_fetcher/        # NEU: BBC RSS mit Sentiment
│   │   ├── nyt_news_fetcher/        # NEU: NYT RSS mit Sentiment
│   │   ├── street_construction_fetcher/  # NEU: Baustellen
│   │   └── requirements.txt
│   ├── stream/
│   │   ├── city_mood_pipeline.py    # Haupt-Streaming-Pipeline
│   │   └── requirements.txt
│   └── Dockerfile                   # Base Python Image
├── db/
│   └── init/
│       └── init.sql                 # Datenbankschema
├── doc/
│   ├── doc.typ                      # Diese Dokumentation
│   ├── doc.pdf
│   ├── architecture.puml            # PlantUML Diagramm
│   └── architecture.png
├── grafana/
│   ├── provisioning/
│   │   ├── dashboards/
│   │   │   └── city-mood-dashboard.json
│   │   └── datasources/
│   │       └── postgres.yml
│   └── data/                        # Grafana SQLite DB
├── gx-reports/                      # Great Expectations Reports
│   ├── report_batch_*.json
│   └── report_batch_*.html
├── spark-apps/                      # Spark Job Artefakte
├── spark-data/                      # Spark Working Directory
├── spark-logs/                      # Spark Event Logs
├── docker-compose.yml               # Vollständige Infrastruktur
├── build_containers.sh              # Docker Build Script
└── README.md
```

== B. Glossar

#table(
  columns: (auto, 1fr),
  align: (left, left),
  inset: 10pt,

  [*Begriff*], [*Erklärung*],
  [AQI], [Air Quality Index - Luftqualitätsindex (European AQI: 0-500)],
  [BBK], [Bundesamt für Bevölkerungsschutz und Katastrophenhilfe],
  [EDA], [Event-Driven Architecture],
  [Flair], [NLP-Framework für Sentiment-Analyse],
  [GeoJSON], [JSON-basiertes Format für geografische Daten],
  [Great Expectations], [Python-Framework für Datenqualitätsprüfung],
  [KRaft], [Kafka Raft - Kafka ohne Zookeeper (ab Kafka 3.0)],
  [Micro-Batch], [Mini-Batch-Verarbeitung im Streaming (hier: 20s)],
  [NINA], [Notfall-Informations- und Nachrichten-App],
  [NLP], [Natural Language Processing - Computerlinguistik],
  [RSS], [Really Simple Syndication - Nachrichtenfeed-Format],
  [Sentiment], [Stimmung/Tonalität eines Textes (positiv/neutral/negativ)],
  [UPSERT], [INSERT mit ON CONFLICT UPDATE],
  [Watermark], [Zeitstempel für Spät-Ankömmlinge im Streaming],
  [Window], [Zeitfenster für Aggregationen (hier: 60 Minuten)],
)

== C. Referenzen

*Technologie-Dokumentation:*
- Apache Kafka: https://kafka.apache.org/documentation/
- Apache Spark Structured Streaming: https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html
- PostgreSQL UPSERT: https://www.postgresql.org/docs/current/sql-insert.html
- Grafana: https://grafana.com/docs/
- Redis: https://redis.io/docs/
- Great Expectations: https://docs.greatexpectations.io/
- Flair NLP: https://github.com/flairNLP/flair

*APIs und Datenquellen:*
- Open-Meteo Weather: https://open-meteo.com/
- Open-Meteo Air Quality: https://open-meteo.com/en/docs/air-quality-api
- Hamburg Transparenzportal: https://transparenz.hamburg.de/
- NINA API (BBK): https://nina.api.proxy.bund.dev/
- PegelOnline: https://www.pegelonline.wsv.de/
- BBC RSS Feeds: https://www.bbc.com/news/10628494
- NYT RSS Feeds: https://developer.nytimes.com/docs/rss-api/1/overview
- Hamburg GeoServices: https://geodienste.hamburg.de/

*Wissenschaft:*
- UN World Urbanization Prospects 2018
- Bollen et al. (2011): Twitter Mood Predicts the Stock Market

== D. Kontakt & Support

*Projektteam:*
- Dustin Wickert
- Arash Sedighi

*Institution:*
- HAW Hamburg
- Fakultät Technik und Informatik

*Repository:*
- GitHub: (wird noch veröffentlicht)

#pagebreak()

// Ende der Dokumentation
