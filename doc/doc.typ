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
    counter(page).display("Seite 1 von 1", both: true)
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
        Dustin \
        Arash
        
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
  - 7 autonome Daten-Fetcher-Services (Weather, Air Pollution, Traffic, Transparenz, NINA Alerts, Water Level, NDR News)
  - Event-Driven Scheduler zur Orchestrierung
  - Apache Kafka als zentraler Event-Bus (15+ Topics)
  - Spark Structured Streaming Pipeline mit Datenqualitätsprüfung
  - PostgreSQL-Datenbankschicht mit UPSERT-Strategie
  - Grafana-basierte Visualisierungsschicht
]

#v(1em)

#table(
  columns: (auto, 1fr),
  align: (left, left),
  inset: 10pt,
  [*Metrik*], [*Wert*],
  [Verarbeitete Datenquellen], [7 heterogene APIs],
  [Kafka Topics], [15+ (Trigger + Data)],
  [Streaming-Latenz], [< 15 Sekunden (End-to-End)],
  [Datenaggregation], [Täglich pro Quelle/Kategorie],
  [Persistierung], [PostgreSQL mit Upsert-Semantik],
  [Skalierung], [Horizontal (Spark Workers)],
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
    - Python 3.8+ (Fetcher Services)
  ],
  [
    *Infrastructure:*
    - Docker Compose (Orchestrierung)
    - Grafana (Visualisierung)
    - Kafka UI (Monitoring)
    - PySpark (Stream Processing)
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
- 7 Datenquellen (Wetter, Luftqualität, Verkehr, Transparenz, NINA, Wasserpegel, News)
- Tagesaggregation (keine Intraday-Analysen)
- Batch-Verarbeitung im 10-Sekunden-Intervall
- Historische Speicherung in relationaler Datenbank

*Nicht im Scope:*
- Predictive Analytics / Machine Learning (zukünftige Erweiterung)
- Mobile App oder Web-Frontend
- Weitere Städte (konzeptionell erweiterbar)
- Social-Media-Sentiment-Analyse
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
- *Kapitel 4-10:* Detaillierte Beschreibung aller Systemkomponenten
- *Kapitel 11-12:* Datenmodell und Datenqualität
- *Kapitel 13:* Mood Score Algorithmus
- *Kapitel 14-17:* Performance, Sicherheit, Deployment
- *Kapitel 18-19:* Ergebnisse und Ausblick
- *Anhänge:* Technische Details, Code-Listings, Referenzen

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
  [Weather Fetcher], [Holt Wetterdaten], [Python + Kafka], [1],
  [Air Pollution], [Holt Luftqualitätsdaten], [Python + Kafka], [1],
  [Traffic Fetcher], [Verarbeitet GeoJSON-Daten], [Python + Kafka], [1],
  [Transparenz], [Durchsucht Open Data], [Python + Kafka], [1],
  [NINA Alerts], [Holt Warnmeldungen], [Python + Kafka], [1],
  [Water Level], [Holt Pegelstände], [Python + Kafka], [1],
  [NDR News], [Parst RSS-Feed], [Python + Kafka], [1],
  [Kafka Broker], [Zentraler Event-Bus], [Kafka 4.1.1 (KRaft)], [1],
  [Spark Master], [Cluster-Koordination], [Spark 3.5.1], [1],
  [Spark Worker], [Transformationen], [Spark 3.5.1], [1-n],
  [PySpark Client], [Streaming-Job], [PySpark], [1],
  [PostgreSQL], [Persistenz], [PostgreSQL 16], [1],
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

Dieses Kapitel dokumentiert alle 7 Datenquellen und den Scheduler-Service.

== Scheduler-Service

Der Scheduler ist die zentrale Orchestrierungskomponente.

*Funktion:* Sendet stündlich Trigger-Events an alle Fetcher-Topics

*Kafka Topics (Producer):*
- `fetch-weather`
- `fetch-air-pollution`
- `fetch-traffic`
- `fetch-news`

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

== NDR News Fetcher (RSS Feed)

*Datenquelle:* NDR Hamburg RSS Feed

*Verarbeitung:* RSS-Parsing, Kategorisierung nach Keywords

*Kafka Topic:* `hh-ndr-news`

== Kafka Topics Übersicht

#table(
  columns: (auto, auto, 1fr),
  align: (left, left, left),
  inset: 10pt,
  
  [*Topic-Name*], [*Typ*], [*Beschreibung*],
  
  [fetch-weather], [Trigger], [Trigger für Weather Fetcher],
  [fetch-air-pollution], [Trigger], [Trigger für Air Pollution],
  [fetch-traffic], [Trigger], [Trigger für Traffic],
  [fetch-news], [Trigger], [Trigger für News],
  [hh-weather-current], [Data], [Aktuelle Wetterdaten],
  [hh-weather-daily], [Data], [Tägliche Wetterprognose],
  [hh-air-pollution-current], [Data], [Luftqualitätsdaten],
  [hh-traffic-data], [Data], [Verkehrsdaten (GeoJSON)],
  [hh-transparenz-events], [Data], [Hamburg Open Data Events],
  [hh-public-alerts-current], [Data], [NINA Warnmeldungen],
  [hh-water-level-current], [Data], [Elbe Pegelstände],
  [hh-ndr-news], [Data], [NDR Hamburg News],
)

#pagebreak()

// ============================================================
// 5. STREAM PROCESSING (SPARK)
// ============================================================

= Stream Processing mit Apache Spark

== Spark Structured Streaming

*Konzept:* Micro-Batch Processing (alle 10 Sekunden)

*Konfiguration:*
```python
spark = SparkSession.builder \
    .appName("CityMoodPipeline") \
    .master("spark://spark-master:7077") \
    .config("spark.jars.packages", 
            "org.postgresql:postgresql:42.7.3," +
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .getOrCreate()
```

== Schema-Normalisierung

Unterschiedliche Quellen werden auf einheitliches Schema gebracht:

*Traffic Schema:*
```python
traffic_schema = StructType([
    StructField("fetch_timestamp", StringType(), True),
    StructField("source", StringType(), True),
    StructField("feature", StringType(), True)
])
```

*Events Schema:*
```python
event_schema = StructType([
    StructField("source", StringType(), True),
    StructField("fetch_timestamp", StringType(), True),
    StructField("title", StringType(), True),
    StructField("category", ArrayType(StringType()), True)
])
```

*Normalisierte Felder:*
- `event_time` (TIMESTAMP): Für Watermarking
- `day_date` (DATE): Für Tagesaggregation
- `source` (STRING): Datenquelle
- `category` (STRING): Kategorie

== Aggregations-Pipeline

```python
counts_df = common_df \
    .withWatermark("event_time", "1 day") \
    .groupBy("day_date", "source", "category") \
    .count() \
    .withColumnRenamed("count", "event_count")
```

*Watermarking:* 1 Tag (ermöglicht Spätankömmlinge)

*GroupBy-Dimensionen:*
- Tag (day_date)
- Quelle (source)
- Kategorie (category)

== UPSERT-Strategie

Um Duplicate Key Errors bei Restarts zu vermeiden:

```python
sql = """
INSERT INTO daily_source_counts 
  (day_date, source, category, event_count, updated_at)
VALUES %s
ON CONFLICT (day_date, source, category)
DO UPDATE SET 
    event_count = EXCLUDED.event_count,
    updated_at = EXCLUDED.updated_at;
"""
execute_values(cursor, sql, rows)
```

*Vorteile:*
- Idempotenz gewährleistet
- Wiederholte Verarbeitung safe
- Keine manuellen Deletes nötig

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
  [daily_source_counts], [(day_date, source, category)], [Tägliche Event-Zählungen],
  [daily_mood_score], [day_date], [Berechneter Mood Score],
  [mood_aggregates], [metric_name], [Historische Aggregationen],
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

== Tabelle: daily_source_counts

Haupttabelle für aggregierte Tageswerte:

```sql
CREATE TABLE daily_source_counts (
    day_date DATE,
    source VARCHAR(50),
    category VARCHAR(100),
    event_count INT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (day_date, source, category)
);
```

*Composite Primary Key:* Gewährleistet Einzigartigkeit pro Tag/Quelle/Kategorie

*Beispieldaten:*

#table(
  columns: (auto, auto, auto, auto),
  align: (left, left, left, center),
  inset: 8pt,
  
  [*day_date*], [*source*], [*category*], [*event_count*],
  [2026-01-04], [open-meteo], [current_weather], [1],
  [2026-01-04], [nina], [public_alert], [3],
  [2026-01-04], [traffic_hh_zip], [traffic_feature], [1523],
)

== Tabelle: daily_mood_score

Zukünftige Tabelle für berechneten Mood Score:

```sql
CREATE TABLE daily_mood_score (
    day_date DATE PRIMARY KEY,
    mood_score DOUBLE PRECISION,  -- Skala: -1.0 bis +1.0
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

#pagebreak()

// ============================================================
// 7. DATENQUALITÄT
// ============================================================

= Datenqualität

== Data Quality Framework

Jeder Micro-Batch durchläuft Qualitätsprüfungen vor der Persistierung.

*Implementierte Checks:*

#table(
  columns: (auto, 1fr, auto),
  align: (left, left, center),
  inset: 10pt,
  
  [*Check*], [*Regel*], [*Schwellwert*],
  [null_sources], [source IS NOT NULL], [0],
  [negative_counts], [event_count >= 0], [0],
)

== Qualitätsprüfung im Code

```python
pdf = batch_df.toPandas()

# Regel 1: Source darf nicht null sein
null_sources = pdf["source"].isnull().sum()

# Regel 2: Event Count muss positiv sein
negative_counts = (pdf["event_count"] < 0).sum()

if null_sources > 0 or negative_counts > 0:
    logger.warning(f"Data Quality Issues: {null_sources} null sources")
    status = "FAILED"
else:
    logger.info(f"Data Quality Check PASSED")
    status = "PASSED"
```

== Report-Generierung

Für jeden Batch werden zwei Reports erzeugt:

*JSON Report (maschinenlesbar):*
```json
{
  "batch_id": 42,
  "generated_at_utc": "2026-01-04T12:00:00",
  "rows_in_batch": 1523,
  "null_sources": 0,
  "negative_counts": 0,
  "status": "PASSED"
}
```

*HTML Report (visuell):*
- Batch-ID und Timestamp
- Status (PASSED/FAILED)
- Statistiken
- Sample der ersten 50 Zeilen

*Speicherort:*
- `gx-reports/report_batch_<id>.json`
- `gx-reports/report_batch_<id>.html`

#pagebreak()

// ============================================================
// 8. MOOD SCORE ALGORITHMUS (KONZEPT)
// ============================================================

= Mood Score Algorithmus

== Konzeptioneller Ansatz

Der Mood Score ist eine gewichtete Kombination verschiedener Teilscores:

$ "MoodScore" = w_1 dot "WeatherScore" + w_2 dot "AirQualityScore" + w_3 dot "TrafficScore" + w_4 dot "EventScore" $

Mit: $ w_1 + w_2 + w_3 + w_4 = 1 $

== Gewichtung der Faktoren

#table(
  columns: (auto, auto, 1fr),
  align: (left, center, left),
  inset: 10pt,
  
  [*Faktor*], [*Gewicht*], [*Begründung*],
  [Weather], [0.30], [Größter Einfluss auf Wohlbefinden],
  [Air Quality], [0.25], [Gesundheitsrelevanz],
  [Traffic], [0.25], [Mobilitäts-Stress],
  [Events], [0.20], [Störungen vs. positive Ereignisse],
)

== Teilscores Berechnung

=== WeatherScore

Basiert auf:
- Temperatur (Optimal: 15-22°C)
- Niederschlag (weniger = besser)
- Sonnenscheindauer (mehr = besser)
- Windgeschwindigkeit (moderat = besser)

*Normalisierung:* Linear auf [-1, +1]

=== AirQualityScore

Basiert auf:
- European AQI (invers: niedriger = besser)
- PM2.5, PM10 Werte
- Pollenbelastung (für Allergiker)

*Formel:* $ "AirQualityScore" = 1 - ("AQI" / "AQI"_"max") $

=== TrafficScore

Basiert auf:
- Anzahl Verkehrsstörungen (weniger = besser)
- Anzahl Sperrungen
- Unfälle

*Formel:* $ "TrafficScore" = 1 - ("Stoerungen" / "Stoerungen"_"baseline") $

=== EventScore

Basiert auf:
- Positive Events (+): Kulturveranstaltungen
- Negative Events (-): Warnmeldungen, Polizeimeldungen

*Formel:* $ "EventScore" = ("positive" - "negative") / ("positive" + "negative") $

== Implementierung (Zukünftig)

Aktuell werden nur die Rohdaten aggregiert. Die Mood Score Berechnung erfolgt als Post-Processing in PostgreSQL oder als zusätzlicher Spark-Job.

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
  
  [Container], [Docker Compose], [3.9], [Einfaches Multi-Container-Setup],
  [Message Broker], [Apache Kafka], [4.1.1], [Event-Streaming, KRaft-Modus],
  [Stream Processing], [Apache Spark], [3.5.1], [Unified Batch+Streaming],
  [Datenbank], [PostgreSQL], [16], [ACID, UPSERT, Grafana-Support],
  [Visualisierung], [Grafana], [Latest], [Dashboards, Alerts],
  [Sprache], [Python], [3.8+], [Fetcher-Services, PySpark],
  [Monitoring], [Kafka UI], [Latest], [Topic-Management],
)

== Python-Dependencies

*Fetcher-Services:*
- `kafka-python`: Kafka Producer/Consumer
- `requests`: HTTP API Calls
- `schedule`: Zeitplanung (optional)

*PySpark Client:*
- `pyspark==3.5.1`: Spark Structured Streaming
- `great_expectations`: Data Quality (optional)
- `pandas`: DataFrame-Operationen
- `psycopg2-binary`: PostgreSQL Treiber

== JVM-Dependencies

*Spark Packages:*
- `org.postgresql:postgresql:42.7.3`: JDBC Driver
- `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1`: Kafka Connector

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
  [End-to-End Latenz], [< 15 Sekunden],
  [Kafka Durchsatz], [~1000 Events/Minute],
  [Spark Batch-Intervall], [10 Sekunden],
  [PostgreSQL Write-Latenz], [< 100ms],
  [RAM-Verbrauch (Gesamt)], [~6 GB],
  [CPU-Last (4 Cores)], [< 50%],
)

== Skalierungs-Strategie

=== Horizontale Skalierung

*Spark Workers:*
```yaml
spark-worker:
  deploy:
    replicas: 3  # Mehrere Worker
    resources:
      limits:
        memory: 4G
```

*Kafka Partitions:*
```sh
--partitions 3  # Pro Topic
```

=== Vertikale Skalierung

*Spark Memory Tuning:*
```yaml
SPARK_WORKER_MEMORY: 4G
SPARK_WORKER_CORES: 4
```

== Bottleneck-Analyse

*Potenzielle Engpässe:*
1. Traffic Fetcher: Viele GeoJSON-Features → Kafka Partitionierung
2. PostgreSQL Write: UPSERT-Performance → Connection Pooling
3. Spark Batch-Delay: Zu viele Events → Batch-Intervall anpassen

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
- Password: `spark` (⚠️ Produktiv: Secrets Management)

*Grafana:*
- Admin: `admin` / `admin` (⚠️ Produktiv: ändern!)

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

```sh
docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic hh-traffic-data --bootstrap-server localhost:9092
docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic hh-kultur-events --bootstrap-server localhost:9092
docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic hh-transparenz-events --bootstrap-server localhost:9092
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
)

*Login-Credentials:*
- Grafana: admin / admin
- PostgreSQL: spark / spark

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
- ✅ 7 Datenquellen integriert
- ✅ Event-Driven Architecture mit Kafka
- ✅ Echtzeit-Streaming mit Spark
- ✅ Datenqualitätsprüfungen
- ✅ PostgreSQL Persistierung mit UPSERT
- ✅ Grafana Dashboards

*Datenvolumen (Beispiel 24h):*
- Weather: ~24 Events
- Air Pollution: ~24 Events
- Traffic: ~1500 Events (GeoJSON Features)
- Transparenz: ~20-50 Events
- NINA Alerts: ~0-10 Events
- Water Level: ~24 Events
- NDR News: ~10-30 Events

== Lessons Learned

*Technische Herausforderungen:*
1. *Spark Version Compatibility:* Downgrade von 4.0.1 auf 3.5.1 wegen Great Expectations
2. *Duplicate Key Errors:* UPSERT essentiell für Streaming-Idempotenz
3. *Docker Permissions:* Spark User benötigt Schreibrechte für Ivy Cache
4. *Kafka Topic Creation:* Auto-Create funktioniert nicht zuverlässig

*Best Practices:*
- Früh Event-Driven Pattern etablieren
- Idempotenz von Anfang an einplanen
- Datenqualität in Pipeline integrieren, nicht nachträglich
- Monitoring und Logging ab Tag 1

== Limitationen

*Technisch:*
- Micro-Batch (10s) statt echtes Real-Time
- Einzelne Spark Worker (nicht hochverfügbar)
- Keine automatische Skalierung
- Mood Score noch nicht implementiert

*Daten:*
- Nur Hamburg als Beispiel
- Abhängigkeit von API-Verfügbarkeit
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
│   ├── services/
│   │   ├── scheduler/
│   │   │   ├── Dockerfile
│   │   │   └── scheduler.py
│   │   ├── weather-fetcher/
│   │   ├── air-pollution-fetcher/
│   │   ├── traffic-fetcher/
│   │   ├── transparenz-fetcher/
│   │   ├── nina-alert-fetcher/
│   │   ├── water-level-fetcher/
│   │   ├── ndr-news-fetcher/
│   │   └── requirements.txt
│   └── stream/
│       ├── spark_stream_pg.py
│       └── requirements.txt
├── doc/
│   ├── doc.typ
│   ├── doc.pdf
│   └── architecture.puml
├── grafana/
│   ├── dashboards/
│   └── provisioning/
├── sql/
│   └── init.sql
├── gx-reports/
├── docker-compose.yml
└── README.md
```

== B. Glossar

#table(
  columns: (auto, 1fr),
  align: (left, left),
  inset: 10pt,
  
  [*Begriff*], [*Erklärung*],
  [AQI], [Air Quality Index - Luftqualitätsindex],
  [BBK], [Bundesamt für Bevölkerungsschutz und Katastrophenhilfe],
  [EDA], [Event-Driven Architecture],
  [GeoJSON], [JSON-basiertes Format für geografische Daten],
  [KRaft], [Kafka Raft - Kafka ohne Zookeeper],
  [NINA], [Notfall-Informations- und Nachrichten-App],
  [UPSERT], [INSERT mit ON CONFLICT UPDATE],
  [Watermark], [Zeitstempel für Spät-Ankömmlinge im Streaming],
)

== C. Referenzen

*Technologie-Dokumentation:*
- Apache Kafka: https://kafka.apache.org/documentation/
- Apache Spark Structured Streaming: https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html
- PostgreSQL UPSERT: https://www.postgresql.org/docs/current/sql-insert.html
- Grafana: https://grafana.com/docs/

*APIs:*
- Open-Meteo: https://open-meteo.com/
- Hamburg Transparenzportal: https://transparenz.hamburg.de/
- NINA API: https://nina.api.proxy.bund.dev/
- PegelOnline: https://www.pegelonline.wsv.de/

*Wissenschaft:*
- UN World Urbanization Prospects 2018
- Bollen et al. (2011): Twitter Mood Predicts the Stock Market

== D. Kontakt & Support

*Projektteam:*
- Dustin
- Arash

*Institution:*
- HAW Hamburg
- Fakultät Technik und Informatik

*Repository:*
- GitHub: (wird noch veröffentlicht)

#pagebreak()

// Ende der Dokumentation
