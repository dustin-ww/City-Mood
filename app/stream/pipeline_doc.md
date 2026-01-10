# City Mood Score - Calculation Formula

## Overall Formula

```
City Mood Score = (News × 0.25) + (Air × 0.15) + (Weather × 0.20) + 
                  (Traffic × 0.15) + (Alerts × 0.15) + (Construction × 0.05) + 
                  (Water × 0.05)
```

**Value Range:** 0.0 - 1.0 (higher = better mood)

---

## Components & Weighting

| Component | Weight | Fallback Value | Description |
|-----------|--------|----------------|-------------|
| **News Score** | **25%** | 0.5 | Sentiment analysis of news headlines |
| **Weather Score** | **20%** | 0.6 | Temperature, precipitation, wind, weather conditions |
| **Air Score** | 15% | 0.7 | Air quality (European AQI) |
| **Traffic Score** | 15% | 0.8 | Traffic flow (free-flowing, slow, congested) |
| **Alerts Score** | 15% | 1.0 | Public warnings and their severity |
| **Construction Score** | 5% | 0.8 | Number of active construction sites/disruptions |
| **Water Score** | 5% | 1.0 | Elbe river level (flood/low water risk) |

---

## Example Calculations

Under optimal conditions:
```
0.5 × 0.25 + 0.8 × 0.15 + 0.9 × 0.20 + 0.9 × 0.15 + 1.0 × 0.15 + 0.9 × 0.05 + 1.0 × 0.05
= 0.125 + 0.120 + 0.180 + 0.135 + 0.150 + 0.045 + 0.050
= 0.805 (good city mood)
```

Under poor conditions:
```
0.2 × 0.25 + 0.3 × 0.15 + 0.3 × 0.20 + 0.3 × 0.15 + 0.5 × 0.15 + 0.5 × 0.05 + 0.5 × 0.05
= 0.050 + 0.045 + 0.060 + 0.045 + 0.075 + 0.025 + 0.025
= 0.325 (poor city mood)
```

---

## Fallback Logic

When no data is available for a component, a **neutral to slightly positive** default value is used:

- Missing News → 0.5 (neutral)
- Missing Air Quality → 0.7 (slightly good)
- Missing Weather → 0.6 (slightly good)
- Missing Traffic → 0.8 (good)
- No Alerts → 1.0 (optimal)
- No Construction → 0.8 (good)
- Normal Water Levels → 1.0 (optimal)

This prevents missing data from artificially pulling the score down.