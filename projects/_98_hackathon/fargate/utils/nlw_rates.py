from datetime import date

# Hourly National Living Wage (£) by the April date each rate took effect.
# `bands` are (minimum age, rate), oldest band first.
# Source: gov.uk National Minimum Wage and National Living Wage rates.
NLW_RATES: dict[date, dict] = {
    date(2019, 4, 1): {
        "apprentice": 3.90,
        "bands": [(25, 8.21), (21, 7.70), (18, 6.15), (0, 4.35)],
    },
    date(2020, 4, 1): {
        "apprentice": 4.15,
        "bands": [(25, 8.72), (21, 8.20), (18, 6.45), (0, 4.55)],
    },
    date(2021, 4, 1): {
        "apprentice": 4.30,
        "bands": [(23, 8.91), (21, 8.36), (18, 6.56), (0, 4.62)],
    },
    date(2022, 4, 1): {
        "apprentice": 4.81,
        "bands": [(23, 9.50), (21, 9.18), (18, 6.83), (0, 4.81)],
    },
    date(2023, 4, 1): {
        "apprentice": 5.28,
        "bands": [(23, 10.42), (21, 10.18), (18, 7.49), (0, 5.28)],
    },
    date(2024, 4, 1): {
        "apprentice": 6.40,
        "bands": [(21, 11.44), (18, 8.60), (0, 6.40)],
    },
    date(2025, 4, 1): {
        "apprentice": 7.55,
        "bands": [(21, 12.21), (18, 10.00), (0, 7.55)],
    },
    date(2026, 4, 1): {
        "apprentice": 8.00,
        "bands": [(21, 12.71), (18, 10.85), (0, 8.00)],
    },
}

# Every band start age used in any year, highest first. Workers are placed in the
# highest start at or below their age.
BAND_STARTS = sorted(
    {min_age for rates in NLW_RATES.values() for min_age, _ in rates["bands"]},
    reverse=True,
)


def build_nlw_lookup_rows() -> list[tuple[date, bool, int, float]]:
    """Returns `(effective_date, is_apprentice, band_start, rate)` rows.

    Each date has one apprentice row at band start 0, as their rate ignores age,
    and one non-apprentice row per `BAND_STARTS`. A start that is not a band in
    that year repeats the rate of the band below it, so every worker's band start
    matches a row.
    """
    rows = []
    for effective_date, rates in NLW_RATES.items():
        rows.append((effective_date, True, 0, rates["apprentice"]))
        for band_start in BAND_STARTS:
            rate = next(
                rate for min_age, rate in rates["bands"] if band_start >= min_age
            )
            rows.append((effective_date, False, band_start, rate))
    return rows
