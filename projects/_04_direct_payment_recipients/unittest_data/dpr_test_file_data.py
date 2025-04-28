from dataclasses import dataclass
from datetime import date


@dataclass
class PAFilledPostsByIcbArea:
    sample_ons_contemporary_with_duplicates_rows = [
        ("AB10AA", date(2024, 1, 1), "cssr1", "icb1"),
        ("AB10AB", date(2024, 1, 1), "cssr1", "icb1"),
        ("AB10AC", date(2024, 1, 1), "cssr1", "icb1"),
        ("AB10AC", date(2024, 1, 1), "cssr1", "icb1"),
    ]

    sample_ons_contemporary_rows = [
        ("AB10AA", date(2024, 1, 1), "cssr1", "icb1"),
        ("AB10AB", date(2024, 1, 1), "cssr1", "icb1"),
        ("AB10AC", date(2024, 1, 1), "cssr1", "icb1"),
        ("AB10AA", date(2024, 1, 1), "cssr2", "icb2"),
        ("AB10AB", date(2024, 1, 1), "cssr2", "icb3"),
        ("AB10AC", date(2024, 1, 1), "cssr2", "icb3"),
        ("AB10AD", date(2024, 1, 1), "cssr2", "icb3"),
        ("AB10AA", date(2023, 1, 1), "cssr1", "icb1"),
        ("AB10AB", date(2023, 1, 1), "cssr1", "icb1"),
        ("AB10AC", date(2023, 1, 1), "cssr1", "icb1"),
        ("AB10AA", date(2023, 1, 1), "cssr2", "icb2"),
        ("AB10AB", date(2023, 1, 1), "cssr2", "icb3"),
        ("AB10AC", date(2023, 1, 1), "cssr2", "icb3"),
    ]

    expected_postcode_count_per_la_rows = [
        ("AB10AA", date(2024, 1, 1), "cssr1", "icb1", 3),
        ("AB10AB", date(2024, 1, 1), "cssr1", "icb1", 3),
        ("AB10AC", date(2024, 1, 1), "cssr1", "icb1", 3),
        ("AB10AA", date(2024, 1, 1), "cssr2", "icb2", 4),
        ("AB10AB", date(2024, 1, 1), "cssr2", "icb3", 4),
        ("AB10AC", date(2024, 1, 1), "cssr2", "icb3", 4),
        ("AB10AD", date(2024, 1, 1), "cssr2", "icb3", 4),
        ("AB10AA", date(2023, 1, 1), "cssr1", "icb1", 3),
        ("AB10AB", date(2023, 1, 1), "cssr1", "icb1", 3),
        ("AB10AC", date(2023, 1, 1), "cssr1", "icb1", 3),
        ("AB10AA", date(2023, 1, 1), "cssr2", "icb2", 3),
        ("AB10AB", date(2023, 1, 1), "cssr2", "icb3", 3),
        ("AB10AC", date(2023, 1, 1), "cssr2", "icb3", 3),
    ]

    expected_postcode_count_per_la_icb_rows = [
        ("AB10AA", date(2024, 1, 1), "cssr1", "icb1", 3),
        ("AB10AB", date(2024, 1, 1), "cssr1", "icb1", 3),
        ("AB10AC", date(2024, 1, 1), "cssr1", "icb1", 3),
        ("AB10AA", date(2024, 1, 1), "cssr2", "icb2", 1),
        ("AB10AB", date(2024, 1, 1), "cssr2", "icb3", 3),
        ("AB10AC", date(2024, 1, 1), "cssr2", "icb3", 3),
        ("AB10AD", date(2024, 1, 1), "cssr2", "icb3", 3),
        ("AB10AA", date(2023, 1, 1), "cssr1", "icb1", 3),
        ("AB10AB", date(2023, 1, 1), "cssr1", "icb1", 3),
        ("AB10AC", date(2023, 1, 1), "cssr1", "icb1", 3),
        ("AB10AA", date(2023, 1, 1), "cssr2", "icb2", 1),
        ("AB10AB", date(2023, 1, 1), "cssr2", "icb3", 2),
        ("AB10AC", date(2023, 1, 1), "cssr2", "icb3", 2),
    ]

    sample_rows_with_la_and_hybrid_area_postcode_counts = [
        (date(2024, 1, 1), 3, 3),
        (date(2024, 1, 1), 4, 1),
        (date(2024, 1, 1), 4, 3),
        (date(2023, 1, 1), 3, 3),
        (date(2023, 1, 1), 3, 1),
        (date(2023, 1, 1), 3, 2),
    ]

    expected_ratio_between_hybrid_area_and_la_area_postcodes_rows = [
        (date(2024, 1, 1), 3, 3, 1.00000),
        (date(2024, 1, 1), 4, 1, 0.25000),
        (date(2024, 1, 1), 4, 3, 0.75000),
        (date(2023, 1, 1), 3, 3, 1.00000),
        (date(2023, 1, 1), 3, 1, 0.33333),
        (date(2023, 1, 1), 3, 2, 0.66666),
    ]

    full_rows_with_la_and_hybrid_area_postcode_counts = [
        ("AB10AA", date(2023, 5, 1), "cssr1", "icb1", 3, 3, 1.00000),
        ("AB10AB", date(2023, 5, 1), "cssr1", "icb1", 3, 3, 1.00000),
        ("AB10AA", date(2023, 5, 1), "cssr2", "icb2", 4, 1, 0.25000),
        ("AB10AB", date(2023, 5, 1), "cssr2", "icb3", 4, 3, 0.75000),
        ("AB10AA", date(2022, 5, 1), "cssr1", "icb1", 3, 3, 1.00000),
        ("AB10AB", date(2022, 5, 1), "cssr1", "icb1", 3, 3, 1.00000),
        ("AB10AC", date(2022, 5, 1), "cssr1", "icb1", 3, 3, 1.00000),
    ]

    expected_deduplicated_import_date_hybrid_and_la_and_ratio_rows = [
        (date(2023, 5, 1), "cssr1", "icb1", 1.00000),
        (date(2023, 5, 1), "cssr2", "icb2", 0.25000),
        (date(2023, 5, 1), "cssr2", "icb3", 0.75000),
        (date(2022, 5, 1), "cssr1", "icb1", 1.00000),
    ]

    sample_pa_filled_posts_rows = [
        ("Leeds", 100.2, 2023, "2023"),
        ("Bradford", 200.3, 2023, "2023"),
        ("Hull", 300.3, 2022, "2023"),
    ]

    expected_create_date_column_from_year_in_pa_estimates_rows = [
        ("Leeds", 100.2, 2023, "2023", date(2024, 3, 31)),
        ("Bradford", 200.3, 2023, "2023", date(2024, 3, 31)),
        ("Hull", 300.3, 2022, "2023", date(2023, 3, 31)),
    ]

    sample_postcode_proportions_before_joining_pa_filled_posts_rows = [
        (date(2023, 5, 1), "Leeds", "icb1", 1.00000),
        (date(2023, 5, 1), "Bradford", "icb2", 0.25000),
        (date(2023, 5, 1), "Bradford", "icb3", 0.75000),
        (date(2022, 5, 1), "Leeds", "icb1", 1.00000),
        (date(2022, 5, 1), "Barking & Dagenham", "icb4", 1.00000),
    ]

    sample_pa_filled_posts_prepared_for_joining_to_postcode_proportions_rows = [
        ("Leeds", 100.2, "2023", date(2024, 3, 31)),
        ("Bradford", 200.3, "2023", date(2024, 3, 31)),
        ("Leeds", 300.3, "2022", date(2023, 3, 31)),
        ("Barking and Dagenham", 300.3, "2022", date(2023, 3, 31)),
    ]

    expected_postcode_proportions_after_joining_pa_filled_posts_rows = [
        (date(2023, 5, 1), "Leeds", "icb1", 1.00000, 100.2, "2023"),
        (date(2023, 5, 1), "Bradford", "icb2", 0.25000, 200.3, "2023"),
        (date(2023, 5, 1), "Bradford", "icb3", 0.75000, 200.3, "2023"),
        (date(2022, 5, 1), "Leeds", "icb1", 1.00000, 300.3, "2022"),
        (date(2022, 5, 1), "Barking & Dagenham", "icb4", 1.00000, None, None),
    ]

    sample_proportions_and_pa_filled_posts_rows = [
        (0.25000, 100.2),
        (None, 200.3),
        (0.75000, None),
        (None, None),
    ]

    expected_pa_filled_posts_after_applying_proportions_rows = [
        (0.25000, 25.05000),
        (None, None),
        (0.75000, None),
        (None, None),
    ]

    sample_la_name_rows = [
        ("Bath & N E Somerset",),
        ("Southend",),
        ("Bedford",),
        (None,),
    ]

    expected_la_names_with_correct_spelling_rows = [
        ("Bath and North East Somerset",),
        ("Southend on Sea",),
        ("Bedford",),
        (None,),
    ]


@dataclass
class CalculatePaRatioData:
    calculate_pa_ratio_rows = [
        (2021, 1.0),
        (2021, 2.0),
        (2021, 2.0),
        (2021, 1.0),
        (2021, 1.0),
    ]

    exclude_outliers_rows = [
        (2021, 10.0),
        (2021, 20.0),
        (2021, 0.0),
        (2021, 9.0),
        (2021, 1.0),
        (2021, -1.0),
    ]

    calculate_average_ratio_rows = [
        (2021, 1.0),
        (2021, 2.0),
        (2020, 1.0),
        (2020, 1.0),
        (2019, 2.0),
        (2019, 2.0),
    ]

    add_historic_rows = [
        (2011, None),
        (2012, None),
        (2013, None),
        (2014, 1.0),
        (2015, None),
        (2016, None),
        (2017, 1.0),
        (2018, None),
        (2019, 1.0),
        (2020, 1.0),
        (2021, 1.0),
        (2022, 1.6),
        (2023, 2.2),
    ]

    apply_rolling_average_rows = [
        (2019, 1.0),
        (2020, 1.0),
        (2021, 1.0),
        (2022, 1.6),
        (2023, 2.2),
    ]

    reduce_year_by_one_rows = [
        (2024, "some data"),
        (2023, "other data"),
    ]
    expected_reduce_year_by_one_rows = [
        (2023, "some data"),
        (2022, "other data"),
    ]
