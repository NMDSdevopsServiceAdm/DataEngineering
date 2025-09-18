from dataclasses import dataclass


@dataclass
class JoinDimensionData:
    join_dimension_with_simple_equivalence_cqc_rows = [
        ("loc_1", "value_a1", "20200101"),
        ("loc_1", "value_a2", "20200102"),
        ("loc_1", "value_a3", "20200103"),
        ("loc_1", "value_a4", "20200104"),
        ("loc_2", "value_b1", "20200101"),
        ("loc_2", "value_b2", "20200102"),
        ("loc_2", "value_b3", "20200103"),
        ("loc_2", "value_b4", "20200104"),
    ]

    join_dimension_with_simple_equivalence_dim_rows = [
        ("loc_1", "dim_a1", "20200101", "2020", "01", "01", "20200101"),
        ("loc_1", "dim_a2", "20200102", "2020", "01", "02", "20200102"),
        ("loc_1", "dim_a3", "20200103", "2020", "01", "03", "20200103"),
        ("loc_1", "dim_a4", "20200104", "2020", "01", "04", "20200104"),
        ("loc_2", "dim_b1", "20200101", "2020", "01", "01", "20200101"),
        ("loc_2", "dim_b2", "20200102", "2020", "01", "02", "20200102"),
        ("loc_2", "dim_b3", "20200103", "2020", "01", "03", "20200103"),
        ("loc_2", "dim_b4", "20200104", "2020", "01", "04", "20200104"),
    ]

    expected_join_dimension_with_simple_equivalence_rows = [
        ("loc_1", "20200101", "value_a1", "dim_a1"),
        ("loc_1", "20200102", "value_a2", "dim_a2"),
        ("loc_1", "20200103", "value_a3", "dim_a3"),
        ("loc_1", "20200104", "value_a4", "dim_a4"),
        ("loc_2", "20200101", "value_b1", "dim_b1"),
        ("loc_2", "20200102", "value_b2", "dim_b2"),
        ("loc_2", "20200103", "value_b3", "dim_b3"),
        ("loc_2", "20200104", "value_b4", "dim_b4"),
    ]
