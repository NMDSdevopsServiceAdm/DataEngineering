from dataclasses import dataclass


@dataclass
class InvalidPostcodes:
    invalid_postcodes_map = {
        "70 6PD": "S70 6PD",
        "B12 ODG": "B12 0DG",
        "B66 2FF": "B66 2AL",
        "B69 E3G": "B69 3EG",
        "B7 5DP": "B7 5PD",
        "B97 6DT": "B97 6AT",
        "BA 2UA": "BA3 2UA",
        "BN6 4EA": "BN16 4EA",
        "C07 8LA": "CO7 8LA",
        "CA1 2XT": "CA1 2TX",
        "CA11 OFA": "CA11 0FA",
        "CCT8 8SA": "CT8 8SA",
        "CH1 4QL": "CH1 4QN",
        "CH1 6BS": "CH1 5RE",
        "CH4 0NR": "CH4 8RQ",
        "CH4 0RG": "CH4 7QJ",
        "CH4 8RQ": "CH4 8BJ",
        "CH41 1UE": "CH41 1EU",
        "CH5 2LY": "CH1 6HU",
        "CO10 0F0": "CO10 0FD",
        "CRO 4TB": "CR0 4TB",
        "CV9 9HG": "CV12 9HG",
        "DE1 IBT": "DE1 1BT",
        "DN20 ODA": "DN20 0DA",
        "DY1 0PX": "DY10 2PX",
        "EC2 5UU": "EC2M 5UU",
        "GU1 3FS": "GU51 3FS",
        "GU11 4TH": "GU11 1TH",
        "HD1 1AD": "HD1 1RL",
        "HP20 1SN.": "HP20 1SN",
        "HU13 OEG": "HU13 0EG",
        "HU17 ORH": "HU17 0RH",
        "HU21 0LS": "HU170LS",
        "L20 4QC": "L20 4QG",
        "L33 7QX": "L33 7TX",
        "LE65 3LP": "LE65 2RW",
        "LS11 9YT": "LS11 9YJ",
        "LS23 8UE": "LS26 8UE",
        "M24 4TL": "M24 4GH",
        "ME8 OEQ": "ME8 0EQ",
        "MK6 43AY": "MK6 3AY",
        "MK9 1HF": "MK9 1FH",
        "N12 8FP": "N12 8NP",
        "N8 5HY": "N8 7HS",
        "NG10 9LA": "NN10 9LA",
        "NG6 3DG": "NG5 2AT",
        "NN17 7NN": "NN17 1NN",
        "NR330TJ": "NR33 0TQ",
        "OX4 2XQ": "OX4 2SQ",
        "PA20 3AR": "PO20 3BD",
        "PA20 3BD": "PO20 3BD",
        "PL7 1RP": "PL7 1RF",
        "PO8 4PY": "PO4 8PY",
        "PR! 9HL": "PR1 9HL",
        "RG2 2EG": "RG1 2EG",
        "RG7 7QF": "RG1 7QF",
        "RM3 8HM": "RM3 8HN",
        "S26 4DW": "S26 4WD",
        "S43 9HE": "S41 9HE",
        "SG1 8AL": "SG18 8AL",
        "SO50 4FD": "SO50 9FD",
        "ST4 4GF": "ST4 7AA",
        "SY13 3PA": "SY13 1AA",
        "SY2 9JN": "SY3 9JN",
        "TF7 3BY": "TF4 3BY",
        "TS12 1SU": "TS12 1DY",
        "TS17 3TB": "TS18 3TB",
        "TS20 2BI": "TS20 2BL",
        "UB4 0EJ.": "UB4 0EJ",
        "WF10 9UG": "WF10 4BJ",
        "WF12 2SE": "WF13 2SE",
        "WR13 3JS": "WF13 3JS",
        "YO61 3FF": "YO61 3FN",
    }
