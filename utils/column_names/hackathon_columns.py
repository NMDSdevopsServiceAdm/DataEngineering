from dataclasses import dataclass


@dataclass
class HackathonColumns:
    import_month: str = "import_month"
    end_month: str = "end_month"
    establishment_in_both_snapshots: str = "establishment_in_both_snapshots"
    is_leaver: str = "is_leaver"
