from dataclasses import dataclass


@dataclass
class HackathonColumns:
    import_month: str = "import_month"
    end_month: str = "end_month"
    is_leaver: str = "is_leaver"
    main_service_hackathon: str = "main_service_hackathon"
