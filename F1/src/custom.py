from fastf1.core import Session


class CustomSession:
    """This class streamlines all the data corresponding to a race weekend
    made available through various attributes for each session as one object."""

    # ============ Standard Methods ============
    def __init__(self, session: Session) -> None:
        
        # Laps: A High-Level pd.DataFrame
        if session.laps is not None:
            self.laps = session.laps

        # SessionResults: A High-Level pd.DataFrame
        if session.results is not None:
            self.results = session.results

        # Weather: A regular pd.DataFrame
        if session.weather_data is not None:
            self.weather = session.weather_data

        # Telemetry, Position Data: dict[str, pd.DataFrame] corresponding to each driver number as key
        if session.car_data is not None:
            self.telemetry = session.car_data
        
        if session.pos_data is not None:
            self.pos_data = session.pos_data