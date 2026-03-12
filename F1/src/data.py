from fastf1.events import Event
from fastf1.core import Session, Laps


class DataUtils:
    """This class implements most of the operations that will be taking place
    on the raw data that was loaded from a race-weekend using the FastF1 API.
    It handles all the loading, transformations and structured storage operations."""

    # ============ Standard Methods ============
    def __init__(self, race_event: Event, cache_dir: str) -> None:
        
        self.race_event = race_event
        self.cache_dir = cache_dir

    # ============ Member Methods ============
    def load_data(self) -> tuple[Session, Session]:
        """Loads the raw data for 2 sessions corresponding to the race weekend
        that was passed during initialisation of the instance.
        
        The sessions loaded are:
        - The Qualifying Session
        - The Race Session

        Args:
        - self: Instance of the DataUtils object

        Returns:
        - tuple[quali: Session, race: Session]
        """

        # Qualifying Session
        quali = self.race_event.get_qualifying()

        # Race Session
        race = self.race_event.get_race()

        # Loading all the data corresponding to the sessions
        sessions = [quali, race]
        for session in sessions:
            session.load(laps=True, telemetry=True, weather=True, messages=True)

        return quali, race
    
    def get_throttle_map(
            self, 
            driver_number: str,
            driver_quali_laps: Laps
        ) -> tuple[float, float, float]:
        """Utilises the telemetry of each driver for their fastest lap in Q1
        to identify the amount time spent on full throttle, gear changes / feathering and 
        lift and coast.
        
        Args:
        - driver_number: str
        - driver_quali_telemetry: Telemetry
        
        Returns:
        - tuple[
            percent_full_throttle: float
            percent_feathering_throttle: float
            percent_lico_throttle: float
        ]"""

        # Accessing the Driver Telemetry
        driver_telemetry = (
            driver_quali_laps
            .pick_drivers(identifiers=driver_number)  # type: ignore
            .pick_fastest()
            .get_car_data()  # type: ignore
        )
        tele_len = len(driver_telemetry)

        # Throttle Params
        full_throttle_percent = len(driver_telemetry[driver_telemetry["Throttle"] >= 90]) / tele_len
        lico_percent = len(driver_telemetry[driver_telemetry["Throttle"] <= 10].count()) / tele_len
        transition_percent = 1 - full_throttle_percent - lico_percent

        return full_throttle_percent, transition_percent, lico_percent
